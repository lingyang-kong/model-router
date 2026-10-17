use std::{sync::Arc, time::Duration};

use axum::http::{HeaderMap, Uri};
use futures_util::{SinkExt, StreamExt};
use futures_util::{
    future::BoxFuture,
    stream::{BoxStream, SelectAll, SplitSink, SplitStream},
};
use hyper::upgrade::Upgraded as DownstreamUpgraded;
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{
        Message,
        protocol::{Role, WebSocketConfig},
    },
};

use crate::{adapters::Client, config::Provider, router::RouterState};

#[derive(Clone)]
pub(crate) struct WebSocketContext {
    pub state: Arc<RouterState>,
    pub provider: Arc<Provider>,
    pub headers: HeaderMap,
    pub uri: Uri,
    pub model: Option<String>,
    pub origin: Option<Arc<Provider>>,
    /// True only while the original upstream WebSocket is available for forwarding.
    pub connected: bool,
    /// The initial provider explicitly refused WebSocket negotiation.
    pub http_only: bool,
}

pub(crate) enum WebSocketAction {
    Forward(Message),
    Respond(BoxFuture<'static, Vec<Message>>),
    Stream(BoxStream<'static, Message>),
}

pub(crate) async fn relay(
    downstream: TokioIo<DownstreamUpgraded>,
    upstream: Option<reqwest::Upgraded>,
    client: Client,
    context: WebSocketContext,
) {
    let config = WebSocketConfig::default()
        .max_message_size(None)
        .max_frame_size(None);
    let downstream = WebSocketStream::from_raw_socket(downstream, Role::Server, Some(config)).await;
    let upstream = match upstream {
        Some(upstream) => {
            Some(WebSocketStream::from_raw_socket(upstream, Role::Client, Some(config)).await)
        }
        None => None,
    };
    relay_sockets(downstream, upstream, client, context).await;
}

async fn relay_sockets<D, U>(
    downstream: WebSocketStream<D>,
    upstream: Option<WebSocketStream<U>>,
    client: Client,
    context: WebSocketContext,
) where
    D: AsyncRead + AsyncWrite + Unpin,
    U: AsyncRead + AsyncWrite + Unpin,
{
    let (mut downstream_sink, mut downstream_stream) = downstream.split();
    let (mut upstream_sink, mut upstream_stream) = match upstream {
        Some(upstream) => {
            let (sink, stream) = upstream.split();
            (Some(sink), Some(stream))
        }
        None => (None, None),
    };
    let (local_tx, local_rx) = tokio::sync::mpsc::channel(1);
    let (closed_tx, closed_rx) = tokio::sync::oneshot::channel();
    let (from_downstream, close) = tokio::select! {
        result = forward_requests(
            &mut downstream_stream, &mut upstream_sink, client,
            context.clone(),
            local_tx,
            closed_rx,
        ) => (true, result),
        result = forward_responses(
            &mut upstream_stream, &mut downstream_sink, client, local_rx, closed_tx,
        ) => (false, result),
    };
    let Some(close) = close else {
        return;
    };

    if from_downstream && (upstream_stream.is_some() || upstream_sink.is_none()) {
        if let Some(upstream) = &mut upstream_sink {
            finish_close(&mut downstream_sink, upstream, close).await;
        } else {
            let _ = tokio::time::timeout(Duration::from_secs(1), downstream_sink.flush()).await;
        }
    } else {
        if let Some(upstream) = &mut upstream_sink {
            finish_close(upstream, &mut downstream_sink, close).await;
        }
    }
}

async fn forward_requests<S, D>(
    source: &mut SplitStream<WebSocketStream<S>>,
    destination: &mut Option<SplitSink<WebSocketStream<D>, Message>>,
    client: Client,
    mut context: WebSocketContext,
    local_tx: tokio::sync::mpsc::Sender<Vec<Message>>,
    mut closed_rx: tokio::sync::oneshot::Receiver<Option<Message>>,
) -> Option<Message>
where
    S: AsyncRead + AsyncWrite + Unpin,
    D: AsyncRead + AsyncWrite + Unpin,
{
    let mut jobs = SelectAll::<BoxStream<'static, Message>>::new();
    // Once local jobs exist, their last events may still be queued after the jobs finish.
    let mut has_local_requests = false;
    loop {
        tokio::select! {
            close = &mut closed_rx, if context.connected => {
                let close = close.ok().flatten();
                if !has_local_requests {
                    return close;
                }
                context.connected = false;
                if let Some(mut sink) = destination.take() {
                    let _ = tokio::time::timeout(Duration::from_secs(1), sink.flush()).await;
                }
            }
            incoming = source.next() => {
                let Some(Ok(message)) = incoming else { return None };
                match message {
                    Message::Ping(_) => continue,
                    close @ Message::Close(_) => return Some(close),
                    message => {
                        let stream = match client.websocket_request(message, &context).await {
                            WebSocketAction::Forward(message) => {
                                // Control/opaque frames can still arrive after the original hop closes.
                                if let Some(sink) = destination.as_mut()
                                    && sink.send(message).await.is_err()
                                {
                                    if !has_local_requests {
                                        return None;
                                    }
                                    context.connected = false;
                                    *destination = None;
                                }
                                continue;
                            }
                            WebSocketAction::Respond(job) => futures_util::stream::once(job).flat_map(futures_util::stream::iter).boxed(),
                            WebSocketAction::Stream(stream) => stream,
                        };
                        has_local_requests = true;
                        jobs.push(stream);
                    }
                }
            }
            Some(message) = jobs.next(), if !jobs.is_empty() => {
                if local_tx.send(vec![message]).await.is_err() { return None; }
            }
        }
    }
}

async fn forward_responses<S, D>(
    source: &mut Option<SplitStream<WebSocketStream<S>>>,
    destination: &mut SplitSink<WebSocketStream<D>, Message>,
    client: Client,
    mut local_rx: tokio::sync::mpsc::Receiver<Vec<Message>>,
    closed_tx: tokio::sync::oneshot::Sender<Option<Message>>,
) -> Option<Message>
where
    S: AsyncRead + AsyncWrite + Unpin,
    D: AsyncRead + AsyncWrite + Unpin,
{
    let mut closed_tx = Some(closed_tx);
    loop {
        tokio::select! {
            incoming = async { source.as_mut().unwrap().next().await }, if source.is_some() => {
                let message = match incoming {
                    Some(Ok(message)) if !message.is_close() => message,
                    closed => {
                        *source = None;
                        let _ = closed_tx.take().unwrap().send(closed.and_then(Result::ok));
                        continue;
                    }
                };
                match message {
                    Message::Ping(_) => continue,
                    message => {
                        let message = client.adapt_websocket_response(message);
                        destination.send(message).await.ok()?;
                    }
                }
            }
            messages = local_rx.recv() => match messages {
                Some(messages) => for message in messages {
                    destination.send(message).await.ok()?;
                },
                None => return None,
            }
        }
    }
}

async fn finish_close<S, D>(
    source: &mut SplitSink<WebSocketStream<S>, Message>,
    destination: &mut SplitSink<WebSocketStream<D>, Message>,
    close: Message,
) where
    S: AsyncRead + AsyncWrite + Unpin,
    D: AsyncRead + AsyncWrite + Unpin,
{
    // Finish both hops normally, but do not retain a cancelled session forever
    // if either peer has stopped reading. This grace applies only after Close.
    let _ = tokio::time::timeout(Duration::from_secs(1), async {
        let _ = tokio::join!(source.flush(), destination.send(close));
    })
    .await;
}
