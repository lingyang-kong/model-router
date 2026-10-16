use std::{sync::Arc, time::Duration};

use axum::http::{HeaderMap, Uri};
use futures_util::{SinkExt, StreamExt};
use futures_util::{
    future::BoxFuture,
    stream::{FuturesUnordered, SplitSink, SplitStream},
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
}

pub(crate) enum WebSocketAction {
    Forward(Message),
    Respond(BoxFuture<'static, Vec<Message>>),
}

pub(crate) async fn relay(
    downstream: TokioIo<DownstreamUpgraded>,
    upstream: reqwest::Upgraded,
    client: Client,
    context: WebSocketContext,
) {
    let config = WebSocketConfig::default()
        .max_message_size(None)
        .max_frame_size(None);
    let downstream = WebSocketStream::from_raw_socket(downstream, Role::Server, Some(config)).await;
    let upstream = WebSocketStream::from_raw_socket(upstream, Role::Client, Some(config)).await;
    relay_sockets(downstream, upstream, client, context).await;
}

async fn relay_sockets<D, U>(
    downstream: WebSocketStream<D>,
    upstream: WebSocketStream<U>,
    client: Client,
    context: WebSocketContext,
) where
    D: AsyncRead + AsyncWrite + Unpin,
    U: AsyncRead + AsyncWrite + Unpin,
{
    let (mut downstream_sink, mut downstream_stream) = downstream.split();
    let (mut upstream_sink, mut upstream_stream) = upstream.split();
    let (local_tx, local_rx) = tokio::sync::mpsc::channel(1);
    let (from_downstream, close) = tokio::select! {
        result = forward_requests(
            &mut downstream_stream, &mut upstream_sink, client,
            context.clone(),
            local_tx,
        ) => (true, result),
        result = forward_responses(
            &mut upstream_stream, &mut downstream_sink, client, local_rx,
        ) => (false, result),
    };
    let Some(close) = close else {
        return;
    };

    if from_downstream {
        finish_close(&mut downstream_sink, &mut upstream_sink, close).await;
    } else {
        finish_close(&mut upstream_sink, &mut downstream_sink, close).await;
    }
}

async fn forward_requests<S, D>(
    source: &mut SplitStream<WebSocketStream<S>>,
    destination: &mut SplitSink<WebSocketStream<D>, Message>,
    client: Client,
    context: WebSocketContext,
    local_tx: tokio::sync::mpsc::Sender<Vec<Message>>,
) -> Option<Message>
where
    S: AsyncRead + AsyncWrite + Unpin,
    D: AsyncRead + AsyncWrite + Unpin,
{
    let mut jobs = FuturesUnordered::<BoxFuture<'static, Vec<Message>>>::new();
    loop {
        tokio::select! {
            incoming = source.next() => {
                let Some(Ok(message)) = incoming else { return None };
                match message {
                    Message::Ping(_) => continue,
                    close @ Message::Close(_) => return Some(close),
                    message => match client.websocket_request(message, &context).await {
                        WebSocketAction::Forward(message) => destination.send(message).await.ok()?,
                        WebSocketAction::Respond(job) => jobs.push(job),
                    }
                }
            }
            Some(messages) = jobs.next(), if !jobs.is_empty() => {
                if local_tx.send(messages).await.is_err() { return None; }
            }
        }
    }
}

async fn forward_responses<S, D>(
    source: &mut SplitStream<WebSocketStream<S>>,
    destination: &mut SplitSink<WebSocketStream<D>, Message>,
    client: Client,
    mut local_rx: tokio::sync::mpsc::Receiver<Vec<Message>>,
) -> Option<Message>
where
    S: AsyncRead + AsyncWrite + Unpin,
    D: AsyncRead + AsyncWrite + Unpin,
{
    let mut local_open = true;
    loop {
        tokio::select! {
            incoming = source.next() => {
                let Some(Ok(message)) = incoming else { return None };
                match message {
                    Message::Ping(_) => continue,
                    close @ Message::Close(_) => return Some(close),
                    message => {
                        let message = client.adapt_websocket_response(message);
                        destination.send(message).await.ok()?;
                    }
                }
            }
            messages = local_rx.recv(), if local_open => match messages {
                Some(messages) => for message in messages {
                    destination.send(message).await.ok()?;
                },
                None => local_open = false,
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
