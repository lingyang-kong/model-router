use std::{collections::VecDeque, sync::Arc, time::Duration};

use axum::http::{HeaderMap, Uri};
use futures_util::{FutureExt, SinkExt, StreamExt};
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
}

pub(crate) enum WebSocketAction {
    Forward(Message),
    Respond(BoxFuture<'static, Vec<Message>>),
    Stream(BoxStream<'static, Message>),
}

enum Close {
    Downstream(Message),
    Upstream(Message),
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
    let (sink, stream) = upstream.split();
    let (mut upstream_sink, mut upstream_stream) = (Some(sink), Some(stream));
    let (local_tx, local_rx) = tokio::sync::mpsc::channel(1);
    let (closed_tx, closed_rx) = tokio::sync::oneshot::channel();
    let close = tokio::select! {
        result = forward_requests(
            &mut downstream_stream, &mut upstream_sink, client,
            context,
            local_tx,
            closed_rx,
        ) => result,
        result = forward_responses(
            &mut upstream_stream, &mut downstream_sink, client, local_rx, closed_tx,
        ) => result.map(Close::Upstream),
    };
    let Some(close) = close else {
        return;
    };

    match close {
        Close::Downstream(close) => {
            if let Some(upstream) = &mut upstream_sink {
                finish_close(&mut downstream_sink, upstream, close).await;
            } else {
                let _ = tokio::time::timeout(Duration::from_secs(1), downstream_sink.flush()).await;
            }
        }
        Close::Upstream(close) => {
            if let Some(upstream) = &mut upstream_sink {
                finish_close(upstream, &mut downstream_sink, close).await;
            } else {
                let _ =
                    tokio::time::timeout(Duration::from_secs(1), downstream_sink.send(close)).await;
            }
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
) -> Option<Close>
where
    S: AsyncRead + AsyncWrite + Unpin,
    D: AsyncRead + AsyncWrite + Unpin,
{
    let mut jobs = SelectAll::<BoxStream<'static, Message>>::new();
    // Serialize preparation to preserve forwarding order, while polling active
    // responses and downstream control frames independently. All work is owned
    // by this relay, so closing the session drops pending preparation and jobs.
    let mut pending = VecDeque::<Message>::new();
    let mut preparation: Option<BoxFuture<'static, (WebSocketAction, Option<Message>)>> = None;
    // Once local jobs exist, their last events may still be queued after the jobs finish.
    let mut has_local_requests = false;
    let mut upstream_close = None;
    loop {
        // Preparation may only forward an opaque/control frame. If rerouting
        // creates no local work, the original close still ends this session.
        if !context.connected && !has_local_requests && preparation.is_none() && pending.is_empty()
        {
            return upstream_close.map(Close::Upstream);
        }
        if preparation.is_none()
            && let Some(message) = pending.pop_front()
        {
            let context = context.clone();
            preparation = Some(
                async move {
                    let original = context.connected.then(|| message.clone());
                    (client.websocket_request(message, &context).await, original)
                }
                .boxed(),
            );
        }
        tokio::select! {
            close = &mut closed_rx, if context.connected => {
                let close = close.ok().flatten();
                if !has_local_requests && preparation.is_none() {
                    return close.map(Close::Upstream);
                }
                upstream_close = close;
                context.connected = false;
                if let Some(mut sink) = destination.take() {
                    let _ = tokio::time::timeout(Duration::from_secs(1), sink.flush()).await;
                }
            }
            incoming = source.next() => {
                let Some(Ok(message)) = incoming else { return None };
                match message {
                    Message::Ping(_) => continue,
                    close @ Message::Close(_) => return Some(Close::Downstream(close)),
                    message => pending.push_back(message),
                }
            }
            (action, original) = async { preparation.as_mut().unwrap().await }, if preparation.is_some() => {
                preparation = None;
                let stream = match action {
                    WebSocketAction::Forward(message) => {
                        // Preparation can outlive the original hop. Route the original
                        // frame again, since request adaptation need not be idempotent.
                        if !context.connected && let Some(original) = original {
                            pending.push_front(original);
                            continue;
                        }
                        // Control/opaque frames can still arrive after the original hop closes.
                        if let Some(sink) = destination.as_mut()
                            && sink.send(message).await.is_err()
                        {
                            // Close may have been read before this send. Resolve
                            // pending work just as if the close branch had won.
                            match closed_rx.try_recv() {
                                Ok(close) => {
                                    upstream_close = close;
                                    if let Some(original) = original {
                                        pending.push_front(original);
                                    }
                                }
                                Err(_) if !has_local_requests => return None,
                                Err(_) => {}
                            }
                            context.connected = false;
                            if let Some(mut sink) = destination.take() {
                                let _ = tokio::time::timeout(Duration::from_secs(1), sink.flush()).await;
                            }
                        }
                        continue;
                    }
                    WebSocketAction::Respond(job) => futures_util::stream::once(job).flat_map(futures_util::stream::iter).boxed(),
                    WebSocketAction::Stream(stream) => stream,
                };
                has_local_requests = true;
                jobs.push(stream);
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
                    let closing = message.is_close();
                    destination.send(message).await.ok()?;
                    if closing { return None; }
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
        // A simultaneous peer Close can reject send() while its automatic
        // reply is still queued. Flush that reply as well.
        let _ = destination.flush().await;
    })
    .await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn simultaneous_closes_flush_both_automatic_replies() {
        let (downstream_io, downstream_peer_io) = tokio::io::duplex(1024);
        let (upstream_io, upstream_peer_io) = tokio::io::duplex(1024);
        let downstream = WebSocketStream::from_raw_socket(downstream_io, Role::Server, None).await;
        let upstream = WebSocketStream::from_raw_socket(upstream_io, Role::Client, None).await;
        let mut downstream_peer =
            WebSocketStream::from_raw_socket(downstream_peer_io, Role::Client, None).await;
        let mut upstream_peer =
            WebSocketStream::from_raw_socket(upstream_peer_io, Role::Server, None).await;
        let (mut downstream_sink, mut downstream_stream) = downstream.split();
        let (mut upstream_sink, mut upstream_stream) = upstream.split();

        downstream_peer.send(Message::Close(None)).await.unwrap();
        upstream_peer.send(Message::Close(None)).await.unwrap();
        let close = downstream_stream.next().await.unwrap().unwrap();
        assert!(close.is_close());
        assert!(upstream_stream.next().await.unwrap().unwrap().is_close());

        finish_close(&mut downstream_sink, &mut upstream_sink, close).await;

        for peer in [&mut downstream_peer, &mut upstream_peer] {
            let reply = tokio::time::timeout(Duration::from_secs(1), peer.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert!(reply.is_close());
        }
    }
}
