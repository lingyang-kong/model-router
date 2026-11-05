use std::{
    collections::VecDeque,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use axum::http::{HeaderMap, Method, Request, Uri};
use futures_util::{FutureExt, Sink, SinkExt, StreamExt, future::poll_fn};
use futures_util::{
    future::BoxFuture,
    stream::{FuturesUnordered, SplitSink},
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

use crate::{adapters::Client, config::Provider, error::Error, router::RouterState};

#[derive(Clone)]
pub(crate) struct WebSocketContext {
    pub state: Arc<RouterState>,
    pub provider: Arc<Provider>,
    pub headers: HeaderMap,
    pub uri: Uri,
    /// The client's original handshake model, used when a request omits model.
    pub model: Option<String>,
    /// Credential owner, independent of the current upstream provider.
    pub origin: Option<Arc<Provider>>,
}

impl WebSocketContext {
    pub async fn connect(&self) -> crate::error::Result<reqwest::Response> {
        let mut request = Request::builder()
            .method(Method::GET)
            .uri(self.uri.clone())
            .body(())
            .map_err(Error::bad_request)?;
        *request.headers_mut() = self.headers.clone();
        let (mut parts, _) = request.into_parts();
        parts.headers.insert(
            "sec-websocket-key",
            tokio_tungstenite::tungstenite::handshake::client::generate_key()
                .parse()
                .unwrap(),
        );
        crate::websocket::connect(
            &self.state,
            &self.provider,
            self.origin.as_ref(),
            &parts,
            reqwest::Body::from(Vec::new()),
            true,
        )
        .await
    }
}

pub(crate) struct WebSocketSwitch {
    pub context: WebSocketContext,
    pub request: Message,
    // The adapter formats handshake errors for its protocol. The relay owns
    // when this future runs and the lifetime of the resulting connection.
    pub opening: BoxFuture<'static, Result<reqwest::Upgraded, Vec<Message>>>,
}

pub(crate) enum WebSocketAction {
    Forward(Message),
    Respond(ResponseJob),
    Switch(Box<WebSocketSwitch>),
}

type ResponseJob = BoxFuture<'static, Vec<Message>>;

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
    let (mut downstream_sink, mut downstream_stream) = downstream.split();
    let socket = WebSocketStream::from_raw_socket(upstream, Role::Client, Some(config)).await;
    let (sink, stream) = socket.split();
    let mut context = Arc::new(context);
    let mut upstream_sink = Some(sink);
    let mut upstream_stream = Some(stream);
    let mut pending = VecDeque::new();
    let mut preparation: Option<BoxFuture<'static, WebSocketAction>> = None;
    let mut replacing: Option<WebSocketSwitch> = None;
    let mut jobs = FuturesUnordered::new();
    let mut outbound = VecDeque::new();
    let mut outgoing = VecDeque::new();
    let mut upstream_writing = false;
    let mut downstream_writing = false;
    let mut ending = false;
    loop {
        if ending && outgoing.is_empty() && !downstream_writing {
            return;
        }
        if preparation.is_none()
            && upstream_sink.is_some()
            && !ending
            && let Some(message) = pending.pop_front()
        {
            let context = Arc::clone(&context);
            preparation =
                Some(async move { client.websocket_request(message, &context).await }.boxed());
        }
        tokio::select! {
            incoming = downstream_stream.next() => {
                let Some(Ok(message)) = incoming else { return };
                match message {
                    Message::Ping(_) => continue,
                    close @ Message::Close(_) => {
                        if let Some(sink) = upstream_sink.as_mut() {
                            finish_close(&mut downstream_sink, sink, close).await;
                        } else {
                            let _ = tokio::time::timeout(Duration::from_secs(1), downstream_sink.flush()).await;
                        }
                        return;
                    }
                    message => pending.push_back(message),
                }
            }
            action = async { preparation.as_mut().unwrap().await }, if preparation.is_some() => {
                preparation = None;
                match action {
                    WebSocketAction::Forward(message) => outbound.push_back(message),
                    WebSocketAction::Respond(job) => jobs.push(job),
                    WebSocketAction::Switch(change) => {
                        // A provider change abandons any writes accepted by the
                        // old sink. Retiring both halves first cancels the old
                        // inference without flushing those frames on the way out.
                        outbound.clear();
                        upstream_writing = false;
                        drop(upstream_sink.take().unwrap());
                        drop(upstream_stream.take().unwrap());
                        replacing = Some(*change);
                    }
                }
            }
            result = poll_fn(|cx| poll_write(upstream_sink.as_mut().unwrap(), &mut outbound, &mut upstream_writing, cx)),
                if upstream_sink.is_some() && (upstream_writing || !outbound.is_empty()) => {
                if result.is_err() { return; }
            }
            incoming = async { upstream_stream.as_mut().unwrap().next().await },
                if upstream_stream.is_some() && outgoing.is_empty() && !downstream_writing => {
                match incoming {
                    Some(Ok(close @ Message::Close(_))) => {
                        finish_close(upstream_sink.as_mut().unwrap(), &mut downstream_sink, close).await;
                        return;
                    }
                    Some(Ok(Message::Ping(_))) => {}
                    Some(Ok(message)) => outgoing.push_back(client.adapt_websocket_response(message)),
                    _ => return,
                }
            }
            result = async { replacing.as_mut().unwrap().opening.as_mut().await }, if replacing.is_some() => {
                let change = replacing.take().unwrap();
                match result {
                    Ok(connection) => {
                        let socket = WebSocketStream::from_raw_socket(connection, Role::Client, Some(config)).await;
                        let (sink, stream) = socket.split();
                        upstream_sink = Some(sink);
                        upstream_stream = Some(stream);
                        context = Arc::new(change.context);
                        // Preparation runs against the new connection exactly once.
                        pending.push_front(change.request);
                    }
                    Err(messages) => {
                        outgoing.extend(messages);
                        ending = true;
                    }
                }
            }
            Some(messages) = jobs.next(), if !jobs.is_empty() && !ending && outgoing.is_empty() && !downstream_writing => outgoing.extend(messages),
            result = poll_fn(|cx| poll_write(&mut downstream_sink, &mut outgoing, &mut downstream_writing, cx)),
                if downstream_writing || !outgoing.is_empty() => {
                if result.is_err() { return; }
            }
        }
    }
}

// Keep the frame in the queue until accepted, then remember the pending flush
// across select iterations. Neither cancellation nor a switch can resend it.
fn poll_write<S: Sink<Message> + Unpin>(
    sink: &mut S,
    messages: &mut VecDeque<Message>,
    started: &mut bool,
    cx: &mut Context<'_>,
) -> Poll<Result<(), S::Error>> {
    if !*started {
        match Pin::new(&mut *sink).poll_ready(cx) {
            Poll::Ready(Ok(())) => {
                let message = messages.pop_front().unwrap();
                Pin::new(&mut *sink).start_send(message)?;
                *started = true;
            }
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Pending => return Poll::Pending,
        }
    }
    match Pin::new(sink).poll_flush(cx) {
        Poll::Ready(result) => {
            *started = false;
            Poll::Ready(result)
        }
        Poll::Pending => Poll::Pending,
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
    async fn queued_writes_resume_after_cancellation_without_duplicates() {
        let (io, peer_io) = tokio::io::duplex(64);
        let socket = WebSocketStream::from_raw_socket(
            io,
            Role::Server,
            Some(WebSocketConfig::default().write_buffer_size(0)),
        )
        .await;
        let (mut sink, _) = socket.split();
        let mut peer = WebSocketStream::from_raw_socket(peer_io, Role::Client, None).await;
        let expected = [
            Message::Text("x".repeat(8192).into()),
            Message::Text("tail".into()),
            Message::Text("queued".into()),
        ];
        let mut outgoing = VecDeque::from(expected.clone());
        let mut started = false;
        assert!(
            futures_util::poll!(poll_fn(|cx| poll_write(
                &mut sink,
                &mut outgoing,
                &mut started,
                cx
            )))
            .is_pending()
        );
        assert!(started);
        assert_eq!(outgoing.len(), 2);
        tokio::time::timeout(Duration::from_secs(1), async {
            tokio::join!(
                async {
                    while started || !outgoing.is_empty() {
                        poll_fn(|cx| poll_write(&mut sink, &mut outgoing, &mut started, cx))
                            .await
                            .unwrap();
                    }
                },
                async {
                    for message in expected {
                        assert_eq!(peer.next().await.unwrap().unwrap(), message);
                    }
                }
            );
        })
        .await
        .unwrap();
    }

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
