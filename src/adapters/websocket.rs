use std::{
    collections::VecDeque,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use axum::http::{HeaderMap, Method, Request, Uri};
use futures_util::{FutureExt, Sink, SinkExt, Stream, StreamExt, future::poll_fn};
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

enum SessionEnd {
    Downstream(Message),
    Upstream(Message),
    Switch(Box<WebSocketSwitch>),
}

pub(crate) async fn relay(
    downstream: TokioIo<DownstreamUpgraded>,
    upstream: reqwest::Upgraded,
    client: Client,
    mut context: WebSocketContext,
) {
    let config = WebSocketConfig::default()
        .max_message_size(None)
        .max_frame_size(None);
    let downstream = WebSocketStream::from_raw_socket(downstream, Role::Server, Some(config)).await;
    let (mut downstream_sink, mut downstream_stream) = downstream.split();
    let mut upstream = upstream;
    let mut pending = VecDeque::new();
    // Local responses belong to the client session, including across switches.
    let (local_tx, mut local_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut jobs = FuturesUnordered::new();
    let mut outgoing = VecDeque::new();
    loop {
        let socket = WebSocketStream::from_raw_socket(upstream, Role::Client, Some(config)).await;
        let (mut upstream_sink, mut upstream_stream) = socket.split();
        let end = tokio::select! {
            result = forward_requests(
                &mut downstream_stream, &mut upstream_sink, client, &context, &mut pending, &local_tx,
            ) => result,
            result = forward_responses(
                &mut upstream_stream, &mut downstream_sink, client, &mut local_rx, &mut jobs, &mut outgoing,
            ) => result.map(SessionEnd::Upstream),
        };
        let change = match end {
            Some(SessionEnd::Downstream(close)) => {
                finish_close(&mut downstream_sink, &mut upstream_sink, close).await;
                return;
            }
            Some(SessionEnd::Upstream(close)) => {
                finish_close(&mut upstream_sink, &mut downstream_sink, close).await;
                return;
            }
            Some(SessionEnd::Switch(change)) => *change,
            None => return,
        };
        let WebSocketSwitch {
            context: next,
            request,
            opening,
        } = change;
        // Retire both halves immediately, abandoning old-provider writes,
        // before polling the replacement handshake.
        drop(upstream_sink);
        drop(upstream_stream);
        let mut replacing = opening;
        let mut idle = futures_util::stream::pending();
        let result = loop {
            tokio::select! {
                result = &mut replacing => break result,
                _ = forward_responses(
                    &mut idle, &mut downstream_sink, client, &mut local_rx, &mut jobs, &mut outgoing,
                ) => return,
                incoming = downstream_stream.next() => {
                    match incoming {
                        Some(Ok(Message::Ping(_))) => {}
                        Some(Ok(Message::Close(_))) => {
                            let _ = tokio::time::timeout(
                                Duration::from_secs(1), downstream_sink.flush(),
                            ).await;
                            return;
                        }
                        Some(Ok(message)) => pending.push_back(message),
                        _ => return,
                    }
                }
            }
        };
        match result {
            Ok(connection) => {
                upstream = connection;
                context = next;
                // Preparation runs against the new connection exactly once.
                pending.push_front(request);
            }
            Err(messages) => {
                for message in messages {
                    if downstream_sink.send(message).await.is_err() {
                        break;
                    }
                }
                return;
            }
        }
    }
}

async fn forward_requests<S, D>(
    source: &mut SplitStream<WebSocketStream<S>>,
    destination: &mut SplitSink<WebSocketStream<D>, Message>,
    client: Client,
    context: &WebSocketContext,
    pending: &mut VecDeque<Message>,
    local_tx: &tokio::sync::mpsc::UnboundedSender<ResponseJob>,
) -> Option<SessionEnd>
where
    S: AsyncRead + AsyncWrite + Unpin,
    D: AsyncRead + AsyncWrite + Unpin,
{
    let mut preparation: Option<BoxFuture<'_, WebSocketAction>> = None;
    let mut outbound = VecDeque::new();
    let mut writing = false;
    loop {
        if preparation.is_none()
            && let Some(message) = pending.pop_front()
        {
            preparation = Some(client.websocket_request(message, context).boxed());
        }
        tokio::select! {
            incoming = source.next() => {
                let Some(Ok(message)) = incoming else { return None };
                match message {
                    Message::Ping(_) => continue,
                    close @ Message::Close(_) => return Some(SessionEnd::Downstream(close)),
                    message => pending.push_back(message),
                }
            }
            action = async { preparation.as_mut().unwrap().await }, if preparation.is_some() => {
                preparation = None;
                match action {
                    WebSocketAction::Forward(message) => outbound.push_back(message),
                    WebSocketAction::Respond(job) => local_tx.send(job).ok()?,
                    WebSocketAction::Switch(change) => return Some(SessionEnd::Switch(change)),
                }
            }
            result = poll_fn(|cx| poll_write(destination, &mut outbound, &mut writing, cx)),
                if writing || !outbound.is_empty() => {
                result.ok()?;
            }
        }
    }
}

async fn forward_responses<S, D>(
    source: &mut S,
    destination: &mut SplitSink<WebSocketStream<D>, Message>,
    client: Client,
    local_rx: &mut tokio::sync::mpsc::UnboundedReceiver<ResponseJob>,
    jobs: &mut FuturesUnordered<ResponseJob>,
    outgoing: &mut VecDeque<Message>,
) -> Option<Message>
where
    S: Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
    D: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        // Unlike send(), feed() has no await after accepting the frame. Keep it
        // queued until then, so a provider switch cannot lose a partial send.
        while let Some(message) = outgoing.front() {
            destination.feed(message.clone()).await.ok()?;
            outgoing.pop_front();
        }
        destination.flush().await.ok()?;
        tokio::select! {
            incoming = source.next() => {
                let message = match incoming {
                    Some(Ok(message)) if !message.is_close() => message,
                    closed => return closed.and_then(Result::ok),
                };
                match message {
                    Message::Ping(_) => continue,
                    message => {
                        outgoing.push_back(client.adapt_websocket_response(message));
                    }
                }
            }
            job = local_rx.recv() => match job {
                Some(job) => jobs.push(job),
                None => return None,
            },
            Some(messages) = jobs.next(), if !jobs.is_empty() => outgoing.extend(messages),
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
    async fn replacing_upstream_preserves_partly_sent_local_responses() {
        let (io, peer_io) = tokio::io::duplex(64);
        let socket = WebSocketStream::from_raw_socket(
            io,
            Role::Server,
            Some(WebSocketConfig::default().write_buffer_size(0)),
        )
        .await;
        let (mut sink, _) = socket.split();
        let mut peer = WebSocketStream::from_raw_socket(peer_io, Role::Client, None).await;
        let mut upstream = futures_util::stream::pending();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let mut jobs = FuturesUnordered::new();
        let expected = [
            Message::Text("x".repeat(8192).into()),
            Message::Text("tail".into()),
            Message::Text("queued".into()),
        ];
        let mut outgoing = VecDeque::from(expected.clone());
        {
            let forwarding = forward_responses(
                &mut upstream,
                &mut sink,
                Client::Codex,
                &mut rx,
                &mut jobs,
                &mut outgoing,
            );
            tokio::pin!(forwarding);
            assert!(futures_util::poll!(forwarding).is_pending());
        }
        // Frames remain both in the socket's write buffers and in our queue.
        assert_eq!(outgoing.len(), 1);
        tokio::time::timeout(Duration::from_secs(1), async {
            tokio::select! {
                _ = forward_responses(
                    &mut upstream, &mut sink, Client::Codex, &mut rx, &mut jobs, &mut outgoing,
                ) => panic!("response forwarding ended"),
                _ = async {
                    for message in expected {
                        assert_eq!(peer.next().await.unwrap().unwrap(), message);
                    }
                    tx.send(async { vec![Message::Text("next".into())] }.boxed()).unwrap();
                    assert_eq!(peer.next().await.unwrap().unwrap(), Message::Text("next".into()));
                } => {}
            }
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
