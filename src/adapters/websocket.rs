use std::{sync::Arc, time::Duration};

use axum::http::{HeaderMap, Uri};

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
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

pub(crate) struct WebSocketContext {
    pub state: Arc<RouterState>,
    pub provider: Arc<Provider>,
    pub headers: HeaderMap,
    pub uri: Uri,
    pub model: Option<String>,
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
    let (from_downstream, close) = tokio::select! {
        result = forward_messages(
            &mut downstream_stream, &mut upstream_sink, true, client, &context,
        ) => (true, result),
        result = forward_messages(
            &mut upstream_stream, &mut downstream_sink, false, client, &context,
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

async fn forward_messages<S, D>(
    source: &mut SplitStream<WebSocketStream<S>>,
    destination: &mut SplitSink<WebSocketStream<D>, Message>,
    request: bool,
    client: Client,
    context: &WebSocketContext,
) -> Option<Message>
where
    S: AsyncRead + AsyncWrite + Unpin,
    D: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        let Some(Ok(message)) = source.next().await else {
            return None;
        };
        match message {
            // Each WebSocket hop answers its own pings through tungstenite's
            // automatic pong queue. Do not forward pings and create duplicate replies.
            Message::Ping(_) => continue,
            close @ Message::Close(_) => return Some(close),
            message => {
                let message = client
                    .adapt_websocket_message(message, request, context)
                    .await;
                destination.send(message).await.ok()?;
            }
        }
    }
}
