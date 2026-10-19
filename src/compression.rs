use std::io;

use async_compression::tokio::bufread::{
    BrotliDecoder, GzipDecoder, GzipEncoder, ZlibDecoder, ZstdDecoder,
};
use axum::{body::Body, http::HeaderMap};
use tokio::io::{AsyncRead, AsyncReadExt, BufReader};
use tokio_util::io::ReaderStream;

/// Prefer gzip unless the client excludes it or explicitly prefers identity.
pub(crate) fn accepts_gzip(headers: &HeaderMap) -> crate::error::Result<bool> {
    let (mut gzip, mut identity, mut wildcard): (Option<f32>, Option<f32>, Option<f32>) =
        (None, None, None);
    for value in headers.get_all("accept-encoding") {
        let Ok(value) = value.to_str() else { continue };
        for item in value.split(',') {
            let mut parts = item.split(';').map(str::trim);
            let slot = match parts
                .next()
                .unwrap_or_default()
                .to_ascii_lowercase()
                .as_str()
            {
                "gzip" | "x-gzip" => &mut gzip,
                "identity" => &mut identity,
                "*" => &mut wildcard,
                _ => continue,
            };
            let quality = parts.next().map_or(1.0, |parameter| {
                parameter
                    .split_once('=')
                    .filter(|(name, _)| name.trim().eq_ignore_ascii_case("q"))
                    .and_then(|(_, value)| value.trim().parse::<f32>().ok())
                    .filter(|value| (0.0..=1.0).contains(value))
                    .unwrap_or(0.0)
            });
            // Conflicting duplicate declarations must not override an explicit exclusion.
            *slot = Some(slot.map_or(quality, |old| old.min(quality)));
        }
    }
    let gzip = gzip.or(wildcard).unwrap_or(0.0);
    if gzip > 0.0 && gzip >= identity.unwrap_or(0.0) {
        return Ok(true);
    }
    if identity.unwrap_or(if wildcard == Some(0.0) { 0.0 } else { 1.0 }) > 0.0 {
        return Ok(false);
    }
    Err(crate::error::Error::not_acceptable(
        "Catalog responses support gzip or identity encoding",
    ))
}

pub(crate) fn encode_body(bytes: Vec<u8>, gzip: bool) -> Body {
    if gzip {
        let encoder = GzipEncoder::new(BufReader::new(std::io::Cursor::new(bytes)));
        Body::from_stream(ReaderStream::new(encoder))
    } else {
        Body::from(bytes)
    }
}

/// Decode only bodies the router inspects, without collecting compressed input.
pub(crate) async fn decode_body<R: AsyncRead + Send + Unpin>(
    reader: R,
    headers: &HeaderMap,
) -> io::Result<Vec<u8>> {
    let mut reader = decode_reader(reader, headers)?;
    let mut decoded = Vec::new();
    reader.read_to_end(&mut decoded).await?;
    Ok(decoded)
}

pub(crate) fn decode_reader<'a, R: AsyncRead + Send + Unpin + 'a>(
    reader: R,
    headers: &HeaderMap,
) -> io::Result<Box<dyn AsyncRead + Send + Unpin + 'a>> {
    let mut reader: Box<dyn AsyncRead + Send + Unpin + 'a> = Box::new(reader);
    for value in headers.get_all("content-encoding").iter().rev() {
        for encoding in value
            .to_str()
            .map_err(io::Error::other)?
            .rsplit(',')
            .map(str::trim)
        {
            if encoding.eq_ignore_ascii_case("identity") {
                continue;
            }
            let buffered = BufReader::new(reader);
            reader = match encoding.to_ascii_lowercase().as_str() {
                "gzip" | "x-gzip" => {
                    let mut decoder = GzipDecoder::new(buffered);
                    decoder.multiple_members(true);
                    Box::new(decoder)
                }
                "deflate" => Box::new(ZlibDecoder::new(buffered)),
                "br" => Box::new(BrotliDecoder::new(buffered)),
                "zstd" => {
                    let mut decoder = ZstdDecoder::new(buffered);
                    decoder.multiple_members(true);
                    Box::new(decoder)
                }
                _ => return Err(io::Error::other("Unsupported response encoding")),
            };
        }
    }
    Ok(reader)
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_compression::tokio::bufread::{BrotliEncoder, GzipEncoder, ZlibEncoder, ZstdEncoder};
    use axum::body::Bytes;
    use std::io::Cursor;
    use tokio_util::io::StreamReader;

    async fn encode(bytes: &[u8], coding: &str) -> Vec<u8> {
        let input = BufReader::new(Cursor::new(bytes));
        let mut reader: Box<dyn AsyncRead + Send + Unpin + '_> = match coding {
            "gzip" => Box::new(GzipEncoder::new(input)),
            "deflate" => Box::new(ZlibEncoder::new(input)),
            "br" => Box::new(BrotliEncoder::new(input)),
            "zstd" => Box::new(ZstdEncoder::new(input)),
            _ => unreachable!(),
        };
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).await.unwrap();
        bytes
    }

    async fn decode_chunks(encoded: &[u8], headers: &HeaderMap) -> io::Result<Vec<u8>> {
        let chunks = encoded
            .iter()
            .map(|byte| Ok::<_, io::Error>(Bytes::copy_from_slice(&[*byte])));
        decode_body(
            StreamReader::new(futures_util::stream::iter(chunks)),
            headers,
        )
        .await
    }

    #[tokio::test]
    async fn supported_codings_decode_across_single_byte_chunks() {
        let original = "{\"models\":[{\"id\":\"vendor/雪\"}]}".as_bytes();
        let mut headers = HeaderMap::new();
        assert_eq!(decode_chunks(original, &headers).await.unwrap(), original);
        for (coding, header) in [
            ("gzip", "X-GZIP"),
            ("deflate", "Deflate"),
            ("br", "Br"),
            ("zstd", "zstd"),
        ] {
            let encoded = encode(original, coding).await;
            headers.insert("content-encoding", header.parse().unwrap());
            assert_eq!(decode_chunks(&encoded, &headers).await.unwrap(), original);
        }
    }

    #[tokio::test]
    async fn stacked_codings_and_concatenated_members_preserve_all_bytes() {
        let original = b"{\"error\":{\"message\":\"complete body\"}}";
        let mut headers = HeaderMap::new();
        let gzip = encode(original, "gzip").await;
        let stacked = encode(&gzip, "br").await;
        headers.append("content-encoding", "gzip".parse().unwrap());
        headers.append("content-encoding", "identity, br".parse().unwrap());
        assert_eq!(decode_chunks(&stacked, &headers).await.unwrap(), original);
        for coding in ["gzip", "zstd"] {
            let mut joined = encode(&original[..10], coding).await;
            joined.extend(encode(&original[10..], coding).await);
            headers.insert("content-encoding", coding.parse().unwrap());
            assert_eq!(decode_chunks(&joined, &headers).await.unwrap(), original);
        }
        headers.insert("content-encoding", "unsupported".parse().unwrap());
        assert!(decode_chunks(original, &headers).await.is_err());
    }
}
