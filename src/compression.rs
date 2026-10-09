use std::io;

use async_compression::tokio::bufread::{BrotliDecoder, GzipDecoder, ZlibDecoder, ZstdDecoder};
use axum::http::HeaderMap;
use tokio::io::{AsyncRead, AsyncReadExt, BufReader};

/// Decode only bodies the router inspects, without collecting compressed input.
pub(crate) async fn decode_body<R: AsyncRead + Send + Unpin>(
    reader: R,
    headers: &HeaderMap,
) -> io::Result<Vec<u8>> {
    let mut reader: Box<dyn AsyncRead + Send + Unpin + '_> = Box::new(reader);
    let mut encodings = Vec::new();
    for value in headers.get_all("content-encoding") {
        encodings.extend(
            value
                .to_str()
                .map_err(io::Error::other)?
                .split(',')
                .map(str::trim),
        );
    }
    for encoding in encodings.into_iter().rev() {
        let buffered = BufReader::new(reader);
        reader = match encoding.to_ascii_lowercase().as_str() {
            "identity" => Box::new(buffered),
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
    let mut decoded = Vec::new();
    reader.read_to_end(&mut decoded).await?;
    Ok(decoded)
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
