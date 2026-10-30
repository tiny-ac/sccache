// Copyright 2026 Mozilla Foundation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Downloads blobs from a REAPI server's content-addressable storage (CAS) and checks
//! them against their digests.

use super::client::{AuthChannel, Client, RPC_TIMEOUT, status_error, with_timeout};
use super::digest;
use super::proto::build::bazel::remote::execution::v2 as remexec;
use super::proto::google::bytestream::ReadRequest;
use super::proto::google::bytestream::byte_stream_client::ByteStreamClient;
use crate::errors::*;
use sha2::{Digest as _, Sha256};
use std::io::{self, Write};
use std::time::Duration;
use tokio::sync::mpsc;

/// Allows for links as slow as 1 MiB/s.
fn download_timeout(bytes: u64) -> Duration {
    RPC_TIMEOUT + Duration::from_secs(bytes / (1024 * 1024))
}

/// The zstd window a download's decoder accepts. A server that compresses as it streams may
/// not tell the encoder the blob's size, so small blobs still get a 16 MiB window.
fn zstd_window_log_max(size_bytes: u64) -> u32 {
    (u64::BITS - size_bytes.saturating_sub(1).leading_zeros()).max(24)
}

/// The CAS of the REAPI server behind a [`Client`].
pub(crate) struct Cas {
    instance_name: String,
    bytestream_client: ByteStreamClient<AuthChannel>,
    supports_zstd_bytestream: bool,
}

impl Cas {
    pub(crate) fn new(client: &Client) -> Self {
        Cas {
            instance_name: client.instance_name().to_owned(),
            bytestream_client: ByteStreamClient::new(client.channel()),
            supports_zstd_bytestream: client.supports_zstd_bytestream(),
        }
    }

    fn download_resource_name(&self, digest: &remexec::Digest) -> String {
        let blobs = if self.supports_zstd_bytestream {
            "compressed-blobs/zstd"
        } else {
            "blobs"
        };
        let blob_name = format!("{blobs}/{}/{}", digest.hash, digest.size_bytes);
        if self.instance_name.is_empty() {
            blob_name
        } else {
            format!("{}/{blob_name}", self.instance_name)
        }
    }

    /// Fails without downloading if `digest` says the blob is larger than `max_bytes`.
    pub(crate) async fn download_blob(
        &self,
        digest: &remexec::Digest,
        max_bytes: u64,
    ) -> Result<Vec<u8>> {
        self.download_blob_into(digest, max_bytes, vec![]).await
    }

    /// Downloads `digest`'s blob into `out`, failing if the bytes don't match `digest`. A
    /// blocking thread decompresses, hashes and writes each chunk. Fails without downloading
    /// if `digest` says the blob is larger than `max_bytes`.
    pub(super) async fn download_blob_into<W: Write + Send + 'static>(
        &self,
        digest: &remexec::Digest,
        max_bytes: u64,
        out: W,
    ) -> Result<W> {
        digest::check(digest)?;
        let size_bytes = digest.size_bytes as u64;
        if size_bytes > max_bytes {
            bail!(
                "blob {}/{} in the REAPI CAS is larger than {max_bytes} bytes",
                digest.hash,
                digest.size_bytes
            );
        }
        if size_bytes == 0 {
            return Ok(out);
        }
        let resource_name = self.download_resource_name(digest);
        let (tx, mut rx) = mpsc::channel::<Vec<u8>>(2);
        let supports_zstd_bytestream = self.supports_zstd_bytestream;
        let write_task = tokio::task::spawn_blocking(move || -> Result<(W, remexec::Digest)> {
            let mut hashing_writer = HashingWriter::new(out, size_bytes);
            {
                let mut sink: Box<dyn Write + '_> = if supports_zstd_bytestream {
                    let mut decoder = zstd::stream::write::Decoder::new(&mut hashing_writer)?;
                    decoder.window_log_max(zstd_window_log_max(size_bytes))?;
                    Box::new(decoder)
                } else {
                    Box::new(&mut hashing_writer)
                };
                while let Some(chunk) = rx.blocking_recv() {
                    sink.write_all(&chunk)?;
                }
                sink.flush()?;
            }
            hashing_writer.finish()
        });
        let description = format!(
            "failed to download blob {}/{} from the REAPI CAS",
            digest.hash, digest.size_bytes
        );
        let download_result = with_timeout(&description, download_timeout(size_bytes), async {
            let mut stream = self
                .bytestream_client
                .clone()
                .read(ReadRequest {
                    resource_name,
                    read_offset: 0,
                    read_limit: 0,
                })
                .await
                .map_err(status_error)?
                .into_inner();
            while let Some(chunk) = stream.message().await.map_err(status_error)? {
                // The write task stopped on an error; `write_task.await` returns it.
                if tx.send(chunk.data).await.is_err() {
                    break;
                }
            }
            Ok(())
        })
        .await;
        drop(tx);
        let write_result = write_task.await?;
        download_result?;
        let (out, received_digest) = write_result.with_context(|| description.clone())?;
        if received_digest != *digest {
            bail!(
                "{description}: the REAPI server sent blob {}/{} instead",
                received_digest.hash,
                received_digest.size_bytes
            );
        }
        Ok(out)
    }
}

/// Hashes what passes through to `inner`, failing writes past `max_len` bytes.
struct HashingWriter<W> {
    inner: W,
    hasher: Sha256,
    len: u64,
    max_len: u64,
}

impl<W: Write> HashingWriter<W> {
    fn new(inner: W, max_len: u64) -> Self {
        HashingWriter {
            inner,
            hasher: Sha256::new(),
            len: 0,
            max_len,
        }
    }

    /// Flushes `inner` and returns it with the digest of what passed through.
    fn finish(mut self) -> Result<(W, remexec::Digest)> {
        self.inner.flush()?;
        let digest = remexec::Digest {
            hash: format!("{:x}", self.hasher.finalize()),
            size_bytes: self.len as i64,
        };
        Ok((self.inner, digest))
    }
}

impl<W: Write> Write for HashingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.len + buf.len() as u64 > self.max_len {
            return Err(io::Error::other(format!(
                "the blob is more than {} bytes",
                self.max_len
            )));
        }
        let n = self.inner.write(buf)?;
        self.hasher.update(&buf[..n]);
        self.len += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::reapi::fake::FakeServer;
    use rand::{RngCore, SeedableRng};
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use test_case::test_case;

    const MAX_BYTES: u64 = 1 << 20;

    /// More than one 1000-byte chunk, which zstd can't shrink.
    fn incompressible_bytes() -> Vec<u8> {
        let mut data = vec![0; 64 * 1024];
        rand::rngs::StdRng::seed_from_u64(0).fill_bytes(&mut data);
        data
    }

    async fn cas_and_server(supports_zstd_bytestream: bool) -> (Cas, Arc<FakeServer>) {
        let server = Arc::new(FakeServer {
            supports_zstd_bytestream,
            ..Default::default()
        });
        (Cas::new(&server.serve_and_connect().await), server)
    }

    #[tokio::test]
    async fn test_empty_blob_needs_no_download() {
        let (cas, _server) = cas_and_server(false).await;
        assert!(
            cas.download_blob(&digest::of_bytes(b""), MAX_BYTES)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[test_case(false ; "uncompressed")]
    #[test_case(true ; "zstd")]
    #[tokio::test]
    async fn test_a_download_gives_the_blob(supports_zstd_bytestream: bool) {
        let (cas, server) = cas_and_server(supports_zstd_bytestream).await;
        let data = incompressible_bytes();
        let digest = server.insert_blob(&data);
        assert_eq!(data, cas.download_blob(&digest, MAX_BYTES).await.unwrap());
    }

    #[test_case(false ; "uncompressed")]
    #[test_case(true ; "zstd")]
    #[tokio::test]
    async fn test_a_corrupted_blob_is_an_error(supports_zstd_bytestream: bool) {
        let (cas, server) = cas_and_server(supports_zstd_bytestream).await;
        let digest = server.insert_blob(&incompressible_bytes());
        server.corrupt_reads.store(true, Ordering::SeqCst);
        let e = cas.download_blob(&digest, MAX_BYTES).await.unwrap_err();
        assert!(format!("{e:#}").contains("instead"), "{e:#}");
    }

    #[test_case(false ; "uncompressed")]
    #[test_case(true ; "zstd")]
    #[tokio::test]
    async fn test_a_blob_longer_than_its_digest_is_an_error(supports_zstd_bytestream: bool) {
        let (cas, server) = cas_and_server(supports_zstd_bytestream).await;
        let digest = server.insert_blob(&incompressible_bytes());
        server.oversize_reads.store(true, Ordering::SeqCst);
        let e = cas.download_blob(&digest, MAX_BYTES).await.unwrap_err();
        assert!(format!("{e:#}").contains("the blob is more than"), "{e:#}");
    }

    #[tokio::test]
    async fn test_a_blob_larger_than_max_bytes_is_not_downloaded() {
        let (cas, server) = cas_and_server(false).await;
        let digest = server.insert_blob(b"blob");
        let e = cas.download_blob(&digest, 3).await.unwrap_err();
        assert!(e.to_string().contains("larger than 3 bytes"), "{e}");
    }

    #[tokio::test]
    async fn test_a_write_error_fails_the_download() {
        struct FailingWriter;
        impl Write for FailingWriter {
            fn write(&mut self, _: &[u8]) -> io::Result<usize> {
                Err(io::Error::other("disk full"))
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let (cas, server) = cas_and_server(false).await;
        let digest = server.insert_blob(&incompressible_bytes());
        let e = cas
            .download_blob_into(&digest, MAX_BYTES, FailingWriter)
            .await
            .err()
            .unwrap();
        assert!(format!("{e:#}").contains("disk full"), "{e:#}");
    }

    #[tokio::test]
    async fn test_downloads_use_the_instance_name() {
        let server = Arc::new(FakeServer {
            instance_name: "main".to_owned(),
            ..Default::default()
        });
        let cas = Cas::new(&server.serve_and_connect().await);
        let digest = server.insert_blob(b"blob");
        assert_eq!(
            b"blob".to_vec(),
            cas.download_blob(&digest, MAX_BYTES).await.unwrap()
        );
    }

    #[tokio::test]
    async fn test_a_blob_the_cas_doesnt_have_is_an_error() {
        let (cas, _server) = cas_and_server(false).await;
        let e = cas
            .download_blob(&digest::of_bytes(b"absent"), MAX_BYTES)
            .await
            .unwrap_err();
        assert!(format!("{e:#}").contains("NotFound"), "{e:#}");
    }

    #[test]
    fn test_zstd_window_log_max() {
        assert_eq!(24, zstd_window_log_max(0));
        assert_eq!(24, zstd_window_log_max(1 << 24));
        assert_eq!(25, zstd_window_log_max((1 << 24) + 1));
        assert_eq!(27, zstd_window_log_max(100 << 20));
    }
}
