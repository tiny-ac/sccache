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

use super::client::{AuthChannel, Client, RPC_TIMEOUT, with_timeout};
use super::digest;
use super::proto::build::bazel::remote::execution::v2 as remexec;
use super::proto::google::bytestream::ReadRequest;
use super::proto::google::bytestream::byte_stream_client::ByteStreamClient;
use crate::errors::*;
use sha2::{Digest as _, Sha256};
use std::io::{self, Write};
use std::time::Duration;
use tempfile::{NamedTempFile, TempPath};
use tokio::sync::mpsc;

/// The time to download `bytes` at 1 MiB/s, plus [`RPC_TIMEOUT`].
fn download_timeout(bytes: u64) -> Duration {
    RPC_TIMEOUT + Duration::from_secs(bytes / (1024 * 1024))
}

/// Error for a blob the REAPI CAS doesn't have, such as an evicted one.
#[derive(Debug)]
pub(crate) struct MissingBlob(remexec::Digest);

impl std::fmt::Display for MissingBlob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "the REAPI CAS doesn't have blob {}/{}",
            self.0.hash, self.0.size_bytes
        )
    }
}

impl std::error::Error for MissingBlob {}

/// Fails if `digest`'s hash isn't 64 lowercase hex digits, its size is negative, or it's
/// empty with another hash.
fn check_digest(digest: &remexec::Digest) -> Result<()> {
    let is_lower_hex = |c: char| c.is_ascii_digit() || ('a'..='f').contains(&c);
    if digest.size_bytes < 0
        || digest.hash.len() != 64
        || !digest.hash.chars().all(is_lower_hex)
        || (digest.size_bytes == 0 && *digest != digest::of_bytes(b""))
    {
        bail!("invalid REAPI digest {}/{}", digest.hash, digest.size_bytes);
    }
    Ok(())
}

/// The CAS of the REAPI server behind a [`Client`]. Downloads fail with [`MissingBlob`]
/// when the CAS doesn't have the blob.
pub(crate) struct Cas {
    instance_name: String,
    bytestream: ByteStreamClient<AuthChannel>,
    supports_zstd_bytestream: bool,
}

impl Cas {
    pub(crate) fn new(client: &Client) -> Self {
        Cas {
            instance_name: client.instance_name().to_owned(),
            bytestream: ByteStreamClient::new(client.channel()),
            supports_zstd_bytestream: client.supports_zstd_bytestream(),
        }
    }

    fn download_resource_name(&self, digest: &remexec::Digest) -> String {
        format!(
            "{}{}/{}/{}",
            instance_prefix(&self.instance_name),
            if self.supports_zstd_bytestream {
                "compressed-blobs/zstd"
            } else {
                "blobs"
            },
            digest.hash,
            digest.size_bytes
        )
    }

    /// The digest's size, or zstd's bound on its compressed size.
    fn max_received_bytes(&self, digest: &remexec::Digest) -> u64 {
        let size = digest.size_bytes as u64;
        if self.supports_zstd_bytestream {
            zstd::zstd_safe::compress_bound(size as usize) as u64
        } else {
            size
        }
    }

    pub(crate) async fn read_blob_to_memory(&self, digest: &remexec::Digest) -> Result<Vec<u8>> {
        self.read_blob_into(digest, vec![]).await
    }

    pub(crate) async fn read_blob_to_file(
        &self,
        digest: &remexec::Digest,
        file: NamedTempFile,
    ) -> Result<TempPath> {
        let path = file.path().to_owned();
        let file = self
            .read_blob_into(digest, io::BufWriter::new(file))
            .await?
            .into_inner()
            .map_err(|e| e.into_error())
            .with_context(|| format!("writing {}", path.display()))?;
        Ok(file.into_temp_path())
    }

    /// Downloads `digest`'s blob into `out`, failing if the bytes don't match `digest`. A
    /// blocking thread decompresses, hashes and writes each chunk.
    async fn read_blob_into<W: Write + Send + 'static>(
        &self,
        digest: &remexec::Digest,
        out: W,
    ) -> Result<W> {
        check_digest(digest)?;
        if digest.size_bytes == 0 {
            return Ok(out);
        }
        let resource_name = self.download_resource_name(digest);
        let max_received_bytes = self.max_received_bytes(digest);
        let status_to_error = |status: tonic::Status| match status.code() {
            tonic::Code::NotFound => Error::new(MissingBlob(digest.clone())),
            _ => status.into(),
        };
        let (tx, mut rx) = mpsc::channel::<Vec<u8>>(2);
        let (supports_zstd_bytestream, size_bytes) =
            (self.supports_zstd_bytestream, digest.size_bytes);
        let write_task = tokio::task::spawn_blocking(move || -> Result<(W, remexec::Digest)> {
            let mut hashing_writer = HashingWriter::new(out, size_bytes);
            {
                let mut sink: Box<dyn Write + '_> = if supports_zstd_bytestream {
                    Box::new(zstd::stream::write::Decoder::new(&mut hashing_writer)?)
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
            "downloading blob {}/{} from the REAPI CAS",
            digest.hash, digest.size_bytes
        );
        let read_result = with_timeout(
            &description,
            download_timeout(digest.size_bytes as u64),
            async {
                let mut stream = self
                    .bytestream
                    .clone()
                    .read(ReadRequest {
                        resource_name,
                        read_offset: 0,
                        read_limit: 0,
                    })
                    .await
                    .map_err(status_to_error)?
                    .into_inner();
                let mut received_bytes = 0;
                loop {
                    let chunk = tokio::select! {
                        message = stream.message() => message.map_err(status_to_error)?,
                        // The write task stopped on an error; `write_task.await` returns it.
                        _ = tx.closed() => break,
                    };
                    let Some(chunk) = chunk else { break };
                    received_bytes += chunk.data.len() as u64;
                    if received_bytes > max_received_bytes {
                        bail!("the REAPI server sent more than {max_received_bytes} bytes");
                    }
                    // The write task stopped on an error; `write_task.await` returns it.
                    if tx.send(chunk.data).await.is_err() {
                        break;
                    }
                }
                Ok(())
            },
        )
        .await;
        drop(tx);
        let write_result = write_task.await?;
        read_result?;
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
    len: i64,
    max_len: i64,
}

impl<W: Write> HashingWriter<W> {
    fn new(inner: W, max_len: i64) -> Self {
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
            size_bytes: self.len,
        };
        Ok((self.inner, digest))
    }
}

impl<W: Write> Write for HashingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.len + buf.len() as i64 > self.max_len {
            return Err(io::Error::other(format!(
                "the blob is more than {} bytes",
                self.max_len
            )));
        }
        let n = self.inner.write(buf)?;
        self.hasher.update(&buf[..n]);
        self.len += n as i64;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

fn instance_prefix(instance_name: &str) -> String {
    if instance_name.is_empty() {
        String::new()
    } else {
        format!("{instance_name}/")
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::reapi::fake::FakeServer;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use test_case::test_case;

    fn incompressible_bytes(len: usize) -> Vec<u8> {
        use rand::RngCore;
        let mut data = vec![0; len];
        rand::thread_rng().fill_bytes(&mut data);
        data
    }

    #[tokio::test]
    async fn test_empty_blob_needs_no_download() {
        let server = Arc::new(FakeServer::default());
        let cas = Cas::new(&server.connect().await);
        assert!(
            cas.read_blob_to_memory(&digest::of_bytes(b""))
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[test_case(false, "the REAPI server sent more than" ; "uncompressed")]
    #[test_case(true, "the blob is more than" ; "zstd")]
    #[tokio::test]
    async fn test_downloads_are_checked(supports_zstd_bytestream: bool, oversize_error: &str) {
        let server = Arc::new(FakeServer {
            supports_zstd_bytestream,
            ..Default::default()
        });
        let cas = Cas::new(&server.connect().await);
        let data = incompressible_bytes(2 * 1024 * 1024);
        let digest = server.insert_blob(&data);
        assert_eq!(data, cas.read_blob_to_memory(&digest).await.unwrap());
        let file = cas
            .read_blob_to_file(&digest, NamedTempFile::new().unwrap())
            .await
            .unwrap();
        assert_eq!(data, std::fs::read(&file).unwrap());

        server.corrupt_reads.store(true, Ordering::SeqCst);
        let e = cas
            .read_blob_to_file(&digest, NamedTempFile::new().unwrap())
            .await
            .unwrap_err();
        assert!(format!("{e:#}").contains("instead"), "{e:#}");
        server.corrupt_reads.store(false, Ordering::SeqCst);

        server.oversize_reads.store(true, Ordering::SeqCst);
        let e = cas.read_blob_to_memory(&digest).await.unwrap_err();
        assert!(format!("{e:#}").contains(oversize_error), "{e:#}");
    }

    #[tokio::test]
    async fn test_downloads_use_the_instance_name() {
        let server = Arc::new(FakeServer {
            instance_name: "main".to_owned(),
            ..Default::default()
        });
        let cas = Cas::new(&server.connect().await);
        let digest = server.insert_blob(b"blob");
        assert_eq!(
            b"blob".to_vec(),
            cas.read_blob_to_memory(&digest).await.unwrap()
        );
    }

    #[test_case(false ; "when reading starts")]
    #[test_case(true ; "in the stream")]
    #[tokio::test]
    async fn test_a_blob_the_cas_doesnt_have_is_missing_blob(not_found_in_stream: bool) {
        let server = Arc::new(FakeServer::default());
        server
            .not_found_in_stream
            .store(not_found_in_stream, Ordering::SeqCst);
        let cas = Cas::new(&server.connect().await);
        let e = cas
            .read_blob_to_memory(&digest::of_bytes(b"absent"))
            .await
            .unwrap_err();
        assert!(e.is::<MissingBlob>(), "{e:#}");
    }

    #[test]
    fn test_download_timeout_grows_with_the_blob() {
        assert_eq!(RPC_TIMEOUT, download_timeout(0));
        assert_eq!(
            RPC_TIMEOUT + Duration::from_secs(10),
            download_timeout(10 * 1024 * 1024)
        );
    }

    #[test_case("../../etc/passwd", 5 ; "not a sha256 hash")]
    #[test_case(&digest::of_bytes(b"x").hash, -1 ; "negative size")]
    #[test_case(&digest::of_bytes(b"x").hash, 0 ; "empty size with a non-empty hash")]
    #[tokio::test]
    async fn test_an_invalid_digest_is_refused(hash: &str, size_bytes: i64) {
        let server = Arc::new(FakeServer::default());
        let cas = Cas::new(&server.connect().await);
        let invalid = remexec::Digest {
            hash: hash.to_owned(),
            size_bytes,
        };
        let e = cas.read_blob_to_memory(&invalid).await.unwrap_err();
        assert!(e.to_string().contains("invalid REAPI digest"), "{e}");
    }
}
