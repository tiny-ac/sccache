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

//! Uploads blobs to a REAPI server's content-addressable storage (CAS), and downloads blobs
//! from it and checks them against their digests.

use super::client::{AuthChannel, Client, RPC_TIMEOUT, status_error, with_timeout};
use super::digest;
use super::proto::build::bazel::remote::execution::v2 as remexec;
use super::proto::google::bytestream::ReadRequest;
use super::proto::google::bytestream::byte_stream_client::ByteStreamClient;
use crate::errors::*;
use remexec::content_addressable_storage_client::ContentAddressableStorageClient;
use sha2::{Digest as _, Sha256};
use std::collections::HashSet;
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::mpsc;

/// The largest request gRPC servers take by default.
const GRPC_MAX_MESSAGE_BYTES: usize = 4 * 1024 * 1024;
/// Room for each blob's digest and field tags in a batch, which take about 100 bytes.
#[cfg_attr(not(test), expect(dead_code, reason = "Execute uploads through it"))]
const BATCH_OVERHEAD_PER_BLOB: usize = 256;
/// Keeps a FindMissingBlobs request near 1 MiB, at about 100 bytes per digest.
#[cfg_attr(not(test), expect(dead_code, reason = "Execute uploads through it"))]
const DIGESTS_PER_FIND_MISSING_REQUEST: usize = 10_000;
/// Uploads in flight at once, across [`Cas::ensure_uploaded`] calls.
const UPLOAD_CONCURRENCY: usize = 4;
/// Bounds `present_blobs`; clearing it costs only a FindMissingBlobs call.
const MAX_PRESENT_BLOBS: usize = 100_000;

/// Allows for links as slow as 1 MiB/s.
fn download_timeout(bytes: u64) -> Duration {
    RPC_TIMEOUT + Duration::from_secs(bytes / (1024 * 1024))
}

/// Allows each of [`UPLOAD_CONCURRENCY`] uploads an equal share of a 1 MiB/s link.
#[cfg_attr(not(test), expect(dead_code, reason = "Execute uploads through it"))]
fn upload_timeout(bytes: u64) -> Duration {
    download_timeout(bytes.saturating_mul(UPLOAD_CONCURRENCY as u64))
}

/// Error for a local file that can't be read to upload it.
#[derive(Debug)]
#[cfg_attr(not(test), expect(dead_code, reason = "Execute uploads through it"))]
pub(crate) struct UnreadableBlob(PathBuf);

impl std::fmt::Display for UnreadableBlob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "failed to read {} to upload it", self.0.display())
    }
}

impl std::error::Error for UnreadableBlob {}

/// A blob to upload.
#[cfg_attr(not(test), expect(dead_code, reason = "Execute uploads through it"))]
#[derive(Debug, Clone)]
pub(crate) enum Blob {
    Bytes(Vec<u8>),
    File(PathBuf),
}

#[cfg_attr(not(test), expect(dead_code, reason = "Execute uploads through it"))]
impl Blob {
    fn read(self) -> Result<Vec<u8>> {
        match self {
            Blob::Bytes(bytes) => Ok(bytes),
            Blob::File(path) => std::fs::read(&path).with_context(|| UnreadableBlob(path)),
        }
    }
}

/// The zstd window a download's decoder accepts. A server that compresses as it streams may
/// not tell the encoder the blob's size, so small blobs still get a 16 MiB window.
fn zstd_window_log_max(size_bytes: u64) -> u32 {
    (u64::BITS - size_bytes.saturating_sub(1).leading_zeros()).max(24)
}

/// The CAS of the REAPI server behind a [`Client`].
#[cfg_attr(not(test), expect(dead_code, reason = "Execute uploads through it"))]
pub(crate) struct Cas {
    instance_name: String,
    cas_client: ContentAddressableStorageClient<AuthChannel>,
    bytestream_client: ByteStreamClient<AuthChannel>,
    max_batch_bytes: usize,
    supports_zstd_bytestream: bool,
    /// Blobs known to be in the CAS, so blobs every action shares, such as the toolchain's,
    /// aren't looked up again. Cleared when it would exceed `max_present_blobs`.
    present_blobs: Mutex<HashSet<remexec::Digest>>,
    max_present_blobs: usize,
    upload_permits: tokio::sync::Semaphore,
}

impl Cas {
    pub(crate) fn new(client: &Client) -> Self {
        Cas {
            instance_name: client.instance_name().to_owned(),
            cas_client: ContentAddressableStorageClient::new(client.channel()),
            bytestream_client: ByteStreamClient::new(client.channel()),
            // A server may advertise more than its gRPC transport takes.
            max_batch_bytes: client.max_batch_bytes().min(GRPC_MAX_MESSAGE_BYTES),
            supports_zstd_bytestream: client.supports_zstd_bytestream(),
            present_blobs: Mutex::new(HashSet::new()),
            max_present_blobs: MAX_PRESENT_BLOBS,
            upload_permits: tokio::sync::Semaphore::new(UPLOAD_CONCURRENCY),
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

#[cfg_attr(not(test), expect(dead_code, reason = "Execute uploads through it"))]
impl Cas {
    fn is_known_present(&self, digest: &remexec::Digest) -> bool {
        // The CAS always has the empty blob.
        digest.size_bytes == 0 || self.present_blobs.lock().unwrap().contains(digest)
    }

    /// Uploads whichever of `blobs` the CAS doesn't have, in batches. A local file that can't
    /// be read fails with [`UnreadableBlob`]. Concurrent calls with the same blob may both
    /// upload it.
    pub(crate) async fn ensure_uploaded(&self, blobs: Vec<(remexec::Digest, Blob)>) -> Result<()> {
        let mut seen = HashSet::new();
        let candidates: Vec<(remexec::Digest, Blob)> = blobs
            .into_iter()
            .filter(|(digest, _)| !self.is_known_present(digest) && seen.insert(digest.clone()))
            .collect();
        if candidates.is_empty() {
            return Ok(());
        }
        let digests: Vec<remexec::Digest> = candidates
            .iter()
            .map(|(digest, _)| digest.clone())
            .collect();
        let missing = self.find_missing(&digests).await?;
        // Room for the request's instance name and digest function.
        let empty_batch_bytes = self.instance_name.len() + 16;
        let mut batches = vec![];
        let mut batch = vec![];
        let mut batch_bytes = empty_batch_bytes;
        for (digest, blob) in candidates.into_iter().filter(|(d, _)| missing.contains(d)) {
            let size = digest.size_bytes as usize + BATCH_OVERHEAD_PER_BLOB;
            if empty_batch_bytes + size > self.max_batch_bytes {
                bail!(
                    "failed to upload blob {}/{}: too large for a REAPI batch",
                    digest.hash,
                    digest.size_bytes
                );
            }
            if batch_bytes + size > self.max_batch_bytes {
                batches.push(std::mem::take(&mut batch));
                batch_bytes = empty_batch_bytes;
            }
            batch.push((digest, blob));
            batch_bytes += size;
        }
        if !batch.is_empty() {
            batches.push(batch);
        }
        futures::future::try_join_all(
            batches
                .into_iter()
                .map(|batch| self.with_upload_permit(self.batch_update_blobs(batch))),
        )
        .await?;
        let mut present_blobs = self.present_blobs.lock().unwrap();
        if present_blobs.len() + digests.len() > self.max_present_blobs {
            present_blobs.clear();
        }
        present_blobs.extend(digests);
        Ok(())
    }

    async fn with_upload_permit(&self, upload: impl Future<Output = Result<()>>) -> Result<()> {
        let _permit = self
            .upload_permits
            .acquire()
            .await
            .expect("the upload semaphore is never closed");
        upload.await
    }

    async fn find_missing(&self, digests: &[remexec::Digest]) -> Result<HashSet<remexec::Digest>> {
        let mut missing = HashSet::new();
        for chunk in digests.chunks(DIGESTS_PER_FIND_MISSING_REQUEST) {
            let request = remexec::FindMissingBlobsRequest {
                instance_name: self.instance_name.clone(),
                blob_digests: chunk.to_vec(),
                digest_function: remexec::digest_function::Value::Sha256 as i32,
            };
            let response = with_timeout(
                "failed to ask the REAPI CAS which blobs it lacks",
                RPC_TIMEOUT,
                async {
                    self.cas_client
                        .clone()
                        .find_missing_blobs(request)
                        .await
                        .map_err(status_error)
                },
            )
            .await?;
            missing.extend(response.into_inner().missing_blob_digests);
        }
        Ok(missing)
    }

    async fn batch_update_blobs(&self, blobs: Vec<(remexec::Digest, Blob)>) -> Result<()> {
        let requests = tokio::task::spawn_blocking(move || {
            blobs
                .into_iter()
                .map(|(digest, blob)| {
                    Ok(remexec::batch_update_blobs_request::Request {
                        digest: Some(digest),
                        data: blob.read()?,
                        compressor: remexec::compressor::Value::Identity as i32,
                    })
                })
                .collect::<Result<Vec<_>>>()
        })
        .await??;
        let mut unanswered: HashSet<remexec::Digest> = requests
            .iter()
            .filter_map(|request| request.digest.clone())
            .collect();
        let bytes: u64 = requests.iter().map(|r| r.data.len() as u64).sum();
        let request = remexec::BatchUpdateBlobsRequest {
            instance_name: self.instance_name.clone(),
            requests,
            digest_function: remexec::digest_function::Value::Sha256 as i32,
        };
        let description = format!(
            "failed to upload a batch of {} blobs to the REAPI CAS",
            unanswered.len()
        );
        let response = with_timeout(&description, upload_timeout(bytes), async {
            self.cas_client
                .clone()
                .batch_update_blobs(request)
                .await
                .map_err(status_error)
        })
        .await?
        .into_inner();
        for response in response.responses {
            let digest = response.digest.unwrap_or_default();
            if let Some(status) = response.status.filter(|status| status.code != 0) {
                bail!(
                    "{description}: the REAPI server refused blob {}/{}: {:?}: {}",
                    digest.hash,
                    digest.size_bytes,
                    tonic::Code::from(status.code),
                    status.message
                );
            }
            unanswered.remove(&digest);
        }
        if let Some(digest) = unanswered.iter().next() {
            bail!(
                "{description}: the REAPI server didn't answer for blob {}/{}",
                digest.hash,
                digest.size_bytes
            );
        }
        Ok(())
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
        cas_with(FakeServer {
            supports_zstd_bytestream,
            ..Default::default()
        })
        .await
    }

    fn blob(data: &[u8]) -> (remexec::Digest, Blob) {
        (digest::of_bytes(data), Blob::Bytes(data.to_vec()))
    }

    async fn cas_with(server: FakeServer) -> (Cas, Arc<FakeServer>) {
        let server = Arc::new(server);
        (Cas::new(&server.serve_and_connect().await), server)
    }

    #[tokio::test]
    async fn test_uploads_only_what_the_cas_lacks() {
        let (cas, server) = cas_with(FakeServer {
            instance_name: "main".to_owned(),
            ..Default::default()
        })
        .await;
        server.insert_blob(b"present");
        cas.ensure_uploaded(vec![blob(b"present"), blob(b"new"), blob(b"new")])
            .await
            .unwrap();
        assert!(server.has_blob(b"new"));
        assert_eq!(1, server.batch_update_blobs_calls.load(Ordering::SeqCst));
        // "new" is uploaded once.
        assert_eq!(1, server.batched_blob_count.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_known_present_blobs_are_not_looked_up_again() {
        let (cas, server) = cas_with(FakeServer::default()).await;
        server.insert_blob(b"present");
        cas.ensure_uploaded(vec![blob(b"present"), blob(b"new")])
            .await
            .unwrap();
        cas.ensure_uploaded(vec![blob(b"present"), blob(b"new")])
            .await
            .unwrap();
        assert_eq!(1, server.find_missing_blobs_calls.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_a_full_set_of_present_blobs_is_cleared() {
        let (mut cas, server) = cas_with(FakeServer::default()).await;
        cas.max_present_blobs = 1;
        cas.ensure_uploaded(vec![blob(b"a")]).await.unwrap();
        cas.ensure_uploaded(vec![blob(b"b")]).await.unwrap();
        cas.ensure_uploaded(vec![blob(b"b")]).await.unwrap();
        assert_eq!(2, server.find_missing_blobs_calls.load(Ordering::SeqCst));
        // Adding "b" cleared "a".
        cas.ensure_uploaded(vec![blob(b"a")]).await.unwrap();
        assert_eq!(3, server.find_missing_blobs_calls.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_batches_stay_under_the_server_limit() {
        let (cas, server) = cas_with(FakeServer {
            max_batch_bytes: 3 * 1024,
            ..Default::default()
        })
        .await;
        let blobs: Vec<Vec<u8>> = (0..3u8).map(|i| vec![i; 1000]).collect();
        let to_upload: Vec<_> = blobs.iter().map(|data| blob(data)).collect();
        cas.ensure_uploaded(to_upload).await.unwrap();
        assert!(blobs.iter().all(|data| server.has_blob(data)));
        // Two blobs of 1000 bytes and their overhead fit in 3 KiB; three don't.
        assert_eq!(2, server.batch_update_blobs_calls.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_batches_leave_room_for_the_instance_name() {
        let (cas, server) = cas_with(FakeServer {
            instance_name: "i".repeat(200),
            max_batch_bytes: 3 * 1024,
            ..Default::default()
        })
        .await;
        // Two of these and their overhead fit in 3 KiB only without the instance name.
        cas.ensure_uploaded(vec![blob(&[0; 1250]), blob(&[1; 1250])])
            .await
            .unwrap();
        assert_eq!(2, server.batch_update_blobs_calls.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_batches_stay_under_the_grpc_limit() {
        let (cas, server) = cas_with(FakeServer {
            max_batch_bytes: 16 * 1024 * 1024,
            ..Default::default()
        })
        .await;
        // The fake server, like most, takes requests of up to 4 MiB.
        let blobs: Vec<Vec<u8>> = (0..3u8).map(|i| vec![i; 1536 * 1024]).collect();
        let to_upload: Vec<_> = blobs.iter().map(|data| blob(data)).collect();
        cas.ensure_uploaded(to_upload).await.unwrap();
        assert!(blobs.iter().all(|data| server.has_blob(data)));
        assert_eq!(2, server.batch_update_blobs_calls.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_an_unreadable_file_fails_the_upload() {
        let (cas, _) = cas_with(FakeServer::default()).await;
        let dir = tempfile::tempdir().unwrap();
        let missing = (
            digest::of_bytes(&[0; 100]),
            Blob::File(dir.path().join("missing")),
        );
        let e = cas.ensure_uploaded(vec![missing]).await.unwrap_err();
        assert!(e.is::<UnreadableBlob>(), "{e:#}");
    }

    #[tokio::test]
    async fn test_a_refused_blob_fails_the_upload() {
        let (cas, server) = cas_with(FakeServer::default()).await;
        // A digest that doesn't match the bytes.
        let wrong = || (digest::of_bytes(b"one"), Blob::Bytes(b"two".to_vec()));
        let e = cas.ensure_uploaded(vec![wrong()]).await.unwrap_err();
        assert!(format!("{e:#}").contains("digest mismatch"), "{e:#}");
        // The refused blob isn't remembered as present.
        assert!(cas.ensure_uploaded(vec![wrong()]).await.is_err());
        assert_eq!(2, server.find_missing_blobs_calls.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_empty_blob_needs_no_upload() {
        let (cas, server) = cas_with(FakeServer::default()).await;
        cas.ensure_uploaded(vec![blob(b"")]).await.unwrap();
        assert_eq!(0, server.find_missing_blobs_calls.load(Ordering::SeqCst));
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
