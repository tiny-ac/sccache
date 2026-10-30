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
use super::proto::google::bytestream::byte_stream_client::ByteStreamClient;
use super::proto::google::bytestream::{ReadRequest, WriteRequest};
use crate::errors::*;
use futures::StreamExt;
use remexec::content_addressable_storage_client::ContentAddressableStorageClient;
use sha2::{Digest as _, Sha256};
use std::collections::HashSet;
use std::io::{self, Read, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
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
#[cfg_attr(not(test), expect(dead_code, reason = "Execute uploads through it"))]
const BYTESTREAM_WRITE_CHUNK_BYTES: usize = 1024 * 1024;
/// rlib and rmeta files compress well at low levels.
#[cfg_attr(not(test), expect(dead_code, reason = "Execute uploads through it"))]
const ZSTD_LEVEL: i32 = 1;

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

    fn open(self) -> Result<Box<dyn Read + Send>> {
        match self {
            Blob::Bytes(bytes) => Ok(Box::new(io::Cursor::new(bytes))),
            Blob::File(path) => {
                let file = std::fs::File::open(&path).with_context(|| UnreadableBlob(path))?;
                Ok(Box::new(file))
            }
        }
    }
}

/// Reads a blob to upload, a chunk at a time.
#[cfg_attr(not(test), expect(dead_code, reason = "Execute uploads through it"))]
enum ChunkReader {
    Unopened {
        blob: Blob,
        compress: bool,
    },
    Open {
        reader: Box<dyn Read + Send>,
        file: Option<PathBuf>,
    },
}

#[cfg_attr(not(test), expect(dead_code, reason = "Execute uploads through it"))]
impl ChunkReader {
    /// The next chunk to send, and `self` to read the rest from. Blocks.
    fn next_chunk(self) -> Result<(Self, Vec<u8>)> {
        let (mut reader, file) = match self {
            ChunkReader::Unopened { blob, compress } => {
                let file = match &blob {
                    Blob::File(path) => Some(path.clone()),
                    Blob::Bytes(_) => None,
                };
                let reader = blob.open()?;
                let reader: Box<dyn Read + Send> = if compress {
                    Box::new(zstd::stream::read::Encoder::new(reader, ZSTD_LEVEL)?)
                } else {
                    reader
                };
                (reader, file)
            }
            ChunkReader::Open { reader, file } => (reader, file),
        };
        let mut data = Vec::with_capacity(BYTESTREAM_WRITE_CHUNK_BYTES);
        if let Err(e) = (&mut reader)
            .take(BYTESTREAM_WRITE_CHUNK_BYTES as u64)
            .read_to_end(&mut data)
        {
            return Err(match file {
                Some(path) => Error::from(e).context(UnreadableBlob(path)),
                None => e.into(),
            });
        }
        Ok((ChunkReader::Open { reader, file }, data))
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

    /// `{blobs,compressed-blobs/zstd}/<hash>/<size>`, in the encoding this CAS's transfers use.
    fn blob_name(&self, digest: &remexec::Digest) -> String {
        let blobs = if self.supports_zstd_bytestream {
            "compressed-blobs/zstd"
        } else {
            "blobs"
        };
        format!("{blobs}/{}/{}", digest.hash, digest.size_bytes)
    }

    fn with_instance_name(&self, name: String) -> String {
        if self.instance_name.is_empty() {
            name
        } else {
            format!("{}/{name}", self.instance_name)
        }
    }

    fn download_resource_name(&self, digest: &remexec::Digest) -> String {
        self.with_instance_name(self.blob_name(digest))
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

    /// Uploads whichever of `blobs` the CAS doesn't have, in batches, or through ByteStream
    /// if too large for one. A local file that can't be read fails with [`UnreadableBlob`].
    /// A blob that another call uploads while this one waits for an upload permit isn't
    /// uploaded again.
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
        let mut large_blobs = vec![];
        for (digest, blob) in candidates.into_iter().filter(|(d, _)| missing.contains(d)) {
            let size = digest.size_bytes as usize + BATCH_OVERHEAD_PER_BLOB;
            if empty_batch_bytes + size > self.max_batch_bytes {
                large_blobs.push((digest, blob));
                continue;
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
        futures::try_join!(
            futures::future::try_join_all(batches.into_iter().map(|batch| {
                self.upload_with_permit(batch, |batch| self.batch_update_blobs(batch))
            })),
            futures::future::try_join_all(large_blobs.into_iter().map(|large_blob| {
                self.upload_with_permit(vec![large_blob], |blobs| async move {
                    for (digest, blob) in blobs {
                        self.write_blob(digest, blob).await?;
                    }
                    Ok(())
                })
            })),
        )?;
        self.remember_present(digests);
        Ok(())
    }

    /// Waits for an upload permit, then uploads with `upload` whichever of `blobs` aren't
    /// known to be present by then.
    async fn upload_with_permit<F, U>(
        &self,
        blobs: Vec<(remexec::Digest, Blob)>,
        upload: F,
    ) -> Result<()>
    where
        F: FnOnce(Vec<(remexec::Digest, Blob)>) -> U,
        U: Future<Output = Result<()>>,
    {
        let _permit = self
            .upload_permits
            .acquire()
            .await
            .expect("the upload semaphore is never closed");
        let blobs: Vec<_> = blobs
            .into_iter()
            .filter(|(digest, _)| !self.is_known_present(digest))
            .collect();
        if blobs.is_empty() {
            return Ok(());
        }
        let digests: Vec<_> = blobs.iter().map(|(digest, _)| digest.clone()).collect();
        upload(blobs).await?;
        self.remember_present(digests);
        Ok(())
    }

    fn remember_present(&self, digests: Vec<remexec::Digest>) {
        let mut present_blobs = self.present_blobs.lock().unwrap();
        if present_blobs.len() + digests.len() > self.max_present_blobs {
            present_blobs.clear();
        }
        present_blobs.extend(digests);
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

    fn upload_resource_name(&self, digest: &remexec::Digest) -> String {
        self.with_instance_name(format!(
            "uploads/{}/{}",
            uuid::Uuid::new_v4(),
            self.blob_name(digest)
        ))
    }

    /// Uploads `blob` through ByteStream, a chunk at a time, each read on a blocking thread
    /// when the request stream asks for it.
    async fn write_blob(&self, digest: remexec::Digest, blob: Blob) -> Result<()> {
        let resource_name = self.upload_resource_name(&digest);
        let read_error: Arc<Mutex<Option<Error>>> = Arc::default();
        let sent_bytes = Arc::new(AtomicU64::new(0));
        let chunk_reader = ChunkReader::Unopened {
            blob,
            compress: self.supports_zstd_bytestream,
        };
        let requests = futures::stream::unfold(Some(chunk_reader), {
            let (read_error, sent_bytes) = (read_error.clone(), sent_bytes.clone());
            move |chunk_reader| {
                let (read_error, sent_bytes, resource_name) = (
                    read_error.clone(),
                    sent_bytes.clone(),
                    resource_name.clone(),
                );
                async move {
                    let chunk_reader = chunk_reader?;
                    let next_chunk = tokio::task::spawn_blocking(move || chunk_reader.next_chunk())
                        .await
                        .map_err(Error::from)
                        .flatten();
                    let (chunk_reader, data) = match next_chunk {
                        Ok(next_chunk) => next_chunk,
                        Err(e) => {
                            *read_error.lock().unwrap() = Some(e);
                            return None;
                        }
                    };
                    let offset = sent_bytes.fetch_add(data.len() as u64, Ordering::Relaxed);
                    let finish_write = data.len() < BYTESTREAM_WRITE_CHUNK_BYTES;
                    let request = WriteRequest {
                        // Only the first request names the resource.
                        resource_name: if offset == 0 {
                            resource_name
                        } else {
                            String::new()
                        },
                        write_offset: offset as i64,
                        finish_write,
                        data,
                    };
                    Some((request, (!finish_write).then_some(chunk_reader)))
                }
            }
        });
        // hyper sends the request stream from its own task, which keeps going after a
        // timeout until this function returns and drops `_cancel`.
        let (_cancel, cancelled) = futures::channel::oneshot::channel::<()>();
        let requests = requests.take_until(cancelled);
        let description = format!(
            "failed to upload blob {}/{} to the REAPI CAS",
            digest.hash, digest.size_bytes
        );
        let response = with_timeout(
            &description,
            upload_timeout(digest.size_bytes as u64),
            async {
                self.bytestream_client
                    .clone()
                    .write(requests)
                    .await
                    .map_err(status_error)
            },
        )
        .await;
        // A server that has the blob may end the write early, so a read error matters only
        // when the write failed.
        let committed_size = match response {
            Ok(response) => response.into_inner().committed_size,
            Err(e) => return Err(read_error.lock().unwrap().take().unwrap_or(e)),
        };
        let sent_bytes = sent_bytes.load(Ordering::Relaxed);
        // Servers report the bytes received, or, when they end the write early because they
        // already have the blob, its size (-1 for a compressed upload).
        let early_committed_size = if self.supports_zstd_bytestream {
            -1
        } else {
            digest.size_bytes
        };
        if committed_size != sent_bytes as i64 && committed_size != early_committed_size {
            bail!(
                "{description}: the REAPI server committed {committed_size} of {sent_bytes} bytes"
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
        incompressible_bytes_of_len(64 * 1024)
    }

    fn incompressible_bytes_of_len(len: usize) -> Vec<u8> {
        let mut data = vec![0; len];
        rand::rngs::StdRng::seed_from_u64(0).fill_bytes(&mut data);
        data
    }

    /// A server whose batches take 64 KiB.
    async fn cas_with_small_batches(server: FakeServer) -> (Cas, Arc<FakeServer>) {
        cas_with(FakeServer {
            max_batch_bytes: 64 * 1024,
            ..server
        })
        .await
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
    async fn test_a_blob_another_call_uploaded_is_not_uploaded_again() {
        let (mut cas, server) = cas_with_small_batches(FakeServer::default()).await;
        cas.upload_permits = tokio::sync::Semaphore::new(1);
        let blobs = vec![blob(b"small"), blob(&incompressible_bytes())];
        let (first, second) = futures::join!(
            cas.ensure_uploaded(blobs.clone()),
            cas.ensure_uploaded(blobs)
        );
        first.unwrap();
        second.unwrap();
        assert_eq!(1, server.batched_blob_count.load(Ordering::SeqCst));
        let resource_names = server.bytestream_write_resource_names.lock().unwrap();
        assert_eq!(1, resource_names.len());
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
    async fn test_a_blob_that_fits_only_without_its_overhead_goes_through_bytestream() {
        let (cas, server) = cas_with(FakeServer {
            max_batch_bytes: 3 * 1024,
            ..Default::default()
        })
        .await;
        let large = vec![9; 3 * 1024 - 10];
        cas.ensure_uploaded(vec![blob(&large)]).await.unwrap();
        assert!(server.has_blob(&large));
        assert_eq!(0, server.batch_update_blobs_calls.load(Ordering::SeqCst));
        assert_eq!(
            1,
            server.bytestream_write_resource_names.lock().unwrap().len()
        );
    }

    #[test_case(false ; "uncompressed")]
    #[test_case(true ; "zstd")]
    #[tokio::test]
    async fn test_large_blobs_go_through_bytestream(supports_zstd_bytestream: bool) {
        let (cas, server) = cas_with_small_batches(FakeServer {
            instance_name: "main".to_owned(),
            supports_zstd_bytestream,
            ..Default::default()
        })
        .await;
        // Several chunks even compressed.
        let large = incompressible_bytes_of_len(3 * BYTESTREAM_WRITE_CHUNK_BYTES + 10);
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("large");
        std::fs::write(&file, &large).unwrap();
        cas.ensure_uploaded(vec![(digest::of_bytes(&large), Blob::File(file))])
            .await
            .unwrap();
        assert!(server.has_blob(&large));
        assert_eq!(0, server.batch_update_blobs_calls.load(Ordering::SeqCst));
        let resource_names = server.bytestream_write_resource_names.lock().unwrap();
        assert_eq!(1, resource_names.len());
        assert_eq!(
            supports_zstd_bytestream,
            resource_names[0].contains("/compressed-blobs/zstd/"),
            "{resource_names:?}"
        );
    }

    #[tokio::test]
    async fn test_an_upload_of_whole_chunks_finishes() {
        let (cas, server) = cas_with_small_batches(FakeServer::default()).await;
        // The last request carries no data, only `finish_write`.
        let data = incompressible_bytes_of_len(2 * BYTESTREAM_WRITE_CHUNK_BYTES);
        cas.ensure_uploaded(vec![blob(&data)]).await.unwrap();
        assert!(server.has_blob(&data));
    }

    const EARLY_FINISH_BYTES: usize = 3 * BYTESTREAM_WRITE_CHUNK_BYTES;

    #[test_case(false, EARLY_FINISH_BYTES as i64 ; "uncompressed")]
    #[test_case(true, -1 ; "zstd")]
    #[tokio::test]
    async fn test_a_write_the_server_ends_early_succeeds(
        supports_zstd_bytestream: bool,
        committed_size: i64,
    ) {
        let (cas, server) = cas_with_small_batches(FakeServer {
            supports_zstd_bytestream,
            early_committed_size: Some(committed_size),
            ..Default::default()
        })
        .await;
        let data = incompressible_bytes_of_len(EARLY_FINISH_BYTES);
        cas.ensure_uploaded(vec![blob(&data)]).await.unwrap();
        assert_eq!(
            1,
            server.bytestream_write_resource_names.lock().unwrap().len()
        );
    }

    #[test_case(-1 ; "minus one uncompressed")]
    #[test_case(1000 ; "part of the blob")]
    #[tokio::test]
    async fn test_a_wrong_committed_size_fails_the_upload(committed_size: i64) {
        let (cas, _) = cas_with_small_batches(FakeServer {
            early_committed_size: Some(committed_size),
            ..Default::default()
        })
        .await;
        let data = incompressible_bytes_of_len(EARLY_FINISH_BYTES);
        let e = cas.ensure_uploaded(vec![blob(&data)]).await.unwrap_err();
        assert!(format!("{e:#}").contains("committed"), "{e:#}");
    }

    #[test_case(100 ; "in a batch")]
    #[test_case(100 * 1024 ; "through bytestream")]
    #[tokio::test]
    async fn test_an_unreadable_file_fails_the_upload(size: usize) {
        let (cas, _) = cas_with_small_batches(FakeServer::default()).await;
        let dir = tempfile::tempdir().unwrap();
        let missing = (
            digest::of_bytes(&vec![0; size]),
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
