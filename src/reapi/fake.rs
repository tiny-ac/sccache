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

//! An in-memory REAPI server for tests.

use super::client::{Client, ConnectOptions};
use super::digest;
use super::proto::build::bazel::remote::execution::v2 as remexec;
use super::proto::google::bytestream::byte_stream_server::{ByteStream, ByteStreamServer};
use super::proto::google::bytestream::{
    QueryWriteStatusRequest, QueryWriteStatusResponse, ReadRequest, ReadResponse, WriteRequest,
    WriteResponse,
};
use super::proto::google::rpc;
use futures::Stream;
use remexec::action_cache_server::{ActionCache, ActionCacheServer};
use remexec::capabilities_server::{Capabilities, CapabilitiesServer};
use remexec::content_addressable_storage_server::{
    ContentAddressableStorage, ContentAddressableStorageServer,
};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tokio::net::TcpListener;
use tonic::codegen::BoxFuture;
use tonic::transport::server::TcpIncoming;
use tonic::{Request, Response, Status, Streaming};

type RpcResult<T> = std::result::Result<Response<T>, Status>;

/// A REAPI server that keeps blobs and action results in memory.
#[derive(Default)]
pub(crate) struct FakeServer {
    pub(crate) instance_name: String,
    pub(crate) supports_zstd_bytestream: bool,
    /// Serves blobs with their first byte flipped.
    pub(crate) corrupt_reads: AtomicBool,
    /// Serves blobs with a byte too many.
    pub(crate) oversize_reads: AtomicBool,
    /// Answers GetActionResult with INTERNAL.
    pub(crate) fail_lookups: AtomicBool,
    /// Leaves `cache_capabilities` out of its capabilities.
    pub(crate) without_caching: bool,
    /// Answers GetCapabilities with this code.
    pub(crate) capabilities_error: Mutex<Option<tonic::Code>>,
    pub(crate) capabilities_calls: AtomicUsize,
    /// Advertised as `max_batch_total_size_bytes`, and BatchUpdateBlobs refuses larger
    /// batches; 0 for no limit.
    pub(crate) max_batch_bytes: i64,
    pub(crate) find_missing_blobs_calls: AtomicUsize,
    pub(crate) batch_update_blobs_calls: AtomicUsize,
    pub(crate) batched_blob_count: AtomicUsize,
    pub(crate) bytestream_write_resource_names: Mutex<Vec<String>>,
    /// Answers each ByteStream write after its first request with this `committed_size`, as
    /// a server that already has the blob does.
    pub(crate) early_committed_size: Option<i64>,
    pub(crate) blobs: Mutex<HashMap<(String, i64), Vec<u8>>>,
    pub(crate) action_results: Mutex<HashMap<(String, i64), remexec::ActionResult>>,
}

/// A ByteStream resource name's blob.
struct ResourceName {
    digest: remexec::Digest,
    is_zstd: bool,
}

impl FakeServer {
    pub(crate) fn insert_blob(&self, data: &[u8]) -> remexec::Digest {
        let digest = digest::of_bytes(data);
        self.blobs
            .lock()
            .unwrap()
            .insert((digest.hash.clone(), digest.size_bytes), data.to_vec());
        digest
    }

    pub(crate) fn has_blob(&self, data: &[u8]) -> bool {
        let digest = digest::of_bytes(data);
        self.blobs
            .lock()
            .unwrap()
            .contains_key(&(digest.hash, digest.size_bytes))
    }

    pub(crate) fn insert_action_result(
        &self,
        action_digest: &remexec::Digest,
        result: remexec::ActionResult,
    ) {
        self.action_results.lock().unwrap().insert(
            (action_digest.hash.clone(), action_digest.size_bytes),
            result,
        );
    }

    /// Serves on a new local port and returns a [`Client`] connected with `instance_name`.
    pub(crate) async fn serve_and_connect(self: &Arc<Self>) -> Client {
        Client::connect(&ConnectOptions {
            endpoint: self.serve().await,
            instance_name: self.instance_name.clone(),
            ..Default::default()
        })
        .await
        .unwrap()
    }

    /// Serves on a new local port and returns its `grpc://` endpoint.
    pub(crate) async fn serve(self: &Arc<Self>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let routes = Routes {
            capabilities: CapabilitiesServer::from_arc(self.clone()),
            cas: ContentAddressableStorageServer::from_arc(self.clone()),
            bytestream: ByteStreamServer::from_arc(self.clone()),
            action_cache: ActionCacheServer::from_arc(self.clone()),
        };
        tokio::spawn(
            tonic::transport::Server::builder()
                .serve_with_incoming(routes, TcpIncoming::from(listener)),
        );
        format!("grpc://{addr}")
    }

    /// `[<instance>/]{blobs,compressed-blobs/zstd}/<hash>/<size>`.
    fn parse_read_resource_name(
        &self,
        resource_name: &str,
    ) -> std::result::Result<ResourceName, Status> {
        self.strip_instance_name(resource_name)
            .and_then(parse_blob_name)
            .ok_or_else(|| Status::invalid_argument(resource_name.to_owned()))
    }

    /// `[<instance>/]uploads/<uuid>/{blobs,compressed-blobs/zstd}/<hash>/<size>`.
    fn parse_write_resource_name(
        &self,
        resource_name: &str,
    ) -> std::result::Result<ResourceName, Status> {
        self.strip_instance_name(resource_name)
            .and_then(|name| name.strip_prefix("uploads/"))
            .and_then(|name| name.split_once('/'))
            .filter(|(upload_id, _)| uuid::Uuid::parse_str(upload_id).is_ok())
            .and_then(|(_, blob_name)| parse_blob_name(blob_name))
            .ok_or_else(|| Status::invalid_argument(resource_name.to_owned()))
    }

    fn strip_instance_name<'a>(&self, resource_name: &'a str) -> Option<&'a str> {
        if self.instance_name.is_empty() {
            Some(resource_name)
        } else {
            resource_name.strip_prefix(&format!("{}/", self.instance_name))
        }
    }
}

/// `{blobs,compressed-blobs/zstd}/<hash>/<size>`.
fn parse_blob_name(blob_name: &str) -> Option<ResourceName> {
    let (is_zstd, hash_and_size) = match blob_name.strip_prefix("compressed-blobs/zstd/") {
        Some(hash_and_size) => (true, hash_and_size),
        None => (false, blob_name.strip_prefix("blobs/")?),
    };
    let (hash, size) = hash_and_size.split_once('/')?;
    Some(ResourceName {
        digest: remexec::Digest {
            hash: hash.to_owned(),
            size_bytes: size.parse().ok()?,
        },
        is_zstd,
    })
}

/// Routes each request by its gRPC path, since sccache builds tonic without its `router`
/// feature.
#[derive(Clone)]
struct Routes {
    capabilities: CapabilitiesServer<FakeServer>,
    cas: ContentAddressableStorageServer<FakeServer>,
    bytestream: ByteStreamServer<FakeServer>,
    action_cache: ActionCacheServer<FakeServer>,
}

impl tower_service::Service<http::Request<tonic::body::Body>> for Routes {
    type Response = http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
        let path = request.uri().path();
        if path.starts_with("/build.bazel.remote.execution.v2.Capabilities/") {
            self.capabilities.call(request)
        } else if path.starts_with("/build.bazel.remote.execution.v2.ContentAddressableStorage/") {
            self.cas.call(request)
        } else if path.starts_with("/google.bytestream.ByteStream/") {
            self.bytestream.call(request)
        } else if path.starts_with("/build.bazel.remote.execution.v2.ActionCache/") {
            self.action_cache.call(request)
        } else {
            let response = Status::unimplemented(path.to_owned()).into_http();
            Box::pin(async move { Ok(response) })
        }
    }
}

#[tonic::async_trait]
impl Capabilities for FakeServer {
    async fn get_capabilities(
        &self,
        _: Request<remexec::GetCapabilitiesRequest>,
    ) -> RpcResult<remexec::ServerCapabilities> {
        self.capabilities_calls.fetch_add(1, Ordering::SeqCst);
        if let Some(code) = *self.capabilities_error.lock().unwrap() {
            return Err(Status::new(code, "capabilities_error"));
        }
        let supported_compressors = if self.supports_zstd_bytestream {
            vec![remexec::compressor::Value::Zstd as i32]
        } else {
            vec![]
        };
        Ok(Response::new(remexec::ServerCapabilities {
            cache_capabilities: (!self.without_caching).then(|| remexec::CacheCapabilities {
                supported_compressors,
                max_batch_total_size_bytes: self.max_batch_bytes,
                ..Default::default()
            }),
            ..Default::default()
        }))
    }
}

#[tonic::async_trait]
impl ContentAddressableStorage for FakeServer {
    async fn find_missing_blobs(
        &self,
        request: Request<remexec::FindMissingBlobsRequest>,
    ) -> RpcResult<remexec::FindMissingBlobsResponse> {
        self.find_missing_blobs_calls.fetch_add(1, Ordering::SeqCst);
        let request = request.into_inner();
        if request.instance_name != self.instance_name {
            return Err(Status::invalid_argument(request.instance_name));
        }
        let blobs = self.blobs.lock().unwrap();
        let missing_blob_digests = request
            .blob_digests
            .into_iter()
            .filter(|digest| !blobs.contains_key(&(digest.hash.clone(), digest.size_bytes)))
            .collect();
        Ok(Response::new(remexec::FindMissingBlobsResponse {
            missing_blob_digests,
        }))
    }

    async fn batch_update_blobs(
        &self,
        request: Request<remexec::BatchUpdateBlobsRequest>,
    ) -> RpcResult<remexec::BatchUpdateBlobsResponse> {
        self.batch_update_blobs_calls.fetch_add(1, Ordering::SeqCst);
        let request = request.into_inner();
        if request.instance_name != self.instance_name {
            return Err(Status::invalid_argument(request.instance_name));
        }
        self.batched_blob_count
            .fetch_add(request.requests.len(), Ordering::SeqCst);
        let total_bytes: usize = request.requests.iter().map(|r| r.data.len()).sum();
        if self.max_batch_bytes > 0 && total_bytes as i64 > self.max_batch_bytes {
            return Err(Status::invalid_argument("batch too large"));
        }
        let responses = request
            .requests
            .into_iter()
            .map(|request| {
                let digest = request.digest.unwrap_or_default();
                let status = if digest::of_bytes(&request.data) == digest {
                    self.blobs
                        .lock()
                        .unwrap()
                        .insert((digest.hash.clone(), digest.size_bytes), request.data);
                    None
                } else {
                    Some(rpc::Status {
                        code: tonic::Code::InvalidArgument as i32,
                        message: "digest mismatch".to_owned(),
                        details: vec![],
                    })
                };
                remexec::batch_update_blobs_response::Response {
                    digest: Some(digest),
                    status,
                }
            })
            .collect();
        Ok(Response::new(remexec::BatchUpdateBlobsResponse {
            responses,
        }))
    }

    async fn batch_read_blobs(
        &self,
        _: Request<remexec::BatchReadBlobsRequest>,
    ) -> RpcResult<remexec::BatchReadBlobsResponse> {
        Err(Status::unimplemented("BatchReadBlobs"))
    }

    type GetTreeStream =
        Pin<Box<dyn Stream<Item = std::result::Result<remexec::GetTreeResponse, Status>> + Send>>;

    async fn get_tree(
        &self,
        _: Request<remexec::GetTreeRequest>,
    ) -> RpcResult<Self::GetTreeStream> {
        Err(Status::unimplemented("GetTree"))
    }

    async fn split_blob(
        &self,
        _: Request<remexec::SplitBlobRequest>,
    ) -> RpcResult<remexec::SplitBlobResponse> {
        Err(Status::unimplemented("SplitBlob"))
    }

    async fn splice_blob(
        &self,
        _: Request<remexec::SpliceBlobRequest>,
    ) -> RpcResult<remexec::SpliceBlobResponse> {
        Err(Status::unimplemented("SpliceBlob"))
    }

    type GetChunkMappingStream = Pin<
        Box<
            dyn Stream<Item = std::result::Result<remexec::GetChunkMappingResponse, Status>> + Send,
        >,
    >;

    async fn get_chunk_mapping(
        &self,
        _: Request<remexec::GetChunkMappingRequest>,
    ) -> RpcResult<Self::GetChunkMappingStream> {
        Err(Status::unimplemented("GetChunkMapping"))
    }

    async fn register_chunk_mapping(
        &self,
        _: Request<Streaming<remexec::RegisterChunkMappingRequest>>,
    ) -> RpcResult<remexec::RegisterChunkMappingResponse> {
        Err(Status::unimplemented("RegisterChunkMapping"))
    }
}

#[tonic::async_trait]
impl ActionCache for FakeServer {
    async fn get_action_result(
        &self,
        request: Request<remexec::GetActionResultRequest>,
    ) -> RpcResult<remexec::ActionResult> {
        if self.fail_lookups.load(Ordering::SeqCst) {
            return Err(Status::internal("the action cache is on fire"));
        }
        let request = request.into_inner();
        if request.instance_name != self.instance_name {
            return Err(Status::invalid_argument(request.instance_name));
        }
        let Some(action_digest) = request.action_digest else {
            return Err(Status::invalid_argument("no action_digest"));
        };
        let action_results = self.action_results.lock().unwrap();
        match action_results.get(&(action_digest.hash, action_digest.size_bytes)) {
            Some(result) => Ok(Response::new(result.clone())),
            None => Err(Status::not_found("no result")),
        }
    }

    async fn update_action_result(
        &self,
        _: Request<remexec::UpdateActionResultRequest>,
    ) -> RpcResult<remexec::ActionResult> {
        Err(Status::unimplemented("UpdateActionResult"))
    }
}

#[tonic::async_trait]
impl ByteStream for FakeServer {
    type ReadStream = Pin<Box<dyn Stream<Item = std::result::Result<ReadResponse, Status>> + Send>>;

    async fn read(&self, request: Request<ReadRequest>) -> RpcResult<Self::ReadStream> {
        let request = request.into_inner();
        if request.read_offset != 0 || request.read_limit != 0 {
            return Err(Status::invalid_argument("partial reads not supported"));
        }
        let ResourceName { digest, is_zstd } =
            self.parse_read_resource_name(&request.resource_name)?;
        if is_zstd && !self.supports_zstd_bytestream {
            return Err(Status::invalid_argument("compressed-blobs not offered"));
        }
        let Some(mut data) = self
            .blobs
            .lock()
            .unwrap()
            .get(&(digest.hash.clone(), digest.size_bytes))
            .cloned()
        else {
            return Err(Status::not_found(digest.hash));
        };
        if self.corrupt_reads.load(Ordering::SeqCst) {
            data[0] ^= 1;
        }
        if self.oversize_reads.load(Ordering::SeqCst) {
            data.push(0);
        }
        if is_zstd {
            data = zstd::bulk::compress(&data, 1).map_err(|e| Status::internal(e.to_string()))?;
        }
        // Several chunks, as servers send large blobs.
        let chunks: Vec<_> = data
            .chunks(1000)
            .map(|chunk| {
                Ok(ReadResponse {
                    data: chunk.to_vec(),
                })
            })
            .collect();
        Ok(Response::new(Box::pin(futures::stream::iter(chunks))))
    }

    async fn write(&self, request: Request<Streaming<WriteRequest>>) -> RpcResult<WriteResponse> {
        let mut stream = request.into_inner();
        let mut resource_name = None;
        let mut data = vec![];
        let mut finished = false;
        while let Some(request) = stream.message().await? {
            if resource_name.is_none() {
                resource_name = Some(request.resource_name.clone());
                self.bytestream_write_resource_names
                    .lock()
                    .unwrap()
                    .push(request.resource_name);
                if let Some(committed_size) = self.early_committed_size {
                    return Ok(Response::new(WriteResponse { committed_size }));
                }
            }
            if request.write_offset != data.len() as i64 {
                return Err(Status::invalid_argument("write_offset"));
            }
            data.extend_from_slice(&request.data);
            if request.finish_write {
                finished = true;
                break;
            }
        }
        if !finished {
            return Err(Status::invalid_argument("no finish_write"));
        }
        let committed_size = data.len() as i64;
        let ResourceName { digest, is_zstd } =
            self.parse_write_resource_name(&resource_name.unwrap_or_default())?;
        if is_zstd {
            data = zstd::bulk::decompress(&data, digest.size_bytes as usize)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
        }
        if digest::of_bytes(&data) != digest {
            return Err(Status::invalid_argument("digest mismatch"));
        }
        self.blobs
            .lock()
            .unwrap()
            .insert((digest.hash, digest.size_bytes), data);
        Ok(Response::new(WriteResponse { committed_size }))
    }

    async fn query_write_status(
        &self,
        _: Request<QueryWriteStatusRequest>,
    ) -> RpcResult<QueryWriteStatusResponse> {
        Err(Status::unimplemented("QueryWriteStatus"))
    }
}
