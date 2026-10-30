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
use futures::Stream;
use remexec::action_cache_server::{ActionCache, ActionCacheServer};
use remexec::capabilities_server::{Capabilities, CapabilitiesServer};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
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
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let routes = Routes {
            capabilities: CapabilitiesServer::from_arc(self.clone()),
            bytestream: ByteStreamServer::from_arc(self.clone()),
            action_cache: ActionCacheServer::from_arc(self.clone()),
        };
        tokio::spawn(
            tonic::transport::Server::builder()
                .serve_with_incoming(routes, TcpIncoming::from(listener)),
        );
        Client::connect(&ConnectOptions {
            endpoint: format!("grpc://{addr}"),
            instance_name: self.instance_name.clone(),
            ..Default::default()
        })
        .await
        .unwrap()
    }

    fn parse_resource_name(
        &self,
        resource_name: &str,
    ) -> std::result::Result<ResourceName, Status> {
        let invalid = || Status::invalid_argument(resource_name.to_owned());
        let blob_name = if self.instance_name.is_empty() {
            resource_name
        } else {
            resource_name
                .strip_prefix(&format!("{}/", self.instance_name))
                .ok_or_else(invalid)?
        };
        let (is_zstd, hash_and_size) =
            if let Some(hash_and_size) = blob_name.strip_prefix("compressed-blobs/zstd/") {
                (true, hash_and_size)
            } else if let Some(hash_and_size) = blob_name.strip_prefix("blobs/") {
                (false, hash_and_size)
            } else {
                return Err(invalid());
            };
        let (hash, size) = hash_and_size.split_once('/').ok_or_else(invalid)?;
        let size_bytes = size.parse().map_err(|_| invalid())?;
        Ok(ResourceName {
            digest: remexec::Digest {
                hash: hash.to_owned(),
                size_bytes,
            },
            is_zstd,
        })
    }
}

/// Routes each request by its gRPC path, since sccache builds tonic without its `router`
/// feature.
#[derive(Clone)]
struct Routes {
    capabilities: CapabilitiesServer<FakeServer>,
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
        let supported_compressors = if self.supports_zstd_bytestream {
            vec![remexec::compressor::Value::Zstd as i32]
        } else {
            vec![]
        };
        Ok(Response::new(remexec::ServerCapabilities {
            cache_capabilities: Some(remexec::CacheCapabilities {
                supported_compressors,
                ..Default::default()
            }),
            ..Default::default()
        }))
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
        let ResourceName { digest, is_zstd } = self.parse_resource_name(&request.resource_name)?;
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

    async fn write(&self, _: Request<Streaming<WriteRequest>>) -> RpcResult<WriteResponse> {
        Err(Status::unimplemented("Write"))
    }

    async fn query_write_status(
        &self,
        _: Request<QueryWriteStatusRequest>,
    ) -> RpcResult<QueryWriteStatusResponse> {
        Err(Status::unimplemented("QueryWriteStatus"))
    }
}
