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

//! A client for the Remote Execution API (REAPI), Bazel's protocol for remote caching
//! and execution.

mod action_cache;
mod auth_token;
mod cas;
mod client;
mod connection;
mod credential_helper;
mod digest;
mod execution;
mod executor;
#[cfg(test)]
mod fake;
mod input_root;
mod ipc;
mod mapping;
mod pause;
pub(crate) mod proto;
mod remote_compile;
mod sysroot;
mod terminal_output;
mod toolchains;
mod worker_toolchains;

pub(crate) use executor::ReapiExecutor;
pub(crate) use ipc::{
    ReapiIpcExecutor, ReapiIpcRequest, ReapiIpcResponse, handle_reapi_ipc_request,
};
pub(crate) use mapping::{
    ActionEnv, CToolchainKey, CompileLocallyReason, InternalFile, NotEligible, PathMapper,
    ReapiCompile, ToolchainKey, WorkerProgram, lexically_normalize,
};
pub(crate) use sysroot::sysroot_copy;
