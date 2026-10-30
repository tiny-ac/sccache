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
mod cas;
mod client;
mod digest;
mod executor;
#[cfg(test)]
mod fake;
mod input_root;
mod mapping;
pub(crate) mod proto;
mod remote_compile;
mod sysroot;

pub(crate) use executor::ReapiExecutor;
pub(crate) use mapping::{
    ActionEnv, CompileLocallyReason, NotEligible, PathMapper, ReapiCompile, lexically_normalize,
};
pub(crate) use sysroot::sysroot_copy;
