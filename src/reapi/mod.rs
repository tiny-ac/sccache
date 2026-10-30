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
#[cfg(test)]
mod fake;
mod input_root;
mod mapping;
pub(crate) mod proto;

pub(crate) use mapping::{
    CompileLocallyReason, NotEligible, PathMapper, ReapiCompile, lexically_normalize,
};
