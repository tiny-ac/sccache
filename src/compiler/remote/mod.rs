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

//! Compiling cache misses on another machine, through a [`RemoteExecutor`] such as
//! sccache-dist's `DistExecutor`.

use crate::compiler::{Compilation, CompileCommand, DistType};
use crate::errors::*;
use crate::mock_command::CommandCreatorSync;
use crate::server::SccacheService;
use async_trait::async_trait;
use std::path::PathBuf;
use std::process;

#[cfg(feature = "dist-client")]
mod dist;

#[cfg(feature = "dist-client")]
pub use self::dist::DistExecutor;

/// A cache miss, for an executor to compile.
pub struct RemoteJob<T: CommandCreatorSync> {
    pub compilation: Box<dyn Compilation<T>>,
    pub out_pretty: String,
    /// `None` when `dist_input_options` returns `None` or the compiler can't distribute
    /// this compile.
    pub dist_job: Option<DistJob>,
}

/// A compile to run on an sccache-dist cluster.
pub struct DistJob {
    pub path_transformer: crate::dist::PathTransformer,
    pub command: crate::dist::CompileCommand,
    pub weak_toolchain_key: String,
    pub cwd: PathBuf,
}

/// Options for preprocessing sources that sccache-dist compiles.
#[derive(Clone, Copy, Debug)]
pub struct DistInputOptions {
    /// Preprocess C and C++ sources only partly, inlining their includes
    /// (`-frewrite-includes` with clang, `-fdirectives-only` with gcc).
    pub rewrite_includes_only: bool,
}

/// The job's local compile, for when an executor doesn't run it remotely.
pub struct LocalCompile<'a, T: CommandCreatorSync> {
    pub service: &'a SccacheService<T>,
    pub creator: &'a T,
    pub command: Box<dyn CompileCommand<T>>,
}

impl<T: CommandCreatorSync> LocalCompile<'_, T> {
    pub async fn run(&self) -> Result<process::Output> {
        self.command.execute(self.service, self.creator).await
    }
}

/// Compiles cache misses remotely.
#[async_trait]
pub trait RemoteExecutor<T: CommandCreatorSync>: Send + Sync {
    /// `None` when no compile is distributed.
    fn dist_input_options(&self) -> Option<DistInputOptions>;

    /// Compiles `job`, remotely or with `local_compile`, and writes its outputs where a
    /// local compile would. If a remote compile can't complete, none of its outputs are
    /// left; if the remote compiler runs and exits non-zero, its outputs stay.
    async fn compile(
        &self,
        job: RemoteJob<T>,
        local_compile: LocalCompile<'_, T>,
    ) -> Result<(DistType, process::Output)>;
}
