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

use crate::compiler::{CacheControl, Compilation, CompileCommand, DistType};
use crate::errors::*;
use crate::mock_command::CommandCreatorSync;
use crate::server::SccacheService;
use async_trait::async_trait;
use std::ffi::OsString;
use std::path::PathBuf;
use std::process;

#[cfg(feature = "dist-client")]
mod dist;

#[cfg(feature = "dist-client")]
pub use self::dist::DistExecutor;
#[cfg(feature = "dist-client")]
pub(crate) use self::dist::{
    DistClientSetup, DistIpcExecutor, DistIpcRequest, DistIpcResponse, PendingDistToolchains,
    handle_dist_ipc_request,
};

/// A cache miss, for an executor to compile.
pub struct RemoteJob<T: CommandCreatorSync> {
    pub compilation: Box<dyn Compilation<T>>,
    pub out_pretty: String,
    /// `None` when `dist_input_options` returns `None` or the compiler can't distribute
    /// this compile.
    pub dist_job: Option<DistJob>,
    pub cache_control: CacheControl,
    /// Whether `local_compile_changes` changed the local compile's arguments. sccache caches a
    /// remote result under the hash key of the changed compile, so an executor uses one only
    /// when this is true.
    pub local_compile_changed: bool,
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

/// Maps paths in a local compile's stdout or stderr.
pub type StdoutStderrMapper = Box<dyn Fn(&[u8]) -> Vec<u8> + Send + Sync>;

/// An argument an executor appends to a local compile.
#[derive(Clone)]
pub struct LocalArg {
    pub arg: OsString,
    /// `arg` as it goes into the hash key, with a path that doesn't change the outputs
    /// replaced by a placeholder.
    pub hashed_as: OsString,
}

/// How an executor changes a local compile: arguments to append, and a mapper for the paths
/// they change in its stdout and stderr.
#[derive(Default)]
pub struct LocalCompileChanges {
    pub args: Vec<LocalArg>,
    pub stdout_stderr_mapper: Option<StdoutStderrMapper>,
}

/// The job's local compile, for when an executor doesn't run it remotely.
pub struct LocalCompile<'a, T: CommandCreatorSync> {
    pub service: &'a SccacheService<T>,
    pub creator: &'a T,
    pub command: Box<dyn CompileCommand<T>>,
    pub stdout_stderr_mapper: Option<StdoutStderrMapper>,
}

impl<T: CommandCreatorSync> LocalCompile<'_, T> {
    pub async fn run(&self) -> Result<process::Output> {
        let result = self.command.execute(self.service, self.creator).await;
        let Some(mapper) = &self.stdout_stderr_mapper else {
            return result;
        };
        let map = |mut output: process::Output| {
            output.stdout = mapper(&output.stdout);
            output.stderr = mapper(&output.stderr);
            output
        };
        // A compiler that exits non-zero comes back as a `ProcessError`, whose stderr the user
        // reads most.
        match result {
            Ok(output) => Ok(map(output)),
            Err(error) => match error.downcast::<ProcessError>() {
                Ok(ProcessError(output)) => Err(ProcessError(map(output)).into()),
                Err(error) => Err(error),
            },
        }
    }
}

/// Compiles cache misses remotely.
#[async_trait]
pub trait RemoteExecutor<T: CommandCreatorSync>: Send + Sync {
    /// Options for preprocessing the sources sccache-dist compiles, or `None` when the executor
    /// doesn't use sccache-dist.
    fn dist_input_options(&self) -> Option<DistInputOptions>;

    /// Changes to the local compile of `compilation`. The arguments go after the compile's
    /// own, so an executor returns them only for compilers that accept them there, and never
    /// with `dist_input_options`, since sccache-dist compiles don't get them. Called from a
    /// blocking thread, so it may read files.
    fn local_compile_changes(&self, _compilation: &dyn Compilation<T>) -> LocalCompileChanges {
        LocalCompileChanges::default()
    }

    /// Compiles `job`, remotely or with `local_compile`, and writes its outputs where a
    /// local compile would. A remote compile that can't complete leaves none of its outputs.
    /// A remote compiler that runs and exits non-zero leaves its outputs in place.
    async fn compile(
        &self,
        job: RemoteJob<T>,
        local_compile: LocalCompile<'_, T>,
    ) -> Result<(DistType, process::Output)>;
}
