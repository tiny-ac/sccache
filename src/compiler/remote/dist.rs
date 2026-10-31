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

use super::{DistInputOptions, DistJob, LocalCompile, RemoteExecutor, RemoteJob};
use crate::compiler::{Compilation, DistPackagers, DistType, OutputsRewriter};
use crate::dist::{self, pkg};
use crate::errors::*;
use crate::lru_disk_cache;
use crate::mock_command::CommandCreatorSync;
use async_trait::async_trait;
use fs_err::{self as fs, File};
use serde::{Deserialize, Serialize};
use std::io;
use std::path::{Path, PathBuf};
use std::process;
use std::sync::Arc;

mod ipc;

pub(crate) use self::ipc::{
    DistClientSetup, DistIpcRequest, DistIpcResponse, PendingDistToolchains,
    handle_dist_ipc_request,
};

/// What to do when sccache-dist can't complete a compile.
#[derive(Debug)]
enum DistError {
    CompileLocally(Error),
    FailCompile(FailCompileReason),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum FailCompileReason {
    Misconfigured(String),
    ToolchainTooLargeToCache(String),
    HttpClientError(String),
}

impl From<FailCompileReason> for Error {
    fn from(reason: FailCompileReason) -> Self {
        match reason {
            // Rebuilt as an `HttpClientError`, which `SccacheService` looks for to reset the
            // dist client.
            FailCompileReason::HttpClientError(message) => HttpClientError(message).into(),
            FailCompileReason::Misconfigured(message)
            | FailCompileReason::ToolchainTooLargeToCache(message) => anyhow!(message),
        }
    }
}

/// A [`RemoteExecutor`] that runs compiles on an sccache-dist cluster.
pub struct DistExecutor {
    client: Arc<dyn dist::Client>,
}

/// A remote compile to run on the cluster, without its packagers.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterCompile {
    command: dist::CompileCommand,
    dist_output_paths: Vec<String>,
    compiler_path: PathBuf,
    weak_toolchain_key: String,
    out_pretty: String,
}

/// The remote compile's result, with its outputs written to their local paths.
#[derive(Serialize, Deserialize)]
pub struct ClusterCompileOutput {
    server_id: dist::ServerId,
    output: dist::ProcessOutput,
    local_output_paths: Vec<PathBuf>,
    path_transformer: dist::PathTransformer,
    custom_toolchain_archive: Option<PathBuf>,
}

#[async_trait]
impl<T: CommandCreatorSync> RemoteExecutor<T> for DistExecutor {
    fn dist_input_options(&self) -> Option<DistInputOptions> {
        Some(DistInputOptions {
            rewrite_includes_only: self.client.rewrite_includes_only(),
        })
    }

    async fn compile(
        &self,
        job: RemoteJob<T>,
        local_compile: LocalCompile<'_, T>,
    ) -> Result<(DistType, process::Output)> {
        let RemoteJob {
            compilation,
            out_pretty,
            dist_job,
            ..
        } = job;
        let Some(dist_job) = dist_job else {
            debug!("[{}]: Compiling locally", out_pretty);
            return Ok((DistType::NoDist, local_compile.run().await?));
        };
        debug!("[{}]: Attempting distributed compilation", out_pretty);
        let compiler_path = local_compile.command.get_executable();
        let result = async {
            let (request, (inputs_packager, toolchain_packager, outputs_rewriter)) =
                Self::prepare(compilation, dist_job, compiler_path.clone(), &out_pretty)?;
            let response = self
                .compile_on_cluster(request, inputs_packager, toolchain_packager)
                .await?;
            Self::finish(response, outputs_rewriter)
        }
        .await
        .map_err(|error| classify_error(error, &compiler_path));
        compile_locally_on_error(result, local_compile, &out_pretty).await
    }
}

/// Returns `result`, compiling with `local_compile` instead if it's a
/// [`DistError::CompileLocally`].
async fn compile_locally_on_error<T: CommandCreatorSync>(
    result: std::result::Result<(DistType, process::Output), DistError>,
    local_compile: LocalCompile<'_, T>,
    out_pretty: &str,
) -> Result<(DistType, process::Output)> {
    match result {
        Ok(compiled) => Ok(compiled),
        Err(DistError::FailCompile(reason)) => Err(reason.into()),
        Err(DistError::CompileLocally(e)) => {
            // `{:#}` prints the error and the causes in a single line.
            let errmsg = format!("{:#}", e);
            warn!(
                "[{}]: Could not perform distributed compile, falling back to local: {}",
                out_pretty, errmsg
            );
            Ok((DistType::Error, local_compile.run().await?))
        }
    }
}

impl DistExecutor {
    pub fn new(client: Arc<dyn dist::Client>) -> Self {
        DistExecutor { client }
    }

    /// Maps the compile's outputs to paths on the build server and builds its packagers.
    fn prepare<T: CommandCreatorSync>(
        compilation: Box<dyn Compilation<T>>,
        dist_job: DistJob,
        compiler_path: PathBuf,
        out_pretty: &str,
    ) -> Result<(ClusterCompile, DistPackagers)> {
        let DistJob {
            mut path_transformer,
            command,
            weak_toolchain_key,
            cwd,
        } = dist_job;
        debug!("[{}]: Creating distributed compile request", out_pretty);
        let dist_output_paths = compilation
            .outputs()
            .map(|output| path_transformer.as_dist_abs(&cwd.join(output.path)))
            .collect::<Option<_>>()
            .context("Failed to adapt an output path for distributed compile")?;
        let packagers = compilation.into_dist_packagers(path_transformer)?;
        let request = ClusterCompile {
            command,
            dist_output_paths,
            compiler_path,
            weak_toolchain_key,
            out_pretty: out_pretty.to_owned(),
        };
        Ok((request, packagers))
    }

    /// Runs `request` on the cluster and writes its outputs to their local paths on the
    /// client machine.
    async fn compile_on_cluster(
        &self,
        request: ClusterCompile,
        inputs_packager: Box<dyn pkg::InputsPackager>,
        toolchain_packager: Box<dyn pkg::ToolchainPackager>,
    ) -> Result<ClusterCompileOutput> {
        let ClusterCompile {
            command: mut dist_compile_cmd,
            dist_output_paths,
            compiler_path,
            weak_toolchain_key,
            out_pretty,
        } = request;
        let dist_client = &self.client;

        debug!(
            "[{}]: Identifying dist toolchain for {:?}",
            out_pretty, compiler_path
        );
        let (dist_toolchain, maybe_dist_compile_executable) = dist_client
            .put_toolchain(compiler_path, weak_toolchain_key, toolchain_packager)
            .await?;
        let mut custom_toolchain_archive = None;
        if let Some((dist_compile_executable, archive_path)) = maybe_dist_compile_executable {
            dist_compile_cmd.executable = dist_compile_executable;
            custom_toolchain_archive = Some(archive_path);
        }

        debug!("[{}]: Requesting allocation", out_pretty);
        let jares = dist_client.do_alloc_job(dist_toolchain.clone()).await?;
        let job_alloc = match jares {
            dist::AllocJobResult::Success {
                job_alloc,
                need_toolchain: true,
            } => {
                debug!(
                    "[{}]: Sending toolchain {} for job {}",
                    out_pretty, dist_toolchain.archive_id, job_alloc.job_id
                );

                match dist_client
                    .do_submit_toolchain(job_alloc.clone(), dist_toolchain)
                    .await
                    .map_err(|e| e.context("Could not submit toolchain"))?
                {
                    dist::SubmitToolchainResult::Success => Ok(job_alloc),
                    dist::SubmitToolchainResult::JobNotFound => {
                        bail!("Job {} not found on server", job_alloc.job_id)
                    }
                    dist::SubmitToolchainResult::CannotCache => bail!(
                        "Toolchain for job {} could not be cached by server",
                        job_alloc.job_id
                    ),
                }
            }
            dist::AllocJobResult::Success {
                job_alloc,
                need_toolchain: false,
            } => Ok(job_alloc),
            dist::AllocJobResult::Fail { msg } => {
                Err(anyhow!("Failed to allocate job").context(msg))
            }
        }?;
        let job_id = job_alloc.job_id;
        let server_id = job_alloc.server_id;
        debug!("[{}]: Running job", out_pretty);
        let ((job_id, server_id), (jres, path_transformer)) = dist_client
            .do_run_job(
                job_alloc,
                dist_compile_cmd,
                dist_output_paths,
                inputs_packager,
            )
            .await
            .map(move |res| ((job_id, server_id), res))
            .with_context(|| {
                format!(
                    "could not run distributed compilation job on {:?}",
                    server_id
                )
            })?;

        let jc = match jres {
            dist::RunJobResult::Complete(jc) => jc,
            dist::RunJobResult::JobNotFound => bail!("Job {} not found on server", job_id),
        };
        debug!(
            "fetched {:?}",
            jc.outputs
                .iter()
                .map(|(p, bs)| (p, bs.lens().to_string()))
                .collect::<Vec<_>>()
        );
        let mut local_output_paths: Vec<PathBuf> = vec![];
        for (path, output_data) in jc.outputs {
            let len = output_data.lens().actual;
            let local_path = remove_outputs_on_error(
                &local_output_paths,
                path_transformer
                    .to_local(&path)
                    .with_context(|| format!("unable to transform output path {}", path)),
            )?;
            local_output_paths.push(local_path);
            // Do this first so cleanup works correctly
            let local_path = local_output_paths
                .last()
                .expect("nothing in vec after push");

            let mut file = remove_outputs_on_error(
                &local_output_paths,
                File::create(local_path).with_context(|| {
                    format!("Failed to create output file {}", local_path.display())
                }),
            )?;
            let count = remove_outputs_on_error(
                &local_output_paths,
                io::copy(&mut output_data.into_reader(), &mut file)
                    .with_context(|| format!("Failed to write output to {}", local_path.display())),
            )?;

            assert!(count == len);
        }
        Ok(ClusterCompileOutput {
            server_id,
            output: jc.output,
            local_output_paths,
            path_transformer,
            custom_toolchain_archive,
        })
    }

    /// Rewrites the outputs for the local machine, and names the build server in stderr if
    /// the remote compiler exited non-zero.
    fn finish(
        response: ClusterCompileOutput,
        outputs_rewriter: Box<dyn OutputsRewriter>,
    ) -> Result<(DistType, process::Output)> {
        let ClusterCompileOutput {
            server_id,
            mut output,
            local_output_paths,
            path_transformer,
            custom_toolchain_archive,
        } = response;
        let extra_inputs = match custom_toolchain_archive {
            Some(p) => vec![p],
            None => vec![],
        };
        remove_outputs_on_error(
            &local_output_paths,
            outputs_rewriter
                .handle_outputs(&path_transformer, &local_output_paths, &extra_inputs)
                .with_context(|| "failed to rewrite outputs from compile"),
        )?;

        if output.code != 0 {
            // Add server info to help diagnose host-specific failures, e.g. due to flaky hardware.
            // Failed builds are not cached so this tampering should not cause too much trouble.
            let server_info = format!("sccache: Job failed on server {}:\n", server_id.addr());
            output.stderr.splice(0..0, server_info.as_bytes().to_vec());
        }

        Ok((DistType::Ok(server_id), output.into()))
    }
}

/// Returns `result`, deleting `outputs` first if it's an error.
fn remove_outputs_on_error<V>(outputs: &[PathBuf], result: Result<V>) -> Result<V> {
    if result.is_err() {
        // Do our best to clear up. We may end up deleting a file that we just wrote over
        // the top of, but it's better to clear up too much than too little
        for local_path in outputs {
            if let Err(e) = fs::remove_file(local_path)
                && e.kind() != io::ErrorKind::NotFound
            {
                warn!(
                    "{} while attempting to clear up {}",
                    e,
                    local_path.display()
                );
            }
        }
    }
    result
}

/// Decides whether to fail the compile or compile locally after an sccache-dist error.
fn classify_error(error: Error, compiler_path: &Path) -> DistError {
    if let Some(HttpClientError(message)) = error.downcast_ref::<HttpClientError>() {
        DistError::FailCompile(FailCompileReason::HttpClientError(message.clone()))
    } else if let Some(lru_disk_cache::Error::FileTooLarge) =
        error.downcast_ref::<lru_disk_cache::Error>()
    {
        DistError::FailCompile(FailCompileReason::ToolchainTooLargeToCache(format!(
            "Could not cache dist toolchain for {:?} locally.
                 Increase `toolchain_cache_size` or decrease the toolchain archive size.",
            compiler_path
        )))
    } else {
        DistError::CompileLocally(error)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_http_client_error_fails_the_compile() {
        let e = Error::new(HttpClientError("unauthorized".to_owned()));
        // Fails the compile, so the error reaches `SccacheService`, which resets the dist
        // client on it.
        match classify_error(e, Path::new("/usr/bin/cc")) {
            DistError::FailCompile(reason) => {
                let e = Error::from(reason);
                assert!(
                    e.is::<HttpClientError>(),
                    "expected an HttpClientError, got {e:#}"
                );
            }
            other => panic!("expected FailCompile, got {other:?}"),
        }
    }

    #[test]
    fn test_toolchain_too_large_for_the_cache_fails_the_compile() {
        let e = Error::new(lru_disk_cache::Error::FileTooLarge);
        match classify_error(e, Path::new("/usr/bin/cc")) {
            DistError::FailCompile(FailCompileReason::ToolchainTooLargeToCache(message)) => {
                assert!(
                    message.contains("toolchain_cache_size"),
                    "expected the toolchain_cache_size hint, got {message}"
                );
            }
            other => panic!("expected FailCompile, got {other:?}"),
        }
    }

    #[test]
    fn test_remove_outputs_on_error() {
        let dir = tempfile::tempdir().unwrap();
        let written = dir.path().join("foo.o");
        let missing = dir.path().join("foo.d");
        fs::write(&written, b"partial").unwrap();
        let outputs = [written.clone(), missing];

        assert_eq!(1, remove_outputs_on_error(&outputs, Ok(1)).unwrap());
        assert!(written.exists());

        let result: Result<()> = remove_outputs_on_error(&outputs, Err(anyhow!("write failed")));
        assert!(result.is_err());
        assert!(!written.exists());
    }

    #[test]
    fn test_other_errors_compile_locally() {
        let result = classify_error(anyhow!("connection refused"), Path::new("/usr/bin/cc"));
        assert!(
            matches!(result, DistError::CompileLocally(_)),
            "expected CompileLocally, got {result:?}"
        );
    }
}
