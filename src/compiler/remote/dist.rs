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

use crate::compiler::{Cacheable, Compilation, DistType};
use crate::dist;
use crate::errors::*;
use crate::lru_disk_cache;
use crate::mock_command::CommandCreatorSync;
use crate::server;
use fs::File;
use fs_err as fs;
use std::path::PathBuf;
use std::process;
use std::sync::Arc;

pub(crate) async fn dist_or_local_compile<T>(
    service: &server::SccacheService<T>,
    dist_client: Option<Arc<dyn dist::Client>>,
    creator: T,
    cwd: PathBuf,
    compilation: Box<dyn Compilation<T>>,
    weak_toolchain_key: String,
    out_pretty: String,
) -> Result<(Cacheable, DistType, process::Output)>
where
    T: CommandCreatorSync,
{
    use std::io;

    let rewrite_includes_only = match dist_client {
        Some(ref client) => client.rewrite_includes_only(),
        _ => false,
    };
    let mut path_transformer = dist::PathTransformer::new();
    let (compile_cmd, dist_compile_cmd, cacheable) = compilation
        .generate_compile_commands(&mut path_transformer, rewrite_includes_only)
        .context("Failed to generate compile commands")?;

    let dist_client = match dist_compile_cmd.clone().and(dist_client) {
        Some(dc) => dc,
        None => {
            debug!("[{}]: Compiling locally", out_pretty);
            return compile_cmd
                .execute(service, &creator)
                .await
                .map(move |o| (cacheable, DistType::NoDist, o));
        }
    };

    debug!("[{}]: Attempting distributed compilation", out_pretty);
    let out_pretty2 = out_pretty.clone();

    let local_executable = compile_cmd.get_executable();
    let local_executable2 = compile_cmd.get_executable();

    let do_dist_compile = async move {
        let mut dist_compile_cmd =
            dist_compile_cmd.context("Could not create distributed compile command")?;
        debug!("[{}]: Creating distributed compile request", out_pretty);
        let dist_output_paths = compilation
            .outputs()
            .map(|output| path_transformer.as_dist_abs(&cwd.join(output.path)))
            .collect::<Option<_>>()
            .context("Failed to adapt an output path for distributed compile")?;
        let (inputs_packager, toolchain_packager, outputs_rewriter) =
            compilation.into_dist_packagers(path_transformer)?;

        debug!(
            "[{}]: Identifying dist toolchain for {:?}",
            out_pretty, local_executable
        );
        let (dist_toolchain, maybe_dist_compile_executable) = dist_client
            .put_toolchain(local_executable, weak_toolchain_key, toolchain_packager)
            .await?;
        let mut tc_archive = None;
        if let Some((dist_compile_executable, archive_path)) = maybe_dist_compile_executable {
            dist_compile_cmd.executable = dist_compile_executable;
            tc_archive = Some(archive_path);
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

        let mut jc = match jres {
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
        let mut output_paths: Vec<PathBuf> = vec![];
        macro_rules! try_or_cleanup {
            ($v:expr) => {{
                match $v {
                    Ok(v) => v,
                    Err(e) => {
                        // Do our best to clear up. We may end up deleting a file that we just wrote over
                        // the top of, but it's better to clear up too much than too little
                        for local_path in output_paths.iter() {
                            if let Err(e) = fs::remove_file(local_path) {
                                if e.kind() != io::ErrorKind::NotFound {
                                    warn!("{} while attempting to clear up {}", e, local_path.display())
                                }
                            }
                        }
                        return Err(e)
                    },
                }
            }};
        }

        for (path, output_data) in jc.outputs {
            let len = output_data.lens().actual;
            let local_path = try_or_cleanup!(
                path_transformer
                    .to_local(&path)
                    .with_context(|| format!("unable to transform output path {}", path))
            );
            output_paths.push(local_path);
            // Do this first so cleanup works correctly
            let local_path = output_paths.last().expect("nothing in vec after push");

            let mut file =
                try_or_cleanup!(File::create(local_path).with_context(|| format!(
                    "Failed to create output file {}",
                    local_path.display()
                )));
            let count = try_or_cleanup!(
                io::copy(&mut output_data.into_reader(), &mut file)
                    .with_context(|| format!("Failed to write output to {}", local_path.display()))
            );

            assert!(count == len);
        }
        let extra_inputs = match tc_archive {
            Some(p) => vec![p],
            None => vec![],
        };
        try_or_cleanup!(
            outputs_rewriter
                .handle_outputs(&path_transformer, &output_paths, &extra_inputs)
                .with_context(|| "failed to rewrite outputs from compile")
        );

        if jc.output.code != 0 {
            // Add server info to help diagnose host-specific failures, e.g. due to flaky hardware.
            // Failed builds are not cached so this tampering should not cause too much trouble.
            let server_info = format!("sccache: Job failed on server {}:\n", server_id.addr());
            jc.output
                .stderr
                .splice(0..0, server_info.as_bytes().to_vec());
        }

        Ok((DistType::Ok(server_id), jc.output.into()))
    };

    use futures::TryFutureExt;
    do_dist_compile
        .or_else(move |e| async move {
            if let Some(HttpClientError(_)) = e.downcast_ref::<HttpClientError>() {
                Err(e)
            } else if let Some(lru_disk_cache::Error::FileTooLarge) =
                e.downcast_ref::<lru_disk_cache::Error>()
            {
                Err(anyhow!(
                    "Could not cache dist toolchain for {:?} locally.
                 Increase `toolchain_cache_size` or decrease the toolchain archive size.",
                    local_executable2
                ))
            } else {
                // `{:#}` prints the error and the causes in a single line.
                let errmsg = format!("{:#}", e);
                warn!(
                    "[{}]: Could not perform distributed compile, falling back to local: {}",
                    out_pretty2, errmsg
                );

                compile_cmd
                    .execute(service, &creator)
                    .await
                    .map(|o| (DistType::Error, o))
            }
        })
        .map_ok(move |(dt, o)| (cacheable, dt, o))
        .await
}
