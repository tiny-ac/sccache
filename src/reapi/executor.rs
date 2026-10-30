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

//! Runs the cache misses of a build with `[dist.reapi]` locally, with the arguments a REAPI
//! worker would use. The stats count the compiles a REAPI server couldn't run, by reason.

use crate::compiler::{
    Compilation, DistInputOptions, DistType, LocalCompile, LocalCompileChanges, ReapiOutcome,
    RemoteExecutor, RemoteJob,
};
use crate::errors::*;
use crate::mock_command::CommandCreatorSync;
use async_trait::async_trait;
use std::path::{Path, PathBuf};
use std::process;

/// Directory under the dist cache dir for the sysroot copies local compiles use.
const SYSROOT_COPIES_DIR: &str = "reapi-sysroot-copies";

/// A [`RemoteExecutor`] for a REAPI server.
pub struct ReapiExecutor {
    sysroot_copies_dir: PathBuf,
}

impl ReapiExecutor {
    pub fn new(dist_cache_dir: &Path) -> Self {
        ReapiExecutor {
            sysroot_copies_dir: dist_cache_dir.join(SYSROOT_COPIES_DIR),
        }
    }
}

#[async_trait]
impl<T: CommandCreatorSync> RemoteExecutor<T> for ReapiExecutor {
    fn dist_input_options(&self) -> Option<DistInputOptions> {
        None
    }

    fn local_compile_changes(&self, compilation: &dyn Compilation<T>) -> LocalCompileChanges {
        compilation
            .reapi_local_compile_changes(&self.sysroot_copies_dir)
            .unwrap_or_default()
    }

    async fn compile(
        &self,
        job: RemoteJob<T>,
        local_compile: LocalCompile<'_, T>,
    ) -> Result<(DistType, process::Output)> {
        let RemoteJob {
            compilation,
            out_pretty,
            ..
        } = job;
        let mapped = tokio::task::spawn_blocking(move || compilation.map_for_reapi()).await;
        let dist_type = match mapped {
            Ok(Ok(_)) => DistType::NoDist,
            // A panic is a bug in sccache, which shouldn't fail a compile that runs locally.
            Err(e) => {
                warn!(
                    "[{}]: Compiling locally: failed to check the compile for REAPI: {}",
                    out_pretty, e
                );
                DistType::NoDist
            }
            Ok(Err(not_eligible)) => {
                match &not_eligible.detail {
                    Some(detail) => debug!(
                        "[{}]: Compiling locally: {}: {:#}",
                        out_pretty, not_eligible.reason, detail
                    ),
                    None => debug!(
                        "[{}]: Compiling locally: {}",
                        out_pretty, not_eligible.reason
                    ),
                }
                // The stats count reasons by their text, as they do non-cacheable ones.
                DistType::Reapi(ReapiOutcome::CompiledLocally(
                    not_eligible.reason.to_string(),
                ))
            }
        };
        Ok((dist_type, local_compile.run().await?))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::compiler::{Cacheable, CompileCommand, LocalArg, SingleCompileCommand};
    use crate::dist;
    use crate::mock_command::{MockChild, MockCommandCreator, exit_status};
    use crate::reapi::{CompileLocallyReason, PathMapper, ReapiCompile};
    use crate::server::SccacheService;
    use crate::test::utils::{new_creator, next_command};
    use std::ffi::OsString;
    use std::sync::{Arc, Mutex};

    type Creator = Arc<Mutex<MockCommandCreator>>;

    #[derive(Clone, Copy)]
    enum Eligibility {
        Eligible,
        Ineligible,
        Panics,
    }

    /// A compile whose local compile gets `--sysroot=<sysroot_copies_dir>`.
    struct FakeCompilation(Eligibility);

    impl Compilation<Creator> for FakeCompilation {
        fn generate_compile_commands(
            &self,
            _: &mut dist::PathTransformer,
            _: bool,
        ) -> Result<(
            Box<dyn CompileCommand<Creator>>,
            Option<dist::CompileCommand>,
            Cacheable,
        )> {
            unimplemented!()
        }

        fn map_for_reapi(&self) -> std::result::Result<ReapiCompile, crate::reapi::NotEligible> {
            match self.0 {
                Eligibility::Eligible => {}
                Eligibility::Ineligible => return Err(CompileLocallyReason::Staticlib.into()),
                Eligibility::Panics => panic!("map_for_reapi"),
            }
            Ok(ReapiCompile {
                path_mapper: PathMapper::new("/src".into(), "/src/target".into()),
                arguments: vec![],
                mapped_crate_root: None,
                target_triple: "x86_64-unknown-linux-gnu".to_owned(),
                env: Default::default(),
                input_root_files: vec![],
                outputs: vec![],
            })
        }

        fn reapi_local_compile_changes(
            &self,
            sysroot_copies_dir: &Path,
        ) -> Option<LocalCompileChanges> {
            let mut arg = OsString::from("--sysroot=");
            arg.push(sysroot_copies_dir);
            Some(LocalCompileChanges {
                args: vec![LocalArg {
                    hashed_as: arg.clone(),
                    arg,
                }],
                stdout_stderr_mapper: None,
            })
        }

        #[cfg(feature = "dist-client")]
        fn into_dist_packagers(
            self: Box<Self>,
            _: dist::PathTransformer,
        ) -> Result<crate::compiler::DistPackagers> {
            unimplemented!()
        }

        fn outputs<'a>(&'a self) -> Box<dyn Iterator<Item = crate::cache::FileObjectSource> + 'a> {
            Box::new(std::iter::empty())
        }
    }

    fn compile(eligibility: Eligibility) -> (DistType, process::Output) {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let cache_dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(crate::cache::disk::DiskCache::new(
            cache_dir.path(),
            0,
            runtime.handle(),
            Default::default(),
            crate::cache::CacheMode::ReadWrite,
            vec![],
        ));
        let service = SccacheService::mock_with_storage(storage, runtime.handle().clone());
        let creator = new_creator();
        next_command(&creator, Ok(MockChild::new(exit_status(0), "out", "err")));
        let command = SingleCompileCommand {
            executable: "rustc".into(),
            arguments: vec![],
            env_vars: vec![],
            cwd: ".".into(),
            share_jobserver: false,
        };
        let local_compile = LocalCompile {
            service: &service,
            creator: &creator,
            command: crate::compiler::CCompileCommand::new(command),
            stdout_stderr_mapper: None,
        };
        let job = RemoteJob {
            compilation: Box::new(FakeCompilation(eligibility)),
            out_pretty: "lib.rs".to_owned(),
            dist_job: None,
        };
        let executor = ReapiExecutor::new("/cache".as_ref());
        runtime
            .block_on(executor.compile(job, local_compile))
            .unwrap()
    }

    #[test]
    fn test_an_ineligible_compile_runs_locally_and_counts_why() {
        let (dist_type, output) = compile(Eligibility::Ineligible);
        assert_eq!(
            DistType::Reapi(ReapiOutcome::CompiledLocally(
                "staticlib crate type".to_owned()
            )),
            dist_type
        );
        assert_eq!(b"out".as_slice(), output.stdout);
    }

    #[test]
    fn test_an_eligible_compile_runs_locally_without_a_reason() {
        let (dist_type, output) = compile(Eligibility::Eligible);
        assert_eq!(DistType::NoDist, dist_type);
        assert_eq!(b"err".as_slice(), output.stderr);
    }

    #[test]
    fn test_a_panicking_eligibility_check_still_compiles_locally() {
        let (dist_type, output) = compile(Eligibility::Panics);
        assert_eq!(DistType::NoDist, dist_type);
        assert_eq!(b"out".as_slice(), output.stdout);
    }

    #[test]
    fn test_local_compiles_get_the_sysroot_copies_dir_under_the_dist_cache_dir() {
        let executor = ReapiExecutor::new("/cache".as_ref());
        let changes = RemoteExecutor::<Creator>::local_compile_changes(
            &executor,
            &FakeCompilation(Eligibility::Eligible),
        );
        let mut expected = OsString::from("--sysroot=");
        expected.push(Path::new("/cache").join("reapi-sysroot-copies"));
        let args: Vec<_> = changes.args.into_iter().map(|a| a.arg).collect();
        assert_eq!(vec![expected], args);
    }
}
