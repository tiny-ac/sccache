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
//! worker would use. The stats count the compiles that couldn't use the REAPI server, by
//! reason.

use super::client::{ConnectOptions, check_connect_options};
use super::connection::{Connection, ServerApis};
use super::mapping::{CompileLocallyReason, ReapiCompile};
use crate::compiler::{
    Compilation, DistInputOptions, DistType, LocalCompile, LocalCompileChanges, ReapiOutcome,
    RemoteExecutor, RemoteJob,
};
use crate::config::ReapiConfig;
use crate::errors::*;
use crate::mock_command::CommandCreatorSync;
use async_trait::async_trait;
use std::path::{Path, PathBuf};
use std::process;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Directory under the dist cache dir for the sysroot copies local compiles use.
const SYSROOT_COPIES_DIR: &str = "reapi-sysroot-copies";
/// Caps `ready_wait_secs`, since `Instant::now() + ready_wait` panics on overflow.
const MAX_READY_WAIT: Duration = Duration::from_secs(3600);

/// A [`RemoteExecutor`] for a REAPI server.
pub struct ReapiExecutor {
    /// `None` when `[dist.reapi]` is misconfigured.
    connection: Option<Arc<Connection>>,
    ready_wait: Duration,
    sysroot_copies_dir: PathBuf,
}

impl ReapiExecutor {
    /// Reads the auth token once, so a changed token takes a restart.
    pub fn new(config: &ReapiConfig, dist_cache_dir: &Path) -> Self {
        let connection = match read_connect_options(config) {
            Ok(options) => Some(Connection::new(options)),
            Err(e) => {
                warn!("REAPI: {e:#}; every compile runs locally");
                None
            }
        };
        let mut ready_wait = Duration::from_secs(config.ready_wait_secs);
        if ready_wait > MAX_READY_WAIT {
            warn!(
                "REAPI: ready_wait_secs is capped at {}",
                MAX_READY_WAIT.as_secs()
            );
            ready_wait = MAX_READY_WAIT;
        }
        ReapiExecutor {
            connection,
            ready_wait,
            sysroot_copies_dir: dist_cache_dir.join(SYSROOT_COPIES_DIR),
        }
    }

    /// `compilation` mapped for REAPI and the server's APIs, or why it compiles locally.
    async fn map_and_connect<T: CommandCreatorSync>(
        &self,
        compilation: Box<dyn Compilation<T>>,
        out_pretty: &str,
    ) -> std::result::Result<(ReapiCompile, Arc<ServerApis>), CompileLocallyReason> {
        let mapped = tokio::task::spawn_blocking(move || compilation.map_for_reapi()).await;
        let reapi_compile = match mapped {
            Ok(Ok(reapi_compile)) => reapi_compile,
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
                return Err(not_eligible.reason);
            }
            // A panic is a bug in sccache, which shouldn't fail a compile that runs locally.
            Err(e) => {
                warn!(
                    "[{}]: Compiling locally: failed to map the compile for REAPI: {}",
                    out_pretty, e
                );
                return Err(CompileLocallyReason::MappingPanicked);
            }
        };
        let Some(connection) = &self.connection else {
            return Err(CompileLocallyReason::Misconfigured);
        };
        match connection
            .server_apis(Instant::now() + self.ready_wait)
            .await
        {
            Ok(apis) => Ok((reapi_compile, apis)),
            Err(reason) => {
                debug!("[{}]: Compiling locally: {}", out_pretty, reason);
                Err(reason)
            }
        }
    }
}

/// Reads the auth token file and checks the options for connecting to `config`'s server,
/// without connecting.
fn read_connect_options(config: &ReapiConfig) -> Result<ConnectOptions> {
    let auth_token = config
        .auth_token_file
        .as_ref()
        .map(|file| {
            std::fs::read_to_string(file).with_context(|| {
                format!(
                    "failed to read the REAPI auth token from {}",
                    file.display()
                )
            })
        })
        .transpose()?;
    let options = ConnectOptions {
        endpoint: config.endpoint.clone(),
        instance_name: config.instance_name.clone(),
        auth_token,
    };
    check_connect_options(&options)?;
    Ok(options)
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
        let dist_type = match self.map_and_connect(compilation, &out_pretty).await {
            Ok(_) => DistType::NoDist,
            // The stats count reasons by their text, as they do non-cacheable ones.
            Err(reason) => DistType::Reapi(ReapiOutcome::CompiledLocally(reason.to_string())),
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
    use crate::reapi::PathMapper;
    use crate::reapi::fake::FakeServer;
    use crate::server::SccacheService;
    use crate::test::utils::{new_creator, next_command};
    use std::ffi::OsString;
    use std::sync::Mutex;

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

    fn config(endpoint: String, ready_wait_secs: u64) -> ReapiConfig {
        ReapiConfig {
            endpoint,
            instance_name: String::new(),
            auth_token_file: None,
            ready_wait_secs,
        }
    }

    /// Compiles through an executor for a fake server, waiting 10s for it, with the config
    /// changed by `configure`, which gets a temp dir.
    fn compile(
        eligibility: Eligibility,
        configure: impl FnOnce(&mut ReapiConfig, &Path),
    ) -> (DistType, process::Output) {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let server = Arc::new(FakeServer::default());
        let mut config = config(runtime.block_on(server.serve()), 10);
        let cache_dir = tempfile::tempdir().unwrap();
        configure(&mut config, cache_dir.path());
        let executor = ReapiExecutor::new(&config, "/cache".as_ref());
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
        runtime
            .block_on(executor.compile(job, local_compile))
            .unwrap()
    }

    fn compiled_locally(reason: &str) -> DistType {
        DistType::Reapi(ReapiOutcome::CompiledLocally(reason.to_owned()))
    }

    #[test]
    fn test_an_ineligible_compile_runs_locally_and_counts_why() {
        let (dist_type, output) = compile(Eligibility::Ineligible, |_, _| {});
        assert_eq!(compiled_locally("staticlib crate type"), dist_type);
        assert_eq!(b"out".as_slice(), output.stdout);
    }

    #[test]
    fn test_an_eligible_compile_runs_locally_without_a_reason_once_connected() {
        let (dist_type, output) = compile(Eligibility::Eligible, |_, _| {});
        assert_eq!(DistType::NoDist, dist_type);
        assert_eq!(b"err".as_slice(), output.stderr);
    }

    #[test]
    fn test_an_eligible_compile_that_doesnt_wait_finds_the_server_connecting() {
        let (dist_type, _) = compile(Eligibility::Eligible, |config, _| {
            config.ready_wait_secs = 0;
        });
        assert_eq!(
            compiled_locally("connecting to the REAPI server"),
            dist_type
        );
    }

    #[test_case::test_case(
        |config, _| config.endpoint = "farm:8980".to_owned() ;
        "bad endpoint"
    )]
    #[test_case::test_case(
        |config, dir| config.auth_token_file = Some(dir.join("missing")) ;
        "unreadable token file"
    )]
    #[test_case::test_case(
        |config, dir| {
            std::fs::write(dir.join("token"), " \n").unwrap();
            config.auth_token_file = Some(dir.join("token"));
        } ;
        "empty token file"
    )]
    fn test_a_misconfigured_executor_compiles_eligible_compiles_locally(
        configure: fn(&mut ReapiConfig, &Path),
    ) {
        let (dist_type, output) = compile(Eligibility::Eligible, configure);
        assert_eq!(compiled_locally("[dist.reapi] misconfigured"), dist_type);
        assert_eq!(b"out".as_slice(), output.stdout);
        let (dist_type, _) = compile(Eligibility::Ineligible, configure);
        assert_eq!(compiled_locally("staticlib crate type"), dist_type);
    }

    #[test]
    fn test_a_panicking_mapping_still_compiles_locally() {
        let (dist_type, output) = compile(Eligibility::Panics, |_, _| {});
        assert_eq!(
            compiled_locally("mapping the compile for REAPI panicked"),
            dist_type
        );
        assert_eq!(b"out".as_slice(), output.stdout);
    }

    #[test]
    fn test_local_compiles_get_the_sysroot_copies_dir_under_the_dist_cache_dir() {
        let executor =
            ReapiExecutor::new(&config("grpc://farm:8980".to_owned(), 0), "/cache".as_ref());
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
