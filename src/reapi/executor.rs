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
use super::digest::DigestCache;
use super::mapping::{CompileLocallyReason, ReapiCompile};
use super::toolchains::{ToolchainFiles, Toolchains};
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
/// File under the dist cache dir that keeps toolchain file digests across restarts.
const DIGEST_CACHE_FILE: &str = "reapi-digests.json";
/// Caps `ready_wait_secs`, since `Instant::now() + ready_wait` panics on overflow.
const MAX_READY_WAIT: Duration = Duration::from_secs(3600);

/// A [`RemoteExecutor`] for a REAPI server.
pub struct ReapiExecutor {
    /// `None` when `[dist.reapi]` is misconfigured.
    connection: Option<Arc<Connection>>,
    digest_cache: Arc<DigestCache>,
    toolchains: Arc<Toolchains>,
    ready_wait: Duration,
    sysroot_copies_dir: PathBuf,
}

/// What a lookup in the action cache needs.
struct Lookup {
    reapi_compile: ReapiCompile,
    apis: Arc<ServerApis>,
    toolchain: ToolchainFiles,
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
        let digest_cache = Arc::new(DigestCache::load(dist_cache_dir.join(DIGEST_CACHE_FILE)));
        ReapiExecutor {
            connection,
            toolchains: Toolchains::new(digest_cache.clone()),
            digest_cache,
            ready_wait,
            sysroot_copies_dir: dist_cache_dir.join(SYSROOT_COPIES_DIR),
        }
    }

    /// What looking `compilation` up needs, or why it compiles locally.
    async fn prepare_lookup<T: CommandCreatorSync>(
        &self,
        compilation: Box<dyn Compilation<T>>,
        out_pretty: &str,
    ) -> std::result::Result<Lookup, CompileLocallyReason> {
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
        let deadline = Instant::now() + self.ready_wait;
        let ready = tokio::try_join!(
            connection.server_apis(deadline),
            self.toolchains.files(&reapi_compile.toolchain, deadline)
        );
        match ready {
            Ok((apis, toolchain)) => Ok(Lookup {
                reapi_compile,
                apis,
                toolchain,
            }),
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
        let dist_type = match self.prepare_lookup(compilation, &out_pretty).await {
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
    use crate::compiler::{
        CacheControl, Cacheable, CompileCommand, LocalArg, SingleCompileCommand,
    };
    use crate::dist;
    use crate::mock_command::{MockChild, MockCommandCreator, exit_status};
    use crate::reapi::fake::FakeServer;
    use crate::reapi::remote_compile::test::fake_sysroot;
    use crate::reapi::{PathMapper, ToolchainKey};
    use crate::server::SccacheService;
    use crate::test::utils::{new_creator, next_command};
    use std::ffi::OsString;
    use std::sync::Mutex;

    type Creator = Arc<Mutex<MockCommandCreator>>;

    const TARGET_TRIPLE: &str = "x86_64-unknown-linux-gnu";
    const RLIB: &str = "target/debug/deps/libfoo.rlib";

    #[derive(Clone, Copy)]
    enum Eligibility {
        Eligible,
        Ineligible,
        Panics,
    }

    /// A compile of `dir/ws/src/lib.rs` into `dir/ws/target/debug/deps/libfoo.rlib` with the
    /// toolchain at `dir/sysroot`. Its local compile gets `--sysroot=<sysroot_copies_dir>`.
    struct FakeCompilation {
        eligibility: Eligibility,
        dir: PathBuf,
    }

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
            match self.eligibility {
                Eligibility::Eligible => {}
                Eligibility::Ineligible => return Err(CompileLocallyReason::Staticlib.into()),
                Eligibility::Panics => panic!("map_for_reapi"),
            }
            let ws = self.dir.join("ws");
            Ok(ReapiCompile {
                path_mapper: PathMapper::new(ws.clone(), ws.join("target")),
                arguments: vec![],
                mapped_crate_root: None,
                toolchain: ToolchainKey::new(
                    "abc123".to_owned(),
                    self.dir.join("sysroot"),
                    TARGET_TRIPLE.to_owned(),
                ),
                env: Default::default(),
                input_root_files: vec![ws.join("src/lib.rs")],
                outputs: vec![ws.join(RLIB)],
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

    /// An executor that waits up to 10s for a fake server, and a temp dir holding the
    /// toolchain and `ws/src/lib.rs`.
    struct Fixture {
        runtime: tokio::runtime::Runtime,
        server: Arc<FakeServer>,
        dir: tempfile::TempDir,
        config: ReapiConfig,
        eligibility: Eligibility,
        cache_control: CacheControl,
        local_compile_changed: bool,
    }

    impl Fixture {
        fn new(eligibility: Eligibility) -> Self {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            let server = Arc::new(FakeServer::default());
            let config = config(runtime.block_on(server.serve()), 10);
            let dir = tempfile::tempdir().unwrap();
            fake_sysroot(dir.path());
            let lib_rs = dir.path().join("ws/src/lib.rs");
            std::fs::create_dir_all(lib_rs.parent().unwrap()).unwrap();
            std::fs::write(&lib_rs, "pub fn f() {}").unwrap();
            // Cargo creates the output directory before running rustc.
            std::fs::create_dir_all(dir.path().join("ws/target/debug/deps")).unwrap();
            Fixture {
                runtime,
                server,
                dir,
                config,
                eligibility,
                cache_control: CacheControl::Default,
                local_compile_changed: true,
            }
        }

        fn compilation(&self) -> FakeCompilation {
            FakeCompilation {
                eligibility: self.eligibility,
                dir: self.dir.path().to_owned(),
            }
        }

        /// Runs the compile through the executor. A local compile writes `out` to stdout and
        /// `err` to stderr.
        fn compile(&self) -> (DistType, process::Output) {
            let executor = ReapiExecutor::new(&self.config, &self.dir.path().join("dist"));
            let storage = Arc::new(crate::cache::disk::DiskCache::new(
                self.dir.path(),
                0,
                self.runtime.handle(),
                Default::default(),
                crate::cache::CacheMode::ReadWrite,
                vec![],
            ));
            let service = SccacheService::mock_with_storage(storage, self.runtime.handle().clone());
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
                compilation: Box::new(self.compilation()),
                out_pretty: "lib.rs".to_owned(),
                dist_job: None,
                cache_control: self.cache_control,
                local_compile_changed: self.local_compile_changed,
            };
            self.runtime
                .block_on(executor.compile(job, local_compile))
                .unwrap()
        }
    }

    /// Compiles with the config changed by `configure`, which gets the fixture's temp dir.
    fn compile(
        eligibility: Eligibility,
        configure: impl FnOnce(&mut ReapiConfig, &Path),
    ) -> (DistType, process::Output) {
        let mut fixture = Fixture::new(eligibility);
        configure(&mut fixture.config, fixture.dir.path());
        fixture.compile()
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
    fn test_an_eligible_compile_runs_locally_without_a_reason_once_ready() {
        let (dist_type, output) = compile(Eligibility::Eligible, |_, _| {});
        assert_eq!(DistType::NoDist, dist_type);
        assert_eq!(b"err".as_slice(), output.stderr);
    }

    #[test]
    fn test_an_eligible_compile_with_a_toolchain_without_std_runs_locally() {
        let (dist_type, _) = compile(Eligibility::Eligible, |_, dir| {
            std::fs::remove_dir_all(dir.join("sysroot/lib/rustlib")).unwrap();
        });
        assert_eq!(
            compiled_locally("toolchain without rustc or std"),
            dist_type
        );
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
            &FakeCompilation {
                eligibility: Eligibility::Eligible,
                dir: "/".into(),
            },
        );
        let mut expected = OsString::from("--sysroot=");
        expected.push(Path::new("/cache").join("reapi-sysroot-copies"));
        let args: Vec<_> = changes.args.into_iter().map(|a| a.arg).collect();
        assert_eq!(vec![expected], args);
    }
}
