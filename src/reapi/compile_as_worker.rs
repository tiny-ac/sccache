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

//! The local compile of a C compile that REAPI could run, which writes the object a worker
//! writes.

use super::mapping::{PathMapper, ReapiCompile, ToolchainKey, WorkerProgram};
use crate::compiler::{CCompileCommand, LocalCompile, SingleCompileCommand};
use crate::errors::*;
use crate::mock_command::CommandCreatorSync;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process;
use tempfile::{NamedTempFile, TempPath};

/// A C compile run locally as a worker runs it: the driver compiles the preprocessed source at
/// its path in a temp dir, with the worker's arguments and the temp dir mapped to `.`.
pub(crate) struct CompileAsWorker {
    driver: PathBuf,
    arguments: Vec<String>,
    path_mapper: PathMapper,
    preprocessed_source: PathBuf,
    mapped_source: String,
    object: PathBuf,
    mapped_object: String,
    /// Keeps the preprocessed source until this compile ends.
    _temp_path: Option<TempPath>,
}

impl CompileAsWorker {
    /// `None` for a compile other than a C compile with its preprocessed source.
    pub(crate) fn new(compile: &mut ReapiCompile) -> Option<Self> {
        let (WorkerProgram::C, ToolchainKey::C(toolchain)) = (&compile.program, &compile.toolchain)
        else {
            return None;
        };
        let source = compile.preprocessed_source.as_mut()?;
        let [object] = compile.outputs.as_slice() else {
            return None;
        };
        Some(CompileAsWorker {
            driver: toolchain.driver.clone(),
            arguments: compile.arguments.clone(),
            path_mapper: compile.path_mapper.clone(),
            preprocessed_source: source.path.clone(),
            mapped_source: source.mapped_path.clone(),
            object: object.clone(),
            mapped_object: compile.path_mapper.to_input_root(object)?,
            _temp_path: source.temp_path.take(),
        })
    }

    /// Compiles as a worker does, or with `local_compile` if that fails, which also gives
    /// diagnostics their local paths.
    pub(crate) async fn run<T: CommandCreatorSync>(
        &self,
        local_compile: &LocalCompile<'_, T>,
        out_pretty: &str,
    ) -> Result<process::Output> {
        match self.compile(local_compile).await {
            Ok(output) => Ok(output),
            Err(e) => {
                debug!("[{out_pretty}]: Compiling with the compile's own command: {e:#}");
                local_compile.run().await
            }
        }
    }

    async fn compile<T: CommandCreatorSync>(
        &self,
        local_compile: &LocalCompile<'_, T>,
    ) -> Result<process::Output> {
        let dir = self.preprocessed_source.parent().context("no parent dir")?;
        let root = tempfile::Builder::new().prefix("root").tempdir_in(dir)?;
        // The compiler names its cwd by its physical path, which the prefix map must match.
        let root_path = root.path().canonicalize()?;
        let root_str = root_path.to_str().context("non-UTF-8 temp dir")?;
        // `=` would end the prefix map's old prefix.
        if root_str.contains([':', '=']) {
            bail!("{root_str} has a : or =");
        }
        let source = root_path.join(&self.mapped_source);
        let object = root_path.join(&self.mapped_object);
        for path in [&source, &object] {
            std::fs::create_dir_all(path.parent().context("no parent dir")?)?;
        }
        if std::fs::hard_link(&self.preprocessed_source, &source).is_err() {
            std::fs::copy(&self.preprocessed_source, &source)?;
        }
        let mut arguments: Vec<OsString> = self.arguments.iter().map(OsString::from).collect();
        arguments.push(format!("-ffile-prefix-map={root_str}=.").into());
        let command = SingleCompileCommand {
            executable: self.driver.clone(),
            arguments,
            env_vars: local_compile.command.get_env_vars(),
            cwd: root_path.clone(),
            share_jobserver: false,
        };
        let mut output = CCompileCommand::new(command)
            .execute(local_compile.service, local_compile.creator)
            .await?;
        move_file(&object, &self.object)?;
        output.stdout = self
            .path_mapper
            .worker_paths_to_local(&output.stdout, root_str);
        output.stderr = self
            .path_mapper
            .worker_paths_to_local(&output.stderr, root_str);
        Ok(output)
    }
}

/// Compiles with `as_worker`, when REAPI could run the compile, or else with `local_compile`.
pub(crate) async fn run_as_worker_or_locally<T: CommandCreatorSync>(
    as_worker: Option<&CompileAsWorker>,
    local_compile: &LocalCompile<'_, T>,
    out_pretty: &str,
) -> Result<process::Output> {
    match as_worker {
        Some(as_worker) => as_worker.run(local_compile, out_pretty).await,
        None => local_compile.run().await,
    }
}

/// Moves `from` to `to`, through a copy next to `to` when they're on different filesystems.
fn move_file(from: &Path, to: &Path) -> Result<()> {
    if std::fs::rename(from, to).is_ok() {
        return Ok(());
    }
    let temp_file = NamedTempFile::new_in(to.parent().context("no parent dir")?)?;
    std::fs::copy(from, temp_file.path())?;
    temp_file.persist(to)?;
    Ok(())
}

// The tests run shell scripts as compilers.
#[cfg(all(test, unix))]
mod test {
    use super::*;
    use crate::compiler::CCompilerKind;
    use crate::jobserver::Client;
    use crate::mock_command::{CommandCreator, ProcessCommandCreator};
    use crate::reapi::PreprocessedSource;
    use crate::reapi::mapping::CToolchainKey;
    use crate::server::SccacheService;
    use crate::test::utils::write_executable;
    use std::sync::Arc;

    const OBJECT: &str = "target/debug/build/foo-1/out/x.o";

    /// A compile of `dir/ws/src/x.c` into `dir/ws/OBJECT` by `toolchain`, whose preprocessed
    /// source under `dir/preprocessed` holds `preprocessed`.
    fn c_compile(
        dir: &Path,
        toolchain: CToolchainKey,
        preprocessed: &str,
        arguments: &[&str],
    ) -> ReapiCompile {
        let ws = dir.join("ws");
        std::fs::create_dir_all(ws.join(OBJECT).parent().unwrap()).unwrap();
        std::fs::create_dir_all(dir.join("preprocessed")).unwrap();
        let mut source = NamedTempFile::new_in(dir.join("preprocessed")).unwrap();
        std::io::Write::write_all(&mut source, preprocessed.as_bytes()).unwrap();
        ReapiCompile {
            path_mapper: PathMapper::new(ws.clone(), ws.join("target")),
            arguments: arguments.iter().map(|arg| arg.to_string()).collect(),
            program: WorkerProgram::C,
            toolchain: ToolchainKey::C(toolchain),
            env: Default::default(),
            input_root_files: vec![],
            preprocessed_source: Some(PreprocessedSource::new(
                source.into_temp_path(),
                "src/x.c".to_owned(),
            )),
            outputs: vec![ws.join(OBJECT)],
        }
    }

    /// A gcc key for a driver at `dir/gcc` that runs `script`.
    fn fake_gcc(dir: &Path, script: &str) -> CToolchainKey {
        let driver = dir.join("gcc");
        write_executable(&driver, script);
        CToolchainKey::new(CCompilerKind::Gcc, driver, vec![]).unwrap()
    }

    /// Runs `as_worker`, with a compile's own command that runs `own_script`.
    fn run(dir: &Path, as_worker: &CompileAsWorker, own_script: &str) -> Result<process::Output> {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let storage = Arc::new(crate::cache::disk::DiskCache::new(
            dir.join("cache"),
            0,
            runtime.handle(),
            Default::default(),
            crate::cache::CacheMode::ReadWrite,
            vec![],
        ));
        let service = SccacheService::mock_with_storage(storage, runtime.handle().clone());
        let creator = <ProcessCommandCreator as CommandCreator>::new(&Client::new_num(1));
        let command = SingleCompileCommand {
            executable: "/bin/sh".into(),
            arguments: vec!["-c".into(), own_script.into()],
            env_vars: vec![("PATH".into(), "/usr/bin:/bin".into())],
            cwd: dir.join("ws"),
            share_jobserver: false,
        };
        let local_compile = LocalCompile {
            service: &service,
            creator: &creator,
            command: CCompileCommand::new(command),
            stdout_stderr_mapper: None,
        };
        runtime.block_on(as_worker.run(&local_compile, "x.c"))
    }

    #[test]
    fn test_a_compile_as_worker_compiles_the_source_at_its_path_in_a_temp_dir() {
        let dir = tempfile::tempdir().unwrap();
        let dir = &dir.path().canonicalize().unwrap();
        let toolchain = fake_gcc(dir, "cat src/x.c > \"$4\"\nprintf '%s\\n' \"$@\" >&2\n");
        let object = format!(".sccache-build/{}", OBJECT.strip_prefix("target/").unwrap());
        let arguments = ["-c", "src/x.c", "-o", &object];
        let mut compile = c_compile(dir, toolchain, "int x;\n", &arguments);
        let as_worker = CompileAsWorker::new(&mut compile).unwrap();
        // The REAPI compile ends before the local one starts.
        drop(compile);
        let output = run(dir, &as_worker, "exit 1").unwrap();
        assert_eq!(
            "int x;\n",
            std::fs::read_to_string(dir.join("ws").join(OBJECT)).unwrap()
        );
        let ws = dir.join("ws");
        let expected = format!(
            "-c\nsrc/x.c\n-o\n{}\n-ffile-prefix-map={}=.\n",
            ws.join(OBJECT).display(),
            ws.display()
        );
        assert_eq!(expected, String::from_utf8_lossy(&output.stderr));
        drop(as_worker);
        let left: Vec<_> = std::fs::read_dir(dir.join("preprocessed"))
            .unwrap()
            .collect();
        assert!(left.is_empty(), "{left:?}");
    }

    #[test_case::test_case("exit 1\n", "" ; "that fails")]
    // This driver would write the object, if the separator didn't stop it running.
    #[test_case::test_case(
        "echo worker > .sccache-build/debug/build/foo-1/out/x.o\n",
        "a=b" ;
        "in a dir with a separator"
    )]
    fn test_a_compile_as_worker_runs_the_compiles_own_command(script: &str, subdir: &str) {
        let dir = tempfile::tempdir().unwrap();
        let dir = &dir.path().canonicalize().unwrap().join(subdir);
        let toolchain = fake_gcc(dir, &format!("echo worker >&2\n{script}"));
        let mut compile = c_compile(dir, toolchain, "int x;\n", &["-c", "src/x.c"]);
        let as_worker = CompileAsWorker::new(&mut compile).unwrap();
        let own_script = format!("echo own > {OBJECT}; echo own");
        let output = run(dir, &as_worker, &own_script).unwrap();
        assert_eq!(b"own\n", output.stdout.as_slice());
        assert!(output.stderr.is_empty(), "{output:?}");
        assert_eq!(
            "own\n",
            std::fs::read_to_string(dir.join("ws").join(OBJECT)).unwrap()
        );
    }
}
