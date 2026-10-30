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

//! A Rust compile's REAPI command and toolchain files.

use super::mapping::{BUILD_ROOT_DIR, ReapiCompile, TOOLCHAIN_DIR};
use super::proto::build::bazel::remote::execution::v2 as remexec;

/// Starts the stderr line where the worker reports the worker root.
const WORKER_ROOT_MARKER: &str = "sccache-reapi-pwd:";

/// The command a worker runs for `compile`, which doesn't depend on the checkout path. A
/// shell script adds the worker root, known only when the action runs, to path env vars and
/// rustc's arguments.
pub(super) fn build_command(compile: &ReapiCompile) -> remexec::Command {
    // rustc's paths use the physical cwd, so $PWD must too for the remaps and the worker
    // root line to match them. The worker root line lets the client machine map the worker
    // root in stdout and stderr to local paths.
    let mut script = format!(
        "set -e\ncd -P .\nPWD=$(pwd -P)\nprintf '{WORKER_ROOT_MARKER}%s\\n' \"$PWD\" >&2\n"
    );
    // Names go into the script unquoted; the mapping only lets identifiers through.
    debug_assert!(
        compile
            .env
            .path_vars
            .iter()
            .map(|(name, _)| name)
            .chain(&compile.env.unset_vars)
            .all(|name| is_identifier(name)),
        "{:?}",
        compile.env
    );
    let mut unset_vars = compile.env.unset_vars.clone();
    unset_vars.sort();
    if !unset_vars.is_empty() {
        script.push_str(&format!("unset {}\n", unset_vars.join(" ")));
    }
    let mut path_vars = compile.env.path_vars.clone();
    path_vars.sort();
    for (name, path) in &path_vars {
        script.push_str(&format!("export {name}=\"$PWD\"/{}\n", sh_quote(path)));
    }
    let mapped_crate_root_arg = match &compile.mapped_crate_root {
        Some(path) => format!(" \"$PWD\"/{}", sh_quote(path)),
        None => String::new(),
    };
    // Execution may hardlink input root files from their CAS, so rustc may not find its
    // libraries and sysroot from its own path; the script sets both.
    // rustc applies the last matching --remap-path-prefix, so BUILD_ROOT_DIR's comes last.
    script.push_str(&format!(
        "export LD_LIBRARY_PATH=\"$PWD\"/{TOOLCHAIN_DIR}/lib DYLD_LIBRARY_PATH=\"$PWD\"/{TOOLCHAIN_DIR}/lib\n\
         exec \"$PWD\"/{TOOLCHAIN_DIR}/bin/rustc --sysroot \"$PWD\"/{TOOLCHAIN_DIR} \"$@\"{mapped_crate_root_arg} \
         --remap-path-prefix=\"$PWD\"=. --remap-path-prefix=\"$PWD\"/{BUILD_ROOT_DIR}={BUILD_ROOT_DIR}\n"
    ));
    let mut arguments = vec![
        "/bin/sh".to_owned(),
        "-c".to_owned(),
        script,
        "rustc".to_owned(),
    ];
    arguments.extend(compile.arguments.iter().cloned());
    // REAPI requires both lists sorted.
    let mut vars = compile.env.vars.clone();
    vars.sort();
    let mut output_paths: Vec<String> = command_output_paths(compile).collect();
    output_paths.sort();
    output_paths.dedup();
    remexec::Command {
        arguments,
        environment_variables: vars
            .into_iter()
            .map(|(name, value)| remexec::command::EnvironmentVariable { name, value })
            .collect(),
        output_paths,
        ..Default::default()
    }
}

/// The paths `compile`'s command declares as outputs, input-root-relative.
fn command_output_paths(compile: &ReapiCompile) -> impl Iterator<Item = String> + '_ {
    compile
        .outputs
        .iter()
        .filter_map(|output| compile.path_mapper.to_input_root(output))
}

fn is_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn sh_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', r"'\''"))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::reapi::{ActionEnv, PathMapper};

    const RLIB: &str = ".sccache-build/debug/deps/libfoo.rlib";
    const RMETA: &str = ".sccache-build/debug/deps/libfoo.rmeta";

    /// A compile in `cwd` that declares an rmeta and an rlib.
    fn reapi_compile_in(cwd: &str) -> ReapiCompile {
        ReapiCompile {
            path_mapper: PathMapper::new(cwd.into(), format!("{cwd}/target").into()),
            arguments: vec!["--crate-name=foo".to_owned()],
            mapped_crate_root: Some("src/lib.rs".to_owned()),
            target_triple: "x86_64-unknown-linux-gnu".to_owned(),
            env: ActionEnv {
                vars: vec![
                    ("CARGO_PKG_VERSION".to_owned(), "1.0.0".to_owned()),
                    ("CARGO_CRATE_NAME".to_owned(), "foo".to_owned()),
                ],
                path_vars: vec![
                    (
                        "OUT_DIR".to_owned(),
                        ".sccache-build/debug/build/foo-1/out".to_owned(),
                    ),
                    ("CARGO_MANIFEST_DIR".to_owned(), ".".to_owned()),
                ],
                unset_vars: vec!["HOSTNAME".to_owned(), "GIT_HASH".to_owned()],
            },
            input_root_files: vec![],
            outputs: vec![
                format!("{cwd}/target/debug/deps/libfoo.rmeta").into(),
                format!("{cwd}/target/debug/deps/libfoo.rlib").into(),
            ],
        }
    }

    #[test]
    fn test_the_command_doesnt_depend_on_the_checkout_path() {
        assert_eq!(
            build_command(&reapi_compile_in("/home/a/ws")),
            build_command(&reapi_compile_in("/srv/b/ws"))
        );
    }

    #[test]
    fn test_the_command_sorts_its_outputs_and_vars() {
        let command = build_command(&reapi_compile_in("/ws"));
        assert_eq!([RLIB, RMETA], command.output_paths.as_slice());
        let var_names: Vec<&str> = command
            .environment_variables
            .iter()
            .map(|var| var.name.as_str())
            .collect();
        assert_eq!(
            ["CARGO_CRATE_NAME", "CARGO_PKG_VERSION"],
            var_names.as_slice()
        );
    }

    #[test]
    fn test_the_script_sets_path_vars_under_the_worker_root() {
        let command = build_command(&reapi_compile_in("/ws"));
        let script = &command.arguments[2];
        assert!(
            script.contains(
                "unset GIT_HASH HOSTNAME\n\
                 export CARGO_MANIFEST_DIR=\"$PWD\"/'.'\n\
                 export OUT_DIR=\"$PWD\"/'.sccache-build/debug/build/foo-1/out'\n"
            ),
            "{script}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn test_the_command_runs_rustc_with_worker_paths() {
        use std::os::unix::fs::PermissionsExt;
        let worker_root = tempfile::tempdir().unwrap();
        let worker_root = worker_root.path().canonicalize().unwrap();
        let rustc = worker_root.join(".sccache-toolchain/bin/rustc");
        std::fs::create_dir_all(rustc.parent().unwrap()).unwrap();
        // A fake rustc in a shell that doesn't set GIT_HASH itself, as bash does HOSTNAME.
        std::fs::write(
            &rustc,
            "#!/bin/sh\nprintf '%s\\n' \"$@\" \"OUT_DIR=$OUT_DIR\" \"GIT_HASH=${GIT_HASH-unset}\" \
             \"LD_LIBRARY_PATH=$LD_LIBRARY_PATH\"\n",
        )
        .unwrap();
        std::fs::set_permissions(&rustc, std::fs::Permissions::from_mode(0o755)).unwrap();
        let command = build_command(&reapi_compile_in("/home/a/ws"));
        let output = std::process::Command::new(&command.arguments[0])
            .args(&command.arguments[1..])
            .current_dir(&worker_root)
            .env("GIT_HASH", "abc")
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        let w = worker_root.display();
        assert_eq!(
            format!(
                "--sysroot\n{w}/.sccache-toolchain\n--crate-name=foo\n{w}/src/lib.rs\n\
                 --remap-path-prefix={w}=.\n\
                 --remap-path-prefix={w}/.sccache-build=.sccache-build\n\
                 OUT_DIR={w}/.sccache-build/debug/build/foo-1/out\nGIT_HASH=unset\n\
                 LD_LIBRARY_PATH={w}/.sccache-toolchain/lib\n"
            ),
            String::from_utf8(output.stdout).unwrap()
        );
        assert_eq!(
            format!("{WORKER_ROOT_MARKER}{w}\n"),
            String::from_utf8(output.stderr).unwrap()
        );
    }

    #[test]
    fn test_sh_quote() {
        assert_eq!(r"'it'\''s'", sh_quote("it's"));
    }
}
