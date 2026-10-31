// Copyright 2016 Mozilla Foundation
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

//! The shared libraries an executable loads, as `ldd` lists them.

use fs_err as fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process;
use std::str;

use crate::errors::*;

// The dynamic linker is the only thing that truly knows how dynamic libraries will be
// searched for, so we need to ask it directly.
//
// This function will extract any absolute paths from output like the following:
// $ ldd /bin/ls
//         linux-vdso.so.1 =>  (0x00007ffeb41f6000)
//         libselinux.so.1 => /lib/x86_64-linux-gnu/libselinux.so.1 (0x00007f6877f4f000)
//         libc.so.6 => /lib/x86_64-linux-gnu/libc.so.6 (0x00007f6877b85000)
//         libpcre.so.3 => /lib/x86_64-linux-gnu/libpcre.so.3 (0x00007f6877915000)
//         libdl.so.2 => /lib/x86_64-linux-gnu/libdl.so.2 (0x00007f6877711000)
//         /lib64/ld-linux-x86-64.so.2 (0x00007f6878171000)
//         libpthread.so.0 => /lib/x86_64-linux-gnu/libpthread.so.0 (0x00007f68774f4000)
//
// Elf executables can be statically or dynamically linked, and position independent (PIE) or not:
// - dynamic + PIE = ET_DYN, ldd stdouts something like the list above and exits with code 0
// - dynamic + non-PIE = ET_EXEC, ldd stdouts something like the list above and exits with code 0
// - static + PIE = ET_DYN, ldd stdouts something like "\tstatically linked" or
//   "\tldd (0x7f79ef662000)" and exits with code 0
// - static + non-PIE = ET_EXEC, ldd stderrs something like "\tnot a dynamic executable" or
//   "ldd: a.out: Not a valid dynamic program" and exits with code 1
//
pub(crate) fn find_ldd_libraries(executable: &Path) -> Result<Vec<PathBuf>> {
    let process::Output {
        status,
        stdout,
        stderr,
    } = process::Command::new("ldd").arg(executable).output()?;

    // Not a file ldd can handle. This can be a non-executable, or a static non-PIE
    if !status.success() {
        // Best-effort detection of static non-PIE
        let mut elf = fs::File::open(executable)?;
        let mut elf_bytes = [0; 0x12];
        elf.read_exact(&mut elf_bytes)?;
        if elf_bytes[..0x4] != [0x7f, 0x45, 0x4c, 0x46] {
            bail!("Elf magic not found")
        }
        let little_endian = match elf_bytes[0x5] {
            1 => true,
            2 => false,
            _ => bail!("Invalid endianness in elf header"),
        };
        let e_type = if little_endian {
            ((elf_bytes[0x11] as u16) << 8) | elf_bytes[0x10] as u16
        } else {
            ((elf_bytes[0x10] as u16) << 8) | elf_bytes[0x11] as u16
        };
        if e_type != 0x02 {
            bail!("ldd failed on a non-ET_EXEC elf")
        }
        // It appears to be an ET_EXEC, good enough for us
        return Ok(vec![]);
    }

    if !stderr.is_empty() {
        trace!(
            "ldd stderr non-empty: {:?}",
            String::from_utf8_lossy(&stderr)
        );
    }

    let stdout = str::from_utf8(&stdout).context("ldd output not utf8")?;
    Ok(parse_ldd_output(stdout))
}

// If it's a static PIE the output will be a line like "\tstatically linked", so be forgiving
// in the parsing here and treat parsing oddities as an empty list.
fn parse_ldd_output(stdout: &str) -> Vec<PathBuf> {
    let mut libs = vec![];
    for line in stdout.lines() {
        let line = line.trim();
        let mut parts: Vec<_> = line.split_whitespace().collect();

        // Remove a possible "(0xdeadbeef)" or assume this isn't a library line
        match parts.pop() {
            Some(s) if s.starts_with('(') && s.ends_with(')') => (),
            Some(_) | None => continue,
        }

        if parts.len() > 3 {
            continue;
        }

        let libpath = match (parts.first(), parts.get(1), parts.get(2)) {
            // "linux-vdso.so.1 =>  (0x00007ffeb41f6000)"
            (Some(_libname), Some(&"=>"), None) => continue,
            // "libc.so.6 => /lib/x86_64-linux-gnu/libc.so.6 (0x00007f6877b85000)"
            (Some(libname), Some(&"=>"), Some(libpath)) => {
                // ldd (version 2.30) will output something like this:
                //   ...
                //   /lib64/ld-linux-x86-64.so.2 => /usr/lib64/ld-linux-x86-64.so.2
                //   ...
                // We need to add /lib64/ld-linux-x86-64.so.2 to deps, else we'll get error "No
                // such file or directory".
                //
                // Workaround: add libname to deps if it's absolute and exists.
                let libname_path = PathBuf::from(libname);
                if libname_path.is_absolute() {
                    libs.push(libname_path);
                }

                PathBuf::from(libpath)
            }
            // "/lib64/ld-linux-x86-64.so.2 (0x00007f6878171000)"
            (Some(libpath), None, None) => PathBuf::from(libpath),
            _ => continue,
        };

        if !libpath.is_absolute() {
            continue;
        }

        libs.push(libpath);
    }

    libs
}

#[test]
fn test_ldd_parse() {
    let ubuntu_ls_output = "\tlinux-vdso.so.1 =>  (0x00007fffcfffe000)
\tlibselinux.so.1 => /lib/x86_64-linux-gnu/libselinux.so.1 (0x00007f69caa6b000)
\tlibc.so.6 => /lib/x86_64-linux-gnu/libc.so.6 (0x00007f69ca6a1000)
\tlibpcre.so.3 => /lib/x86_64-linux-gnu/libpcre.so.3 (0x00007f69ca431000)
\tlibdl.so.2 => /lib/x86_64-linux-gnu/libdl.so.2 (0x00007f69ca22d000)
\t/lib64/ld-linux-x86-64.so.2 (0x00007f69cac8d000)
\tlibpthread.so.0 => /lib/x86_64-linux-gnu/libpthread.so.0 (0x00007f69ca010000)
";
    assert_eq!(
        parse_ldd_output(ubuntu_ls_output)
            .iter()
            .map(|p| p.to_str().unwrap())
            .collect::<Vec<_>>(),
        &[
            "/lib/x86_64-linux-gnu/libselinux.so.1",
            "/lib/x86_64-linux-gnu/libc.so.6",
            "/lib/x86_64-linux-gnu/libpcre.so.3",
            "/lib/x86_64-linux-gnu/libdl.so.2",
            "/lib64/ld-linux-x86-64.so.2",
            "/lib/x86_64-linux-gnu/libpthread.so.0",
        ]
    );
}

#[test]
fn test_ldd_parse_static() {
    let static_outputs = &[
        "\tstatically linked",    // glibc ldd output
        "\tldd (0x7f79ef662000)", // musl ldd output
    ];
    for static_output in static_outputs {
        assert_eq!(parse_ldd_output(static_output).len(), 0);
    }
}

#[test]
fn test_ldd_parse_v2_30() {
    let archlinux_ls_output = "\tlinux-vdso.so.1 (0x00007ffddc1f6000)
\tlibcap.so.2 => /usr/lib/libcap.so.2 (0x00007f4980989000)
\tlibc.so.6 => /lib/x86_64-linux-gnu/libc.so.6 (0x00007f69ca6a1000)
\tlibc.so.6 => /usr/lib/libc.so.6 (0x00007f49807c2000)
\t/lib64/ld-linux-x86-64.so.2 => /usr/lib64/ld-linux-x86-64.so.2 (0x00007f49809e9000)
";
    assert_eq!(
        parse_ldd_output(archlinux_ls_output)
            .iter()
            .map(|p| p.to_str().unwrap())
            .collect::<Vec<_>>(),
        &[
            "/usr/lib/libcap.so.2",
            "/lib/x86_64-linux-gnu/libc.so.6",
            "/usr/lib/libc.so.6",
            "/lib64/ld-linux-x86-64.so.2",
            "/usr/lib64/ld-linux-x86-64.so.2",
        ]
    );
}
