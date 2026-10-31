// cc wraps the compiler in `RUSTC_WRAPPER`, so these compiles run through sccache. The `OUT_DIR`
// header maps under the build root.
fn main() {
    let out_dir = std::path::PathBuf::from(std::env::var_os("OUT_DIR").unwrap());
    std::fs::write(out_dir.join("answer_base.h"), "#define ANSWER_BASE 40\n").unwrap();
    cc::Build::new()
        .file("c/answer.c")
        .include(&out_dir)
        .compile("answer-c");
    cc::Build::new()
        .cpp(true)
        .file("c/answer.cpp")
        .include(&out_dir)
        .compile("answer-cpp");
    println!("cargo:rerun-if-changed=c");
}
