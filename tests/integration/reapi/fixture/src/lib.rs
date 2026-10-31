#[derive(serde::Serialize)]
pub struct Answer {
    pub value: String,
}

pub fn answer() -> Answer {
    let mut buf = itoa::Buffer::new();
    Answer {
        value: buf.format(42u64).to_owned(),
    }
}

pub fn page_size() -> libc::c_long {
    unsafe { libc::sysconf(libc::_SC_PAGESIZE) }
}

extern "C" {
    fn fixture_c_answer() -> libc::c_int;
    fn fixture_c_file() -> *const libc::c_char;
    fn fixture_cpp_answer() -> libc::c_int;
}

/// The answers of the C and C++ code that the build script compiles.
pub fn c_answers() -> (i32, i32) {
    unsafe { (fixture_c_answer(), fixture_cpp_answer()) }
}

/// The C source's `__FILE__`.
pub fn c_file() -> String {
    let file = unsafe { std::ffi::CStr::from_ptr(fixture_c_file()) };
    file.to_string_lossy().into_owned()
}

#[cfg(test)]
mod tests {
    #[test]
    fn links_its_dependencies() {
        assert_eq!("42", super::answer().value);
        assert!(super::page_size() > 0);
        assert_eq!((41, 42), super::c_answers());
        assert!(super::c_file().ends_with("c/answer.c"), "{}", super::c_file());
    }
}
