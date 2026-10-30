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

#[cfg(test)]
mod tests {
    #[test]
    fn links_its_dependencies() {
        assert_eq!("42", super::answer().value);
        assert!(super::page_size() > 0);
    }
}
