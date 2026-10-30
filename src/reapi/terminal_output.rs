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

//! Removes terminal controls from a worker's stdout and stderr before sccache replays them,
//! since a REAPI server could otherwise set the terminal's title or clipboard, or rewrite what
//! the terminal shows. Colors stay.

const ESC: u32 = 0x1b;
const BEL: u32 = 0x07;
/// The 8-bit forms of CSI, OSC and ST.
const CSI_8BIT: u32 = 0x9b;
const OSC_8BIT: u32 = 0x9d;
const ST_8BIT: u32 = 0x9c;

/// A character of a line, and how many bytes it takes there. In a JSON line, an escape such
/// as `\u001b` counts as the character it encodes.
#[derive(Clone, Copy)]
struct DecodedChar {
    code: u32,
    len: usize,
}

/// The character at `line[i..]`.
fn char_at(line: &str, i: usize, json: bool) -> Option<DecodedChar> {
    let rest = &line.as_bytes()[i..];
    if json && rest.first() == Some(&b'\\') {
        let escaped = match rest.get(1) {
            Some(b'u') => rest
                .get(2..6)
                .filter(|hex| hex.iter().all(u8::is_ascii_hexdigit))
                .and_then(|hex| u32::from_str_radix(std::str::from_utf8(hex).ok()?, 16).ok())
                .map(|code| (code, 6)),
            Some(b'b') => Some((0x08, 2)),
            Some(b'f') => Some((0x0c, 2)),
            Some(b'n') => Some((u32::from(b'\n'), 2)),
            Some(b'r') => Some((u32::from(b'\r'), 2)),
            Some(b't') => Some((u32::from(b'\t'), 2)),
            Some(&escaped @ (b'"' | b'\\' | b'/')) => Some((u32::from(escaped), 2)),
            _ => None,
        };
        if let Some((code, len)) = escaped {
            return Some(DecodedChar { code, len });
        }
    }
    let c = line[i..].chars().next()?;
    Some(DecodedChar {
        code: u32::from(c),
        len: c.len_utf8(),
    })
}

/// `output` without terminal controls. SGR sequences stay, since they only set colors and
/// text styles. Tabs and line ends stay too. Bytes that aren't UTF-8 become U+FFFD.
pub(super) fn without_terminal_controls(output: &[u8]) -> Vec<u8> {
    let output = String::from_utf8_lossy(output);
    let mut kept = String::with_capacity(output.len());
    for line in output.split_inclusive('\n') {
        // rustc's JSON diagnostics, whose `rendered` field cargo prints with its escapes
        // decoded. Elsewhere, such as in human-format diagnostics, a backslash is text.
        let json = line.starts_with('{');
        filter_line(line, json, &mut kept);
    }
    kept.into_bytes()
}

/// Adds `line` to `kept` without its terminal controls. A sequence ends at the line's end at
/// the latest, so a broken one costs one line.
fn filter_line(line: &str, json: bool, kept: &mut String) {
    let mut i = 0;
    while let Some(c) = char_at(line, i, json) {
        let next = char_at(line, i + c.len, json);
        let next_code = next.map(|next| next.code);
        let introducer_end = i
            + c.len
            + if c.code == ESC {
                next.map_or(0, |n| n.len)
            } else {
                0
            };
        i = if c.code == ESC && next_code == Some(u32::from(b'[')) || c.code == CSI_8BIT {
            let (end, is_sgr) = csi_end(line, introducer_end, json);
            // Only the 7-bit form, which terminals agree on.
            if is_sgr && c.code == ESC {
                kept.push_str(&line[i..end]);
            }
            end
        } else if c.code == ESC && next_code.is_some_and(is_string_introducer) || c.code == OSC_8BIT
        {
            string_end(line, introducer_end, json)
        } else if c.code == ESC {
            // A two-character escape, such as one that resets the terminal.
            match next {
                Some(next) if (0x30..=0x7e).contains(&next.code) => i + c.len + next.len,
                _ => i + c.len,
            }
        } else {
            if !is_control(c.code) {
                kept.push_str(&line[i..i + c.len]);
            }
            i + c.len
        };
    }
}

/// The end of the CSI sequence whose parameters start at `line[start..]`, and whether it's an
/// SGR sequence. A sequence cut short ends where its characters stop fitting.
fn csi_end(line: &str, start: usize, json: bool) -> (usize, bool) {
    let mut i = start;
    let mut sgr_params = true;
    while let Some(c) = char_at(line, i, json).filter(|c| (0x30..=0x3f).contains(&c.code)) {
        sgr_params &= (u32::from(b'0')..=u32::from(b';')).contains(&c.code);
        i += c.len;
    }
    let params_end = i;
    while let Some(c) = char_at(line, i, json).filter(|c| (0x20..=0x2f).contains(&c.code)) {
        i += c.len;
    }
    match char_at(line, i, json) {
        Some(c) if (0x40..=0x7e).contains(&c.code) => (
            i + c.len,
            sgr_params && i == params_end && c.code == u32::from(b'm'),
        ),
        _ => (i, false),
    }
}

/// OSC, DCS, SOS, PM and APC, which run until a string terminator.
fn is_string_introducer(code: u32) -> bool {
    b"]PX^_"
        .iter()
        .any(|&introducer| u32::from(introducer) == code)
}

/// The end of the control string whose contents start at `line[start..]`: past its BEL or
/// ST, or at the line end.
fn string_end(line: &str, start: usize, json: bool) -> usize {
    let mut i = start;
    let mut previous = None;
    while let Some(c) = char_at(line, i, json) {
        match c.code {
            code if code == u32::from(b'\n') => return i,
            BEL | ST_8BIT => return i + c.len,
            code if code == u32::from(b'\\') && previous == Some(ESC) => return i + c.len,
            _ => {}
        }
        previous = Some(c.code);
        i += c.len;
    }
    i
}

/// C0 controls but tab and line ends, DEL, and C1 controls.
fn is_control(code: u32) -> bool {
    matches!(code, 0..=0x08 | 0x0b | 0x0c | 0x0e..=0x1f | 0x7f..=0x9f)
}

#[cfg(test)]
mod test {
    use super::*;
    use test_case::test_case;

    #[test_case(b"\x1b[1;31merror\x1b[0m: x" ; "raw colors")]
    #[test_case(b"\x1b[38:5:196mred\x1b[m" ; "raw colors with sub-parameters")]
    #[test_case(
        br#"{"rendered":"\u001b[0m\u001b[1m\u001b[38;5;9merror\u001b[0m\n"}"# ;
        "json colors"
    )]
    #[test_case(b"a\tb\r\nc\n" ; "tabs and line ends")]
    #[test_case("caf\u{e9} \u{20ac} \u{1f980}".as_bytes() ; "utf-8")]
    #[test_case(br#"{"r":"\\u001b]0;not an escape"}"# ; "json escaped backslash")]
    #[test_case(
        br#"{"r":"\u00e9 \"quoted\" \ud83e\udd80"}"# ;
        "json escapes of printable characters"
    )]
    #[test_case(br#"error: r"\bword\b"; \f \u001b"# ; "backslashes in human-format output")]
    fn test_kept(output: &[u8]) {
        assert_eq!(
            String::from_utf8_lossy(output),
            String::from_utf8_lossy(&without_terminal_controls(output))
        );
    }

    #[test_case(b"a\x1b]0;title\x07b", "ab" ; "raw title")]
    #[test_case(b"a\x1b]52;c;Zm9v\x1b\\b", "ab" ; "raw clipboard ended by st")]
    #[test_case(br#"{"r":"a\u001b]52;c;Zm9v\u0007b"}"#, r#"{"r":"ab"}"# ; "json clipboard")]
    #[test_case(
        br#"{"r":"\u001b]8;;file:///x\u001b\\text\u001b]8;;\u001b\\"}"#,
        r#"{"r":"text"}"# ;
        "json link ended by st"
    )]
    #[test_case(br#"{"r":"x\u001b"}"#, r#"{"r":"x"}"# ; "json escape before a quote")]
    #[test_case(b"\x1b[2J\x1b[Hx", "x" ; "raw clear screen")]
    #[test_case(br#"{"r":"\u001b[2Kx"}"#, r#"{"r":"x"}"# ; "json erase line")]
    #[test_case(b"\x1b[31;2Jx", "x" ; "erase with color-like parameters")]
    #[test_case(b"\x1bcx", "x" ; "reset")]
    #[test_case(b"a\x07\x08\x7fb", "ab" ; "bell backspace delete")]
    #[test_case(br#"{"r":"a\bb"}"#, r#"{"r":"ab"}"# ; "json backspace")]
    #[test_case("\u{9b}31mx".as_bytes(), "x" ; "8-bit csi")]
    #[test_case("a\\\u{9b}2Jb".as_bytes(), "a\\b" ; "8-bit csi after a backslash")]
    #[test_case(
        "{\"r\":\"a\\\u{9b}2Jb\"}".as_bytes(),
        "{\"r\":\"a\\b\"}" ;
        "json 8-bit csi after a backslash"
    )]
    #[test_case("a\u{9d}0;title\u{9c}b".as_bytes(), "ab" ; "8-bit osc")]
    #[test_case(b"a\x9b2Jb", "a\u{fffd}2Jb" ; "byte that isn't utf-8")]
    #[test_case(br#"{"r":"\u+01b]0;x"}"#, r#"{"r":"\u+01b]0;x"}"# ; "json escape without hex")]
    #[test_case(b"a\x1b]0;unterminated\nb\n", "a\nb\n" ; "unterminated title ends at the line")]
    #[test_case(b"a\x1b[31", "a" ; "cut short color")]
    #[test_case(b"a\x1bPq#0\x1b\\b", "ab" ; "dcs")]
    fn test_removed(output: &[u8], kept: &str) {
        assert_eq!(
            kept,
            String::from_utf8_lossy(&without_terminal_controls(output))
        );
    }
}
