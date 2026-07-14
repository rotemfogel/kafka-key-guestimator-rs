use std::fs::File;
use std::io::Read;
use std::path::Path;

use anyhow::Result;

/// Read non-blank lines from a JSONL file, replacing invalid UTF-8 bytes.
pub fn read_jsonl(path: &Path) -> Result<Vec<String>> {
    let mut bytes = Vec::new();
    File::open(path)?.read_to_end(&mut bytes)?;
    // from_utf8_lossy replaces invalid sequences with U+FFFD — matches Python's errors="replace"
    let content = String::from_utf8_lossy(&bytes);
    Ok(content
        .lines()
        .filter_map(|l| {
            let s = l.trim();
            if s.is_empty() {
                None
            } else {
                Some(s.to_owned())
            }
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn write_temp(name: &str, content: &[u8]) -> PathBuf {
        let path = std::env::temp_dir().join(name);
        std::fs::write(&path, content).unwrap();
        path
    }

    #[test]
    fn reads_all_nonempty_lines() {
        let path = write_temp("kkgr_reader_basic.jsonl", b"{\"a\":1}\n{\"b\":2}\n");
        let lines = read_jsonl(&path).unwrap();
        assert_eq!(lines, vec![r#"{"a":1}"#, r#"{"b":2}"#]);
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn filters_blank_and_whitespace_only_lines() {
        let path = write_temp("kkgr_reader_blank.jsonl", b"{\"a\":1}\n\n   \n{\"b\":2}\n");
        let lines = read_jsonl(&path).unwrap();
        assert_eq!(lines.len(), 2);
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn trims_whitespace_from_lines() {
        let path = write_temp("kkgr_reader_trim.jsonl", b"  {\"a\":1}  \n");
        let lines = read_jsonl(&path).unwrap();
        assert_eq!(lines[0], r#"{"a":1}"#);
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn file_not_found_returns_err() {
        let path = std::path::Path::new("/tmp/kkgr_does_not_exist_xyzzy.jsonl");
        assert!(read_jsonl(path).is_err());
    }

    #[test]
    fn handles_invalid_utf8_with_replacement() {
        // \xFF\xFE is not valid UTF-8; from_utf8_lossy replaces with U+FFFD (non-blank)
        let path = write_temp(
            "kkgr_reader_utf8.jsonl",
            b"valid\n\xFF\xFE\n{\"ok\":true}\n",
        );
        let lines = read_jsonl(&path).unwrap();
        assert_eq!(lines.len(), 3);
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn empty_file_returns_empty_vec() {
        let path = write_temp("kkgr_reader_empty.jsonl", b"");
        let lines = read_jsonl(&path).unwrap();
        assert!(lines.is_empty());
        std::fs::remove_file(path).ok();
    }
}
