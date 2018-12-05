use std::io::{self, Write};
use std::str;

/// Write text with PostgreSQL text format encoding.
pub fn write_pgencoded<W: Write>(w: &mut W, buf: &[u8]) -> io::Result<()> {
  let mut start = 0;
  for i in 0..buf.len() {
    match buf[i] {
      b'\\' => {
        w.write_all(&buf[start..i])?;
        start = i + 1;
        w.write_all(b"\\\\")?;
      },
      b'\r' => {
        w.write_all(&buf[start..i])?;
        start = i + 1;
      },
      b'\n' => {
        w.write_all(&buf[start..i])?;
        start = i + 1;
        w.write_all(b"\\n")?;
      },
      b'\t' => {
        w.write_all(&buf[start..i])?;
        start = i + 1;
        w.write_all(b"\\t")?;
      },
      _ => ()
    }
  }
  if start < buf.len() {
    w.write_all(&buf[start..])?;
  }
  Ok(())
}

#[test]
fn it_writes_empty() {
  let mut vec = Vec::new();
  write_pgencoded(&mut vec, b"").unwrap();

  assert_eq!(vec.len(), 0);
}

#[test]
fn it_writes_str() {
  let mut vec = Vec::new();
  write_pgencoded(&mut vec, b"foo").unwrap();

  assert_eq!(str::from_utf8(&vec).unwrap(), "foo");
}

#[test]
fn encode_backslash() {
  let mut vec = Vec::new();
  write_pgencoded(&mut vec, b"\\").unwrap();

  assert_eq!(str::from_utf8(&vec).unwrap(), "\\\\");
}

#[test]
fn encode_tab() {
  let mut vec = Vec::new();
  write_pgencoded(&mut vec, b"\t").unwrap();

  assert_eq!(str::from_utf8(&vec).unwrap(), "\\t");
}

#[test]
fn encode_nl() {
  let mut vec = Vec::new();
  write_pgencoded(&mut vec, b"\n").unwrap();

  assert_eq!(str::from_utf8(&vec).unwrap(), "\\n");
}

#[test]
fn skip_cr() {
  let mut vec = Vec::new();
  write_pgencoded(&mut vec, b"\r").unwrap();

  assert_eq!(str::from_utf8(&vec).unwrap(), "");
}

#[test]
fn embedded() {
  let mut vec = Vec::new();
  write_pgencoded(&mut vec, b"foo\nbar\\wombat").unwrap();

  assert_eq!(str::from_utf8(&vec).unwrap(), "foo\\nbar\\\\wombat");
}
