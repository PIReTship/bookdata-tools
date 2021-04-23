use std::io;
use sha1::Sha1;

/// Write wrapper that computes Sha1 checksums of the data written.
pub struct HashWrite<'a, W: io::Write> {
  writer: W,
  hash: &'a mut Sha1
}

impl <'a, W: io::Write> HashWrite<'a, W> {
  /// Create a hash writer
  pub fn create(base: W, hash: &'a mut Sha1) -> HashWrite<'a, W> {
    HashWrite {
      writer: base,
      hash: hash
    }
  }
}

impl <'a, W: io::Write> io::Write for HashWrite<'a, W> {
  fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
    self.hash.update(buf);
    self.writer.write(buf)
  }

  fn flush(&mut self) -> io::Result<()> {
    self.writer.flush()
  }
}

/// Read wrapper that computes Sha1 checksums of the data read.
pub struct HashRead<'a, R: io::Read> {
  reader: R,
  hash: &'a mut Sha1
}

impl <'a, R: io::Read> HashRead<'a, R> {
  /// Create a hash reader
  pub fn create(base: R, hash: &'a mut Sha1) -> HashRead<'a, R> {
    HashRead {
      reader: base,
      hash: hash
    }
  }
}

impl <'a, R: io::Read> io::Read for HashRead<'a, R> {
  fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
    let n = self.reader.read(buf)?;
    self.hash.update(&buf[0..n]);
    Ok(n)
  }
}
