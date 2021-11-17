use std::io;

pub struct DelimPrinter<'a> {
  delim: &'a [u8],
  end: &'a [u8],
  first: bool
}

impl <'a> DelimPrinter<'a> {
  pub fn new(delim: &'a str, end: &'a str) -> DelimPrinter<'a> {
    DelimPrinter {
      delim: delim.as_bytes(),
      end: end.as_bytes(),
      first: true
    }
  }

  pub fn preface<W: io::Write>(&mut self, w: &mut W) -> io::Result<bool> {
    if self.first {
      self.first = false;
      Ok(false)
    } else {
      w.write_all(self.delim)?;
      Ok(true)
    }
  }

  pub fn end<W: io::Write>(&mut self, w: &mut W) -> io::Result<()> {
    w.write_all(self.end)?;
    self.first = true;
    Ok(())
  }
}
