//! Extra parsing combinators for Nom.
use nom::{
  IResult, InputTake, InputLength, Parser,
  Err,
  error::ParseError,
};

#[cfg(test)]
use nom::{
  error::Error,
  bytes::complete::tag,
  combinator::eof
};

#[cfg(test)]
type TestResult<'a, X> = IResult<&'a str, (&'a str, X), Error<&'a str>>;

/// Consume input until a parser succeeds, and return the skipped input and parse result.
pub fn take_until<I, O, E, G>(mut parser: G) -> impl FnMut(I) -> IResult<I, (I, O), E>
where I: InputTake + InputLength,
      G: Parser<I, O, E>,
      E: ParseError<I>
{
  move |i: I| {
    let mut pos = 0;
    loop {
      let (back, front) = i.take_split(pos);
      match parser.parse(back) {
        Ok((tail, v)) => return Ok((tail, (front, v))),
        Err(Err::Error(x)) => {
          if pos == i.input_len() {
            return Err(Err::Error(x))
          }
          // skip forward!
          pos = pos + 1;
        },
        Err(e) => return Err(e)
      }
    }
  }
}


#[test]
fn test_tu_empty() {
  let text = "";
  let res: TestResult<'_, _> = take_until(eof)(text);
  let (_, (f, _)) = res.expect("parse error");
  assert_eq!(f, "");
}


#[test]
fn test_tu_immediate() {
  let text = "foo";
  let res: TestResult<'_, _> = take_until(tag("foo"))(text);
  let (_, (f, v)) = res.expect("parse error");
  assert_eq!(f, "");
  assert_eq!(v, "foo");
}


#[test]
fn test_tu_skip() {
  let text = "foobie bletch";
  let res: TestResult<'_, _> = take_until(tag("bletch"))(text);
  let (_, (f, v)) = res.expect("parse error");
  assert_eq!(f, "foobie ");
  assert_eq!(v, "bletch");
}

#[test]
fn test_tu_skip_leftovers() {
  let text = "foobie bletch scroll";
  let res: TestResult<'_, _> = take_until(tag("bletch"))(text);
  let (r, (f, v)) = res.expect("parse error");
  assert_eq!(r, " scroll");
  assert_eq!(f, "foobie ");
  assert_eq!(v, "bletch");
}

#[test]
fn test_nothing() {
  let text = "hackem muche";
  let res: TestResult<'_, _> = take_until(tag("bletch"))(text);
  assert!(res.is_err());
}
