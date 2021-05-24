//! Extra parsing combinators for Nom.
use nom::{
  IResult, InputTake, InputLength, Parser,
  Err,
  error::ParseError,
};

#[cfg(test)]
use nom::{
  error::Error,
  number::complete::float,
  bytes::complete::tag,
  combinator::eof
};

#[cfg(test)]
type TestResult<'a, X, S=&'a str> = IResult<&'a str, (S, X), Error<&'a str>>;

/// Consume input until a parser succeeds, and return the skipped input and parse result.
pub fn take_until_parse<I, O, E, G>(mut parser: G) -> impl FnMut(I) -> IResult<I, (I, O), E>
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

/// Match a pair of parsers, matching the second *first* and the first on the string skipped
/// to reach it.
pub fn pair_nongreedy<I, O1, O2, E, F, G>(mut first: F, mut second: G) -> impl FnMut(I) -> IResult<I, (O1, O2), E>
where I: InputTake + InputLength + Clone,
      F: Parser<I, O1, E>,
      G: Parser<I, O2, E>,
      E: ParseError<I>
{
  move |i: I| {
    let (rest, (skipped, o2)) = take_until_parse(|ii| second.parse(ii))(i)?;
    let (_s, o1) = first.parse(skipped)?;
    Ok((rest, (o1, o2)))
  }
}

#[test]
fn test_tu_empty() {
  let text = "";
  let res: TestResult<'_, _> = take_until_parse(eof)(text);
  let (_, (f, _)) = res.expect("parse error");
  assert_eq!(f, "");
}


#[test]
fn test_tu_immediate() {
  let text = "foo";
  let res: TestResult<'_, _> = take_until_parse(tag("foo"))(text);
  let (_, (f, v)) = res.expect("parse error");
  assert_eq!(f, "");
  assert_eq!(v, "foo");
}


#[test]
fn test_tu_skip() {
  let text = "foobie bletch";
  let res: TestResult<'_, _> = take_until_parse(tag("bletch"))(text);
  let (_, (f, v)) = res.expect("parse error");
  assert_eq!(f, "foobie ");
  assert_eq!(v, "bletch");
}

#[test]
fn test_tu_skip_leftovers() {
  let text = "foobie bletch scroll";
  let res: TestResult<'_, _> = take_until_parse(tag("bletch"))(text);
  let (r, (f, v)) = res.expect("parse error");
  assert_eq!(r, " scroll");
  assert_eq!(f, "foobie ");
  assert_eq!(v, "bletch");
}

#[test]
fn test_tu_nothing() {
  let text = "hackem muche";
  let res: TestResult<'_, _> = take_until_parse(tag("bletch"))(text);
  assert!(res.is_err());
}

#[test]
fn test_nongreedy_value() {
  let text ="47bob";
  let res: TestResult<'_, _, f32> = pair_nongreedy(float, tag("bob"))(text);
  let (_r, (n, tag)) = res.expect("parse error");
  assert_eq!(n, 47.0);
  assert_eq!(tag, "bob");
}

#[test]
fn test_nongreedy_early() {
  let text ="479";
  let res: TestResult<'_, _, f32> = pair_nongreedy(float, tag("9"))(text);
  let (_r, (n, tag)) = res.expect("parse error");
  assert_eq!(n, 47.0);
  assert_eq!(tag, "9");
}
