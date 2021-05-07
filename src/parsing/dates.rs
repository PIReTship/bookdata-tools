use chrono::prelude::*;
use chrono::ParseResult;

/// The date format used in the GoodReads data.
///
/// This almost RFC2822, but hte year is in the wrong place.
const GR_DATE_FMT: &'static str = "%A %B %d %H:%M:%S %z %Y";

/// Construct a date if all components are defined and specify a valid day.
pub fn maybe_date<Y: Into<i32>, M: Into<u32>, D: Into<u32>>(year: Option<Y>, month: Option<M>, day: Option<D>) -> Option<NaiveDate> {
  match (year, month, day) {
    (Some(y), Some(m), Some(d)) => {
      let year: i32 = y.into();
      if year.abs() < 10000 {  // pandas doesn't like some years
        NaiveDate::from_ymd_opt(year, m.into(), d.into())
      } else {
        None
      }
    },
    _ => None
  }
}


/// Parse a GoodReads date.
pub fn parse_gr_date(s: &str) -> ParseResult<DateTime<FixedOffset>> {
  DateTime::parse_from_str(s, GR_DATE_FMT)
}
