use chrono::prelude::*;
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
