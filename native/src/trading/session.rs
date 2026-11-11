//! Explicitly bounded 2026 NYSE calendar. Unknown years fail closed.
use anyhow::{Result, ensure};
use chrono::{Datelike, NaiveDate, Utc, DateTime};
pub fn bounds(now: DateTime<Utc>) -> Result<Option<(i64,i64)>> {
    let d=now.date_naive();
    ensure!(d.year()==2026, "NYSE calendar only verified for 2026; update before running");
    let holidays=["2026-01-01","2026-01-19","2026-02-16","2026-04-03","2026-05-25","2026-06-19","2026-07-03","2026-09-07","2026-11-26","2026-12-25"];
    if d.weekday().number_from_monday()>5 || holidays.contains(&d.to_string().as_str()) {return Ok(None);}
    // Session is far from UTC midnight and DST transitions: March 8 / November 1.
    let dst=d>=NaiveDate::from_ymd_opt(2026,3,8).unwrap() && d<NaiveDate::from_ymd_opt(2026,11,1).unwrap();
    let start=d.and_hms_opt(if dst {13}else{14},30,0).unwrap().and_utc().timestamp_millis();
    Ok(Some((start,start+3_600_000)))
}
pub fn is_open(now: DateTime<Utc>) -> Result<bool> {
    Ok(bounds(now)?.is_some_and(|(a,b)| (a..b).contains(&now.timestamp_millis())))
}
#[cfg(test)] mod tests {
 use super::*;
 fn t(s:&str)->DateTime<Utc>{s.parse().unwrap()}
 #[test] fn dst_and_exact_boundaries(){
   assert!(is_open(t("2026-10-30T13:30:00Z")).unwrap());
   assert!(!is_open(t("2026-11-02T13:30:00Z")).unwrap());
   assert!(is_open(t("2026-11-02T14:30:00Z")).unwrap());
   assert!(!is_open(t("2026-11-02T15:30:00Z")).unwrap());
 }
 #[test] fn holidays_and_calendar_expiry(){
   assert!(!is_open(t("2026-11-26T14:30:00Z")).unwrap());
   assert!(!is_open(t("2026-11-14T14:30:00Z")).unwrap());
   assert!(is_open(t("2026-11-11T14:30:00Z")).unwrap());
   assert!(bounds(t("2027-01-04T14:30:00Z")).is_err());
 }
}
