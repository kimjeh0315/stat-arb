use crate::model::*;
use anyhow::{Result, ensure};
use serde_json::Value;
use std::collections::BTreeMap;
#[derive(Default)]
pub struct Book {
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
    pub sequence: Option<i64>,
    pub initialized: bool,
    pub epoch: String,
    pub pending: bool,
    pub remaining: usize,
    pub snapshot_sequence: Option<i64>,
}
impl Book {
    pub fn apply(&mut self, b: &Value, a: &Value) -> Result<()> {
        for (rows, side) in [(b, &mut self.bids), (a, &mut self.asks)] {
            for row in rows
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("book side is not an array"))?
            {
                let (p, q) = if row.is_array() {
                    (&row[0], &row[1])
                } else if row.get("px").is_some() {
                    (&row["px"], &row["sz"])
                } else {
                    (&row["price"], &row["size"])
                };
                let p = required(dec(p)?)?;
                let q = required(dec(q)?)?;
                ensure!(p.0 > 0 && q.0 >= 0, "invalid book level");
                if q.0 == 0 {
                    side.remove(&p);
                } else {
                    side.insert(p, q);
                }
            }
        }
        Ok(())
    }
    pub fn snapshot(&mut self, b: &Value, a: &Value, seq: Option<i64>) -> Result<()> {
        self.bids.clear();
        self.asks.clear();
        self.apply(b, a)?;
        self.sequence = seq;
        self.initialized = true;
        self.epoch = uuid::Uuid::new_v4().simple().to_string();
        Ok(())
    }
    pub fn delta(
        &mut self,
        b: &Value,
        a: &Value,
        seq: Option<i64>,
        prev: Option<i64>,
    ) -> Result<bool> {
        ensure!(self.initialized, "delta before snapshot");
        if seq.zip(self.sequence).is_some_and(|(s, last)| s <= last) {
            return Ok(false);
        }
        ensure!(
            prev.is_none() || prev == self.sequence,
            "book sequence gap: {:?} != {:?}",
            prev,
            self.sequence
        );
        self.apply(b, a)?;
        self.sequence = seq;
        Ok(true)
    }
    pub fn export(
        &self,
        i: &Instrument,
        session: &str,
        raw: &str,
        time: Option<i64>,
        received: i64,
    ) -> Result<Event> {
        let mut e = Event::new("book_state_20", i, session, raw, time, received);
        let bids: Vec<_> = self
            .bids
            .iter()
            .rev()
            .take(20)
            .map(|(p, q)| {
                Ok(Level {
                    price: p.div(i.multiplier)?,
                    qty: q.mul(i.multiplier)?,
                })
            })
            .collect::<Result<_>>()?;
        let asks: Vec<_> = self
            .asks
            .iter()
            .take(20)
            .map(|(p, q)| {
                Ok(Level {
                    price: p.div(i.multiplier)?,
                    qty: q.mul(i.multiplier)?,
                })
            })
            .collect::<Result<_>>()?;
        e.b(
            "book_valid",
            !self.pending
                && self.remaining == 0
                && bids
                    .first()
                    .zip(asks.first())
                    .is_some_and(|(b, a)| b.price < a.price),
        );
        e.n("bid_level_count", bids.len() as i64);
        e.n("ask_level_count", asks.len() as i64);
        e.fields.insert("bids".into(), Cell::Levels(bids));
        e.fields.insert("asks".into(), Cell::Levels(asks));
        e.s(
            "event_id",
            id(&format!("{raw}:book:{}:{:?}", i.key(), self.sequence)),
        );
        e.s("book_epoch_id", &self.epoch);
        e.s("book_state_id", e.string("event_id").to_string());
        if let Some(s) = self.sequence {
            e.s("source_sequence", s.to_string());
        }
        Ok(e)
    }
}
