use crate::model::*;
use anyhow::Result;
use std::collections::{BTreeMap, HashSet, VecDeque};
#[derive(Default)]
struct State {
    book: Option<Event>,
    values: BTreeMap<String, Event>,
    connected: bool,
    connected_since: i64,
    trades: VecDeque<Event>,
    liquidations: VecDeque<Event>,
    gap: bool,
}
pub struct Analysis {
    pub disabled: bool,
    states: BTreeMap<String, (Instrument, State)>,
    next: i64,
    stale: i64,
    seen: HashSet<String>,
    seen_order: VecDeque<String>,
}
impl Analysis {
    pub fn new(instruments: &[Instrument], start: i64, stale: u64) -> Self {
        Self {
            disabled: false,
            states: instruments
                .iter()
                .map(|i| (i.key(), (i.clone(), State::default())))
                .collect(),
            next: (start / SECOND + 1) * SECOND,
            stale: stale as i64 * SECOND,
            seen: HashSet::new(),
            seen_order: VecDeque::new(),
        }
    }
    pub fn ingest(&mut self, e: &Event) -> bool {
        if self.disabled { return true; }
        let dedupe = matches!(e.dataset.as_str(), "public_trade" | "liquidation_event")
            || (e.dataset == "funding_event" && e.string("rate_kind") == "settled");
        if dedupe {
            if !self.seen.insert(e.string("event_id").into()) {
                return false;
            }
            self.seen_order.push_back(e.string("event_id").into());
            if self.seen_order.len() > 200_000
                && let Some(s) = self.seen_order.pop_front()
            {
                self.seen.remove(&s);
            }
        }
        let Some((_, s)) = self.states.get_mut(e.string("instrument_id")) else {
            return true;
        };
        let received = e.integer("received_at_ns").unwrap_or(0);
        if received < self.next - SECOND && e.dataset != "funding_event" {
            s.gap = true;
        }
        match e.dataset.as_str() {
            "collection_status" => {
                if e.string("state") == "connected" {
                    s.connected = true;
                    s.connected_since = received;
                } else if ["disconnected", "stopped"].contains(&e.string("state")) {
                    s.connected = false;
                    s.book = None;
                    s.gap = true;
                }
            }
            "book_state_20" => s.book = Some(e.clone()),
            "price_event" => {
                s.values.insert(e.string("price_type").into(), e.clone());
            }
            "oi_event" => {
                s.values.insert("oi".into(), e.clone());
            }
            "funding_event" if e.string("rate_kind") == "predicted" => {
                s.values.insert("funding".into(), e.clone());
            }
            "public_trade" if !e.boolean("is_snapshot") => s.trades.push_back(e.clone()),
            "liquidation_event" => s.liquidations.push_back(e.clone()),
            _ => {}
        }
        true
    }
    // Close every elapsed boundary before ingesting a newer message. No future state is used.
    pub fn advance(&mut self, watermark: i64) -> Result<Vec<Event>> {
        let mut out = Vec::new();
        if self.disabled { return Ok(out); }
        while self.next <= watermark {
            let t = self.next;
            for (i, s) in self.states.values_mut() {
                let mut e = Event::new("analysis_state_1s", i, "analysis", "", Some(t), t);
                e.s("event_id", id(&format!("{}:analysis:v2:{t}", i.key())));
                e.fields.remove("raw_id");
                e.n("as_of_ns", t);
                let age =
                    |event: &Event| (t - event.integer("received_at_ns").unwrap_or(t)) as f64 / 1e6;
                if let Some(b) = &s.book {
                    let received = b.integer("received_at_ns").unwrap_or(t);
                    if received < t {
                        e.fields.insert("book_age_ms".into(), Cell::Float(age(b)));
                        if b.boolean("book_valid") && s.connected && t - received <= self.stale {
                            e.s("book_state_id", b.string("book_state_id"));
                            if let (Some(Cell::Levels(bids)), Some(Cell::Levels(asks))) =
                                (b.fields.get("bids"), b.fields.get("asks"))
                                && let Some((bid, ask)) = bids.first().zip(asks.first())
                            {
                                let mid = bid.price.add(ask.price)?.div(Decimal(2 * SCALE))?;
                                e.d("mid_price", Some(mid));
                                e.d(
                                    "spread_bps",
                                    Some(
                                        ask.price
                                            .sub(bid.price)?
                                            .div(mid)?
                                            .mul(Decimal(10000 * SCALE))?,
                                    ),
                                );
                            }
                        } else {
                            e.flag("book_unavailable_or_stale");
                        }
                    }
                }
                for (key, field, value) in [
                    ("mark", "mark_price", "price"),
                    ("index", "index_price", "price"),
                    ("oracle", "oracle_price", "price"),
                    ("last", "last_price", "price"),
                    ("oi", "oi_base_qty", "oi_base_qty"),
                    ("funding", "funding_rate", "rate_decimal"),
                ] {
                    if let Some(v) = s.values.get(key)
                        && v.integer("received_at_ns").unwrap_or(t) < t
                    {
                        e.d(field, v.decimal(value));
                        e.fields
                            .insert(format!("{key}_age_ms"), Cell::Float(age(v)));
                        if key == "funding" {
                            e.d("funding_interval_hours", v.decimal("interval_hours"));
                            if let Some(n) = v.integer("settlement_at_ns") {
                                e.n("next_funding_at_ns", n);
                            }
                        }
                    }
                }
                let mut totals = [Decimal(0); 4];
                for trade in &s.trades {
                    let n = trade.integer("received_at_ns").unwrap_or(0);
                    if n >= t - SECOND && n < t {
                        let q = trade.decimal("base_qty").unwrap_or_default();
                        totals[0] = totals[0].add(q)?;
                        totals[1] =
                            totals[1].add(q.mul(trade.decimal("price").unwrap_or_default())?)?;
                        let idx = if trade.string("aggressor_side") == "buy" {
                            2
                        } else {
                            3
                        };
                        totals[idx] = totals[idx].add(q)?;
                    }
                }
                let complete = s.connected && s.connected_since <= t - SECOND && !s.gap;
                if complete {
                    for (k, v) in [
                        "trade_volume_1s",
                        "trade_notional_1s",
                        "taker_buy_volume_1s",
                        "taker_sell_volume_1s",
                    ]
                    .iter()
                    .zip(totals)
                    {
                        e.d(k, Some(v));
                    }
                } else {
                    e.flag("incomplete_feed_window");
                }
                let available = coverage(&i.exchange) != "unavailable_public_api";
                if available && complete {
                    let mut total = Decimal(0);
                    for l in &s.liquidations {
                        if l.integer("received_at_ns")
                            .is_some_and(|n| n >= t - SECOND && n < t)
                        {
                            total = total.add(
                                l.decimal("filled_qty")
                                    .or_else(|| l.decimal("order_qty"))
                                    .unwrap_or_default(),
                            )?;
                        }
                    }
                    e.d("reported_liquidation_volume_1s", Some(total));
                }
                e.s("liquidation_coverage", coverage(&i.exchange));
                e.b("trade_feed_connected", s.connected);
                e.b("liquidation_feed_connected", available && s.connected);
                while s
                    .trades
                    .front()
                    .is_some_and(|v| v.integer("received_at_ns").unwrap_or(0) < t)
                {
                    s.trades.pop_front();
                }
                while s
                    .liquidations
                    .front()
                    .is_some_and(|v| v.integer("received_at_ns").unwrap_or(0) < t)
                {
                    s.liquidations.pop_front();
                }
                s.gap = false;
                out.push(e);
            }
            self.next += SECOND;
        }
        Ok(out)
    }
}
