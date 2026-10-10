use crate::{book::Book, model::*};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Duration,
};
use tokio::sync::Mutex;
#[derive(Clone)]
pub struct Http {
    client: reqwest::Client,
    next: Arc<Mutex<tokio::time::Instant>>,
    delay: Duration,
    pub seed_groups: Arc<std::sync::atomic::AtomicUsize>,
}
impl Http {
    pub fn new(rate: f64) -> Result<Self> {
        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()?,
            next: Arc::new(Mutex::new(tokio::time::Instant::now())),
            delay: Duration::from_secs_f64(1. / rate),
            seed_groups: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        })
    }
    pub async fn call(&self, url: &str, body: Option<Value>) -> Result<(String, Value)> {
        for attempt in 0..5u32 {
            let mut next = self.next.lock().await;
            tokio::time::sleep_until(*next).await;
            *next = tokio::time::Instant::now() + self.delay;
            drop(next);
            let response = match body.as_ref() {
                Some(b) => self.client.post(url).json(b),
                None => self.client.get(url),
            }
            .send()
            .await?;
            if response.status().as_u16() == 429 || response.status().is_server_error() {
                tokio::time::sleep(Duration::from_secs(1 << attempt)).await;
                continue;
            }
            let raw = response.error_for_status()?.text().await?;
            let v: Value = serde_json::from_str(&raw)?;
            ensure!(
                int(&v["retCode"]).unwrap_or(0) == 0,
                "API retCode: {}",
                v["retCode"]
            );
            ensure!(int(&v["code"]).unwrap_or(0) >= 0, "API code: {}", v["code"]);
            return Ok((raw, v));
        }
        anyhow::bail!("REST retries exhausted: {url}")
    }
}
pub fn ws_url(ex: &str) -> &'static str {
    match ex {
        "bybit" => "wss://stream.bybit.com/v5/public/linear",
        "aster" => "wss://fstream.asterdex.com/ws",
        "grvt" => "wss://market-data.grvt.io/ws/full",
        _ => "wss://api.hyperliquid.xyz/ws",
    }
}
pub fn ping(ex: &str) -> Value {
    match ex {
        "bybit" => json!({"op":"ping"}),
        "hyperliquid" => json!({"method":"ping"}),
        _ => Value::Null,
    }
}
pub fn subscriptions(ex: &str, items: &[Instrument]) -> Vec<Value> {
    match ex{
        "bybit"=>vec![json!({"op":"subscribe","args":items.iter().flat_map(|i|["orderbook.50","publicTrade","tickers","allLiquidation"].map(|c|format!("{c}.{}",i.native_symbol))).collect::<Vec<_>>()})],
        "aster"=>vec![json!({"method":"SUBSCRIBE","id":1,"params":items.iter().flat_map(|i|["depth@100ms","aggTrade","markPrice@1s","forceOrder"].map(|c|format!("{}@{c}",i.native_symbol.to_lowercase()))).collect::<Vec<_>>()})],
        "grvt"=>["v1.book.d","v1.ticker.s","v1.trade"].iter().enumerate().map(|(n,c)|json!({"jsonrpc":"2.0","method":"subscribe","id":n+1,"params":{"stream":c,"selectors":items.iter().map(|i|format!("{}@{}",i.native_symbol,if n==1{500}else{50})).collect::<Vec<_>>()}})).collect(),
        _=>items.iter().flat_map(|i|["l2Book","trades","activeAssetCtx"].map(|c|{let mut sub=json!({"type":c,"coin":i.native_symbol});if c=="l2Book"{sub["fast"]=json!(false);}json!({"method":"subscribe","subscription":sub})})).collect()
    }
}
pub async fn discover(ex: &str, c: &Config, u: &Universe, h: &Http) -> Result<Vec<Instrument>> {
    let mut out = Vec::new();
    let mut add = |symbol: &str,
                   base: &str,
                   quote: &str,
                   settle: &str,
                   tick: &Value,
                   step: &Value,
                   min: &Value,
                   fund: &Value|
     -> Result<()> {
        if !c.quote_assets.iter().any(|q| q == quote) {
            return Ok(());
        }
        if let Some(mut i) = u.instrument(ex, symbol, base, quote, settle)? {
            i.tick_size = dec(tick)?;
            i.qty_step = dec(step)?;
            i.min_qty = dec(min)?;
            i.funding_interval_hours = dec(fund)?;
            out.push(i);
        }
        Ok(())
    };
    match ex {
        "bybit" => {
            let mut cursor = String::new();
            loop {
                let (_,v)=h.call(&format!("https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000&cursor={cursor}"),None).await?;
                for r in v["result"]["list"]
                    .as_array()
                    .context("Bybit instruments missing")?
                {
                    if r["status"] != "Trading" || r["contractType"] != "LinearPerpetual" {
                        continue;
                    }
                    let funding = dec(&r["fundingInterval"])?
                        .map(|d| d.div(Decimal(60 * SCALE)))
                        .transpose()?
                        .map(|d| json!(d.to_string()))
                        .unwrap_or(Value::Null);
                    add(
                        &text(&r["symbol"]),
                        &text(&r["baseCoin"]),
                        &text(&r["quoteCoin"]),
                        &text(&r["settleCoin"]),
                        &r["priceFilter"]["tickSize"],
                        &r["lotSizeFilter"]["qtyStep"],
                        &r["lotSizeFilter"]["minOrderQty"],
                        &funding,
                    )?;
                }
                cursor = v["result"]["nextPageCursor"].as_str().unwrap_or("").into();
                if cursor.is_empty() {
                    break;
                }
            }
        }
        "aster" => {
            let (_, v) = h
                .call("https://fapi.asterdex.com/fapi/v1/exchangeInfo", None)
                .await?;
            let (_, f) = h
                .call("https://fapi.asterdex.com/fapi/v1/fundingInfo", None)
                .await?;
            for r in v["symbols"].as_array().context("Aster symbols missing")? {
                if r["status"] != "TRADING" || r["contractType"] != "PERPETUAL" {
                    continue;
                }
                let filter = |name: &str| {
                    r["filters"]
                        .as_array()
                        .and_then(|a| a.iter().find(|f| f["filterType"] == name))
                        .cloned()
                        .unwrap_or(Value::Null)
                };
                let p = filter("PRICE_FILTER");
                let lot = filter("LOT_SIZE");
                let interval = f
                    .as_array()
                    .and_then(|a| a.iter().find(|x| x["symbol"] == r["symbol"]))
                    .map(|x| x["fundingIntervalHours"].clone())
                    .unwrap_or(Value::Null);
                add(
                    &text(&r["symbol"]),
                    &text(&r["baseAsset"]),
                    &text(&r["quoteAsset"]),
                    &text(&r["marginAsset"]),
                    &p["tickSize"],
                    &lot["stepSize"],
                    &lot["minQty"],
                    &interval,
                )?;
            }
        }
        "grvt" => {
            let (_, v) = h
                .call(
                    "https://market-data.grvt.io/full/v1/all_instruments",
                    Some(json!({"is_active":true,"kinds":["PERPETUAL"]})),
                )
                .await?;
            for r in v["result"].as_array().context("GRVT instruments missing")? {
                if !r["venues"]
                    .as_array()
                    .is_some_and(|a| a.iter().any(|v| v == "ORDERBOOK"))
                    || r.get("asset_class").is_some_and(|x| x != "CRYPTO")
                {
                    continue;
                }
                let step = json!(format!("1e-{}", int(&r["base_decimals"]).unwrap_or(0)));
                add(
                    &text(&r["instrument"]),
                    &text(&r["base"]),
                    &text(&r["quote"]),
                    &text(&r["quote"]),
                    &r["tick_size"],
                    &step,
                    &r["min_size"],
                    &r["funding_interval_hours"],
                )?;
            }
        }
        "hyperliquid" => {
            let (_, v) = h
                .call(
                    "https://api.hyperliquid.xyz/info",
                    Some(json!({"type":"meta"})),
                )
                .await?;
            for r in v["universe"]
                .as_array()
                .context("Hyperliquid universe missing")?
            {
                if r["isDelisted"] == true {
                    continue;
                }
                add(
                    &text(&r["name"]),
                    &text(&r["name"]),
                    "USDC",
                    "USDC",
                    &Value::Null,
                    &json!(format!("1e-{}", int(&r["szDecimals"]).unwrap_or(0))),
                    &Value::Null,
                    &json!("1"),
                )?;
            }
        }
        _ => anyhow::bail!("unknown exchange"),
    }
    out.sort_by_key(Instrument::key);
    Ok(out)
}
pub struct Parser {
    pub items: HashMap<String, Instrument>,
    pub books: HashMap<String, Book>,
    pub session: String,
    pub start: i64,
    pub grvt_divisor: Decimal,
}
impl Parser {
    pub fn new(items: &[Instrument], c: &Config) -> Self {
        Self {
            items: items
                .iter()
                .map(|i| (i.native_symbol.clone(), i.clone()))
                .collect(),
            books: items
                .iter()
                .map(|i| (i.native_symbol.clone(), Book::default()))
                .collect(),
            session: uuid::Uuid::new_v4().simple().to_string(),
            start: now(),
            grvt_divisor: Decimal(if c.grvt_live_funding_unit == "percentage_points" {
                100 * SCALE
            } else {
                1_000_000 * SCALE
            }),
        }
    }
    pub fn raw(
        &self,
        ex: &str,
        source: &str,
        symbol: Option<String>,
        payload: String,
        received: i64,
    ) -> Raw {
        Raw {
            exchange: ex.into(),
            session_id: self.session.clone(),
            raw_id: uuid::Uuid::new_v4().simple().to_string(),
            received_at_ns: received,
            received_mono_ns: mono(),
            source: source.into(),
            symbol,
            payload,
        }
    }
    pub fn status(&self, items: &[Instrument], state: &str, detail: &str, t: i64) -> Envelope {
        Envelope {
            raw: None,
            events: items
                .iter()
                .map(|i| {
                    let mut e = Event::new(
                        "collection_status",
                        i,
                        &self.session,
                        &uuid::Uuid::new_v4().to_string(),
                        Some(t),
                        t,
                    );
                    e.s("state", state);
                    e.s("detail", detail);
                    e
                })
                .collect(),
        }
    }
    pub fn metadata(&self, items: &[Instrument]) -> Envelope {
        let t = now();
        Envelope {
            raw: None,
            events: items
                .iter()
                .map(|i| {
                    let mut e = Event::new(
                        "instrument_metadata",
                        i,
                        &self.session,
                        &uuid::Uuid::new_v4().to_string(),
                        Some(t),
                        t,
                    );
                    for (k, v) in [
                        ("native_symbol", &i.native_symbol),
                        ("native_base", &i.native_base),
                        ("settle_asset", &i.settle_asset),
                    ] {
                        e.s(k, v);
                    }
                    for (k, v) in [
                        ("multiplier", Some(i.multiplier)),
                        ("tick_size", i.tick_size),
                        ("qty_step", i.qty_step),
                        ("min_qty", i.min_qty),
                        ("funding_interval_hours", i.funding_interval_hours),
                    ] {
                        e.d(k, v);
                    }
                    e
                })
                .collect(),
        }
    }
    fn event(&self, d: &str, i: &Instrument, r: &Raw, t: Option<i64>) -> Event {
        Event::new(d, i, &self.session, &r.raw_id, t, r.received_at_ns)
    }
    fn prices(
        &self,
        out: &mut Vec<Event>,
        i: &Instrument,
        r: &Raw,
        t: Option<i64>,
        values: &[(&str, &Value)],
    ) -> Result<()> {
        for (kind, v) in values {
            if let Some(p) = i.price(v)? {
                let mut e = self.event("price_event", i, r, t);
                e.s("price_type", *kind);
                e.s(
                    "event_id",
                    id(&format!("{}:price:{}:{kind}:{t:?}:{p}", r.raw_id, i.key())),
                );
                e.d("price", Some(p));
                out.push(e);
            }
        }
        Ok(())
    }
    fn oi(
        &self,
        out: &mut Vec<Event>,
        i: &Instrument,
        r: &Raw,
        t: Option<i64>,
        v: &Value,
    ) -> Result<()> {
        if let Some(d) = dec(v)? {
            let mut e = self.event("oi_event", i, r, t);
            e.d("native_value", Some(d));
            e.s("native_unit", "native_base_qty");
            e.d("oi_base_qty", i.qty(v)?);
            out.push(e);
        }
        Ok(())
    }
    #[allow(clippy::too_many_arguments)] // Explicit mapping from the exchange's funding fields.
    fn funding(
        &self,
        out: &mut Vec<Event>,
        i: &Instrument,
        r: &Raw,
        t: Option<i64>,
        v: &Value,
        next: Option<i64>,
        settled: bool,
        divisor: Decimal,
        interval: Option<Decimal>,
    ) -> Result<()> {
        if let Some(d) = dec(v)? {
            let mut e = self.event("funding_event", i, r, t);
            e.d("native_rate", Some(d));
            e.d("rate_decimal", Some(d.div(divisor)?));
            e.s(
                "native_rate_unit",
                if divisor == Decimal(SCALE) {
                    "decimal"
                } else if divisor == Decimal(100 * SCALE) {
                    "percentage_points"
                } else {
                    "centibeeps"
                },
            );
            e.s("rate_kind", if settled { "settled" } else { "predicted" });
            e.d("interval_hours", interval.or(i.funding_interval_hours));
            if let Some(n) = next {
                e.n("settlement_at_ns", n);
            }
            if settled {
                e.s(
                    "event_id",
                    id(&format!("{}:funding:settled:{next:?}", i.key())),
                );
            }
            out.push(e);
        }
        Ok(())
    }
    #[allow(clippy::too_many_arguments)] // Explicit mapping from the exchange's trade fields.
    fn trade(
        &self,
        out: &mut Vec<Event>,
        i: &Instrument,
        r: &Raw,
        t: Option<i64>,
        tid: &Value,
        p: &Value,
        q: &Value,
        side: &str,
        aggregated: bool,
        snapshot: bool,
    ) -> Result<()> {
        let mut e = self.event("public_trade", i, r, t);
        e.s("native_trade_id", text(tid));
        e.s("event_id", id(&format!("{}:trade:{}", i.key(), text(tid))));
        e.d("price", Some(required(i.price(p)?)?));
        e.d("base_qty", Some(required(i.qty(q)?)?));
        e.s("aggressor_side", side);
        e.b("is_aggregated", aggregated);
        e.b("is_snapshot", snapshot || t.is_some_and(|t| t < self.start));
        out.push(e);
        Ok(())
    }
    #[allow(clippy::too_many_arguments)] // Explicit mapping from the exchange's liquidation fields.
    fn liquidation(
        &self,
        out: &mut Vec<Event>,
        i: &Instrument,
        r: &Raw,
        t: Option<i64>,
        v: &Value,
        side: &str,
        position: &str,
        q: &Value,
        filled: &Value,
        p: &Value,
        kind: &str,
    ) -> Result<()> {
        let mut e = self.event("liquidation_event", i, r, t);
        e.s("event_id", id(&format!("{}:liquidation:{v}", i.key())));
        e.s("order_side", side);
        e.s("liquidated_position_side", position);
        e.d("order_qty", i.qty(q)?);
        e.d("filled_qty", i.qty(filled)?);
        e.d("reported_price", i.price(p)?);
        e.s("price_kind", kind);
        e.s("coverage_type", coverage(&i.exchange));
        out.push(e);
        Ok(())
    }
    pub fn seed(&mut self, symbol: &str, v: &Value) -> Result<()> {
        let b = self
            .books
            .get_mut(symbol)
            .context("snapshot unknown symbol")?;
        b.snapshot(&v["bids"], &v["asks"], int(&v["lastUpdateId"]))?;
        b.pending = true;
        Ok(())
    }
    pub fn parse(&mut self, r: &Raw, v: &Value) -> Result<Vec<Event>> {
        let mut out = Vec::new();
        match r.exchange.as_str() {
            "bybit" => {
                ensure!(v["success"] != false, "Bybit rejected subscription: {v}");
                let topic = v["topic"].as_str().unwrap_or("");
                let symbol = topic.rsplit('.').next().unwrap_or("");
                let Some(i) = self.items.get(symbol).cloned() else {
                    return Ok(out);
                };
                let d = &v["data"];
                let t = int(&v["ts"]).map(|n| n * 1_000_000);
                if topic.starts_with("orderbook.") {
                    let b = self.books.get_mut(symbol).unwrap();
                    let seq = int(&d["u"]);
                    if v["type"] == "snapshot" || seq == Some(1) {
                        b.snapshot(&d["b"], &d["a"], seq)?;
                    } else if !b.delta(&d["b"], &d["a"], seq, None)? {
                        return Ok(out);
                    }
                    out.push(
                        b.export(
                            &i,
                            &self.session,
                            &r.raw_id,
                            int(&d["cts"])
                                .or_else(|| int(&v["cts"]))
                                .map(|n| n * 1_000_000)
                                .or(t),
                            r.received_at_ns,
                        )?,
                    );
                } else if topic.starts_with("publicTrade.") {
                    for d in d.as_array().context("Bybit trade array")? {
                        self.trade(
                            &mut out,
                            &i,
                            r,
                            int(&d["T"]).map(|n| n * 1_000_000),
                            &d["i"],
                            &d["p"],
                            &d["v"],
                            &text(&d["S"]).to_lowercase(),
                            false,
                            false,
                        )?;
                    }
                } else if topic.starts_with("tickers.") {
                    self.prices(
                        &mut out,
                        &i,
                        r,
                        t,
                        &[
                            ("mark", &d["markPrice"]),
                            ("index", &d["indexPrice"]),
                            ("last", &d["lastPrice"]),
                        ],
                    )?;
                    self.oi(&mut out, &i, r, t, &d["openInterest"])?;
                    self.funding(
                        &mut out,
                        &i,
                        r,
                        t,
                        &d["fundingRate"],
                        int(&d["nextFundingTime"]).map(|n| n * 1_000_000),
                        false,
                        Decimal(SCALE),
                        None,
                    )?;
                } else if topic.starts_with("allLiquidation.") {
                    for l in d.as_array().context("Bybit liquidation array")? {
                        let long = l["S"] == "Buy";
                        self.liquidation(
                            &mut out,
                            &i,
                            r,
                            int(&l["T"]).map(|n| n * 1_000_000),
                            l,
                            if long { "sell" } else { "buy" },
                            if long { "long" } else { "short" },
                            &l["v"],
                            &Value::Null,
                            &l["p"],
                            "bankruptcy",
                        )?;
                    }
                }
            }
            "aster" => {
                ensure!(v.get("code").is_none(), "Aster subscription error: {v}");
                let d = v.get("data").unwrap_or(v);
                let symbol = d["s"]
                    .as_str()
                    .or_else(|| d["o"]["s"].as_str())
                    .unwrap_or("");
                let Some(i) = self.items.get(symbol).cloned() else {
                    return Ok(out);
                };
                let t = int(&d["T"]).or_else(|| int(&d["E"])).map(|n| n * 1_000_000);
                match d["e"].as_str().unwrap_or("") {
                    "depthUpdate" => {
                        let b = self.books.get_mut(symbol).unwrap();
                        let seq = int(&d["u"]);
                        if seq.zip(b.sequence).is_some_and(|(s, last)| s < last) {
                            return Ok(out);
                        }
                        if b.pending {
                            let last = b.sequence.context("Aster missing snapshot sequence")?;
                            ensure!(
                                int(&d["U"]).is_some_and(|n| n <= last)
                                    && seq.is_some_and(|n| last <= n),
                                "Aster snapshot bridge gap for {symbol}: U={}, u={}, snapshot={last}",
                                d["U"],
                                d["u"]
                            );
                            b.apply(&d["b"], &d["a"])?;
                            b.sequence = seq;
                            b.pending = false;
                        } else if !b.delta(&d["b"], &d["a"], seq, int(&d["pu"]))? {
                            return Ok(out);
                        }
                        out.push(b.export(&i, &self.session, &r.raw_id, t, r.received_at_ns)?);
                    }
                    "aggTrade" => {
                        self.trade(
                            &mut out,
                            &i,
                            r,
                            t,
                            &d["a"],
                            &d["p"],
                            &d["q"],
                            if d["m"] == true { "sell" } else { "buy" },
                            true,
                            false,
                        )?;
                        self.prices(&mut out, &i, r, t, &[("last", &d["p"])])?;
                    }
                    "markPriceUpdate" => {
                        let t = int(&d["E"]).map(|n| n * 1_000_000);
                        self.prices(&mut out, &i, r, t, &[("mark", &d["p"]), ("index", &d["i"])])?;
                        self.funding(
                            &mut out,
                            &i,
                            r,
                            t,
                            &d["r"],
                            int(&d["T"]).map(|n| n * 1_000_000),
                            false,
                            Decimal(SCALE),
                            None,
                        )?;
                    }
                    "forceOrder" => {
                        let o = &d["o"];
                        let long = o["S"] == "SELL";
                        self.liquidation(
                            &mut out,
                            &i,
                            r,
                            int(&o["T"]).map(|n| n * 1_000_000),
                            o,
                            if long { "sell" } else { "buy" },
                            if long { "long" } else { "short" },
                            &o["q"],
                            &o["z"],
                            &o["ap"],
                            "average_fill",
                        )?;
                    }
                    _ => {}
                }
            }
            "grvt" => {
                ensure!(
                    v.get("error").is_none() && v.get("code").is_none(),
                    "GRVT subscription error: {v}"
                );
                if let Some(result) = v.get("result") {
                    if result["stream"] == "v1.book.d"
                        && let Some(selectors) = result["subs"].as_array()
                    {
                        for (n, selector) in selectors.iter().enumerate() {
                            let symbol = selector
                                .as_str()
                                .unwrap_or("")
                                .split('@')
                                .next()
                                .unwrap_or("");
                            if let Some(b) = self.books.get_mut(symbol) {
                                b.remaining =
                                    int(&result["num_snapshots"][n]).unwrap_or(0) as usize;
                                b.snapshot_sequence = int(&result["latest_sequence_number"][n])
                                    .or_else(|| {
                                        int(&result["first_sequence_number"][n]).map(|n| n - 1)
                                    });
                            }
                        }
                    }
                    return Ok(out);
                }
                let Some(d) = v.get("feed") else {
                    return Ok(out);
                };
                if let Some(rows) = d.as_array() {
                    for row in rows {
                        let mut nested = v.clone();
                        nested["feed"] = row.clone();
                        out.extend(self.parse(r, &nested)?);
                    }
                    return Ok(out);
                }
                let symbol = d["instrument"]
                    .as_str()
                    .or_else(|| {
                        v["selector"]
                            .as_str()
                            .map(|s| s.split('@').next().unwrap_or(""))
                    })
                    .unwrap_or("");
                let Some(i) = self.items.get(symbol).cloned() else {
                    return Ok(out);
                };
                let t = int(&d["event_time"]);
                match v["stream"].as_str().unwrap_or("") {
                    "v1.book.d" => {
                        let b = self.books.get_mut(symbol).unwrap();
                        let seq = int(&v["sequence_number"]);
                        if !b.initialized {
                            ensure!(b.remaining > 0, "GRVT snapshot acknowledgement missing");
                            let count = b.remaining;
                            let predecessor = b.snapshot_sequence;
                            b.snapshot(&d["bids"], &d["asks"], seq)?;
                            b.remaining = count;
                            b.snapshot_sequence = predecessor;
                        } else if b.remaining > 0 {
                            b.apply(&d["bids"], &d["asks"])?;
                            b.sequence = seq;
                        } else if !b.delta(
                            &d["bids"],
                            &d["asks"],
                            seq,
                            int(&v["prev_sequence_number"]),
                        )? {
                            return Ok(out);
                        }
                        if b.remaining > 0 {
                            b.remaining -= 1;
                            if b.remaining > 0 {
                                return Ok(out);
                            }
                            if seq == Some(0) {
                                b.sequence = b.snapshot_sequence;
                            }
                        }
                        out.push(b.export(&i, &self.session, &r.raw_id, t, r.received_at_ns)?);
                    }
                    "v1.trade" => {
                        self.trade(
                            &mut out,
                            &i,
                            r,
                            t,
                            &d["trade_id"],
                            &d["price"],
                            &d["size"],
                            if d["is_taker_buyer"] == true {
                                "buy"
                            } else {
                                "sell"
                            },
                            false,
                            v["is_snapshot"] == true || int(&v["sequence_number"]) == Some(0),
                        )?;
                        self.prices(
                            &mut out,
                            &i,
                            r,
                            t,
                            &[
                                ("last", &d["price"]),
                                ("mark", &d["mark_price"]),
                                ("index", &d["index_price"]),
                            ],
                        )?;
                    }
                    "v1.ticker.s" => {
                        self.prices(
                            &mut out,
                            &i,
                            r,
                            t,
                            &[
                                ("mark", &d["mark_price"]),
                                ("index", &d["index_price"]),
                                ("last", &d["last_price"]),
                            ],
                        )?;
                        self.oi(&mut out, &i, r, t, &d["open_interest"])?;
                        self.funding(
                            &mut out,
                            &i,
                            r,
                            t,
                            &d["funding_rate"],
                            int(&d["next_funding_time"]),
                            false,
                            self.grvt_divisor,
                            None,
                        )?;
                        for e in &mut out {
                            if e.dataset == "funding_event" {
                                e.flag("grvt_live_funding_unit_configured");
                            }
                        }
                    }
                    _ => {}
                }
            }
            "hyperliquid" => {
                ensure!(
                    v["channel"] != "error",
                    "Hyperliquid subscription error: {v}"
                );
                let d = &v["data"];
                if v["channel"] == "trades" {
                    for d in d.as_array().context("HL trades array")? {
                        if let Some(i) = self.items.get(d["coin"].as_str().unwrap_or("")) {
                            let t = int(&d["time"]).map(|n| n * 1_000_000);
                            self.trade(
                                &mut out,
                                i,
                                r,
                                t,
                                &d["tid"],
                                &d["px"],
                                &d["sz"],
                                if d["side"] == "B" { "buy" } else { "sell" },
                                false,
                                false,
                            )?;
                            self.prices(&mut out, i, r, t, &[("last", &d["px"])])?;
                        }
                    }
                    return Ok(out);
                }
                let symbol = d["coin"].as_str().unwrap_or("");
                let Some(i) = self.items.get(symbol).cloned() else {
                    return Ok(out);
                };
                if v["channel"] == "l2Book" {
                    let b = self.books.get_mut(symbol).unwrap();
                    let epoch = b.epoch.clone();
                    b.snapshot(&d["levels"][0], &d["levels"][1], None)?;
                    if !epoch.is_empty() {
                        b.epoch = epoch;
                    }
                    out.push(b.export(
                        &i,
                        &self.session,
                        &r.raw_id,
                        int(&d["time"]).map(|n| n * 1_000_000),
                        r.received_at_ns,
                    )?);
                } else if v["channel"] == "activeAssetCtx" {
                    let d = &d["ctx"];
                    self.prices(
                        &mut out,
                        &i,
                        r,
                        None,
                        &[("mark", &d["markPx"]), ("oracle", &d["oraclePx"])],
                    )?;
                    self.oi(&mut out, &i, r, None, &d["openInterest"])?;
                    self.funding(
                        &mut out,
                        &i,
                        r,
                        None,
                        &d["funding"],
                        Some((r.received_at_ns / (3600 * SECOND) + 1) * 3600 * SECOND),
                        false,
                        Decimal(SCALE),
                        None,
                    )?;
                }
            }
            _ => {}
        }
        Ok(out)
    }
    pub fn rest_events(&self, r: &Raw, v: &Value, kind: &str) -> Result<Vec<Event>> {
        let i = self
            .items
            .get(r.symbol.as_deref().unwrap_or(""))
            .context("REST unknown instrument")?;
        let mut out = Vec::new();
        if kind == "oi" {
            self.oi(
                &mut out,
                i,
                r,
                int(&v["time"]).map(|n| n * 1_000_000),
                &v["openInterest"],
            )?;
            return Ok(out);
        }
        let rows = match r.exchange.as_str() {
            "bybit" => &v["result"]["list"],
            "grvt" => &v["result"],
            _ => v,
        };
        for row in rows.as_array().context("funding history missing array")? {
            let (t, rate, divisor, interval) = match r.exchange.as_str() {
                "bybit" => (
                    int(&row["fundingRateTimestamp"]).map(|n| n * 1_000_000),
                    &row["fundingRate"],
                    Decimal(SCALE),
                    None,
                ),
                "aster" => (
                    int(&row["fundingTime"]).map(|n| n * 1_000_000),
                    &row["fundingRate"],
                    Decimal(SCALE),
                    None,
                ),
                "grvt" => (
                    int(&row["funding_time"]),
                    &row["funding_rate"],
                    Decimal(100 * SCALE),
                    dec(&row["funding_interval_hours"])?,
                ),
                _ => (
                    int(&row["time"]).map(|n| n * 1_000_000),
                    &row["fundingRate"],
                    Decimal(SCALE),
                    None,
                ),
            };
            self.funding(&mut out, i, r, t, rate, t, true, divisor, interval)?;
        }
        Ok(out)
    }
}
pub async fn funding_request(h: &Http, i: &Instrument, since: i64) -> Result<(String, Value)> {
    let symbol = &i.native_symbol;
    match i.exchange.as_str(){"bybit"=>h.call(&format!("https://api.bybit.com/v5/market/funding/history?category=linear&symbol={symbol}&startTime={}&endTime={}&limit=200",since/1_000_000,now()/1_000_000),None).await,"aster"=>h.call(&format!("https://fapi.asterdex.com/fapi/v1/fundingRate?symbol={symbol}&startTime={}&limit=1000",since/1_000_000),None).await,"grvt"=>h.call("https://market-data.grvt.io/full/v1/funding",Some(json!({"instrument":symbol,"start_time":since.to_string(),"limit":1000,"agg_type":"FUNDING_INTERVAL"}))).await,_=>h.call("https://api.hyperliquid.xyz/info",Some(json!({"type":"fundingHistory","coin":symbol,"startTime":since/1_000_000}))).await}
}
pub fn report(items: &[Instrument], u: &Universe) -> Value {
    let ex = items.first().map(|i| i.exchange.as_str()).unwrap_or("");
    let found: BTreeMap<_, _> = items.iter().map(|i| (i.base_asset.clone(), true)).collect();
    json!({"exchange":ex,"instruments":items,"missing_assets":u.assets.keys().filter(|a|!found.contains_key(*a)).collect::<Vec<_>>(),"liquidation_coverage":coverage(ex)})
}
