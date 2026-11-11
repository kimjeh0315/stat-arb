use anyhow::{Result, bail, ensure};
use num_bigint::BigInt;
use num_traits::ToPrimitive;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    str::FromStr,
};
pub const SECOND: i64 = 1_000_000_000;
pub const SCALE: i128 = 1_000_000_000_000_000_000;
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Decimal(pub i128);
impl FromStr for Decimal {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        let s = s.trim();
        ensure!(!s.is_empty(), "empty decimal");
        let (mantissa, exponent) = s.split_once(['e', 'E']).unwrap_or((s, "0"));
        let exponent: i32 = exponent.parse()?;
        ensure!(
            (-100..=100).contains(&exponent),
            "decimal exponent too large"
        );
        ensure!(s.len() <= 512, "decimal input too long");
        let negative = mantissa.starts_with('-');
        let m = mantissa
            .strip_prefix('-')
            .or_else(|| mantissa.strip_prefix('+'))
            .unwrap_or(mantissa);
        let (whole, fraction) = m.split_once('.').unwrap_or((m, ""));
        let digits = format!("{whole}{fraction}");
        ensure!(
            !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()),
            "invalid decimal {s}"
        );
        let shift = 18 + exponent - fraction.len() as i32;
        if let Ok(mut v) = digits.parse::<i128>() {
            let magnitude = shift.unsigned_abs();
            if magnitude <= 38 {
                let power = 10i128.pow(magnitude);
                if shift >= 0 {
                    if let Some(n) = v.checked_mul(power) {
                        return Self::checked_i128(if negative { -n } else { n });
                    }
                } else {
                    ensure!(v % power == 0, "precision exceeds 18 decimal places: {s}");
                    v /= power;
                    return Self::checked_i128(if negative { -v } else { v });
                }
            }
        }
        let mut v = BigInt::from_str(&digits)?;
        if shift >= 0 {
            v *= BigInt::from(10).pow(shift as u32);
        } else {
            let divisor = BigInt::from(10).pow((-shift) as u32);
            ensure!(
                &v % &divisor == BigInt::from(0),
                "precision exceeds 18 decimal places: {s}"
            );
            v /= divisor;
        }
        if negative {
            v = -v;
        }
        Self::checked(v)
    }
}
impl Decimal {
    fn checked_i128(n: i128) -> Result<Self> {
        ensure!(
            n.unsigned_abs() < 10u128.pow(38),
            "Decimal128(38,18) overflow"
        );
        Ok(Self(n))
    }
    fn checked(v: BigInt) -> Result<Self> {
        ensure!(v.bits() <= 127, "decimal overflow");
        let n = v
            .to_i128()
            .ok_or_else(|| anyhow::anyhow!("decimal overflow"))?;
        ensure!(
            n.unsigned_abs() < 10u128.pow(38),
            "Decimal128(38,18) overflow"
        );
        Ok(Self(n))
    }
    pub fn add(self, b: Self) -> Result<Self> {
        Self::checked_i128(
            self.0
                .checked_add(b.0)
                .ok_or_else(|| anyhow::anyhow!("decimal overflow"))?,
        )
    }
    pub fn sub(self, b: Self) -> Result<Self> {
        Self::checked_i128(
            self.0
                .checked_sub(b.0)
                .ok_or_else(|| anyhow::anyhow!("decimal overflow"))?,
        )
    }
    pub fn mul(self, b: Self) -> Result<Self> {
        if b.0 == SCALE {
            return Self::checked_i128(self.0);
        }
        if let Some(n) = self.0.checked_mul(b.0) {
            return Self::checked_i128(n / SCALE);
        }
        Self::checked(BigInt::from(self.0) * BigInt::from(b.0) / SCALE)
    }
    pub fn div(self, b: Self) -> Result<Self> {
        ensure!(b.0 != 0, "division by zero");
        if b.0 == SCALE {
            return Self::checked_i128(self.0);
        }
        if let Some(n) = self.0.checked_mul(SCALE) {
            return Self::checked_i128(n / b.0);
        }
        Self::checked(BigInt::from(self.0) * SCALE / b.0)
    }
}
impl std::fmt::Display for Decimal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let n = self.0.unsigned_abs();
        write!(
            f,
            "{}{}.{:018}",
            if self.0 < 0 { "-" } else { "" },
            n / SCALE as u128,
            n % SCALE as u128
        )
    }
}
pub fn text(v: &Value) -> String {
    v.as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| v.to_string())
}
pub fn dec(v: &Value) -> Result<Option<Decimal>> {
    if v.is_null() || v.as_str() == Some("") {
        Ok(None)
    } else {
        Ok(Some(text(v).parse()?))
    }
}
pub fn int(v: &Value) -> Option<i64> {
    text(v).parse().ok()
}
pub fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64
}
pub fn mono() -> i64 {
    static START: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
    START
        .get_or_init(std::time::Instant::now)
        .elapsed()
        .as_nanos() as i64
}
pub fn id(s: &str) -> String {
    format!("{:x}", Sha256::digest(s.as_bytes()))[..32].to_owned()
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub bbo_only: bool,
    pub exchanges: Vec<String>,
    pub quote_assets: Vec<String>,
    pub universe_path: PathBuf,
    pub spool_dir: PathBuf,
    #[serde(default)]
    pub s3_bucket: String,
    pub s3_prefix: String,
    pub aws_region: String,
    pub book_depth: usize,
    pub analysis_interval_seconds: u64,
    pub book_stale_seconds: u64,
    pub journal_rotate_seconds: u64,
    pub journal_max_bytes: u64,
    pub journal_fsync_seconds: f64,
    pub max_spool_bytes: u64,
    pub queue_capacity: usize,
    pub symbols_per_connection: usize,
    pub aster_oi_poll_seconds: u64,
    pub funding_poll_seconds: u64,
    pub funding_lookback_hours: u64,
    pub http_requests_per_second: f64,
    pub grvt_live_funding_unit: String,
    #[serde(default)]
    pub external_liquidation_sources: Vec<Value>,
    #[serde(default)]
    pub aliases: Value,
    #[serde(default = "archive_interval")]
    pub archive_batch_seconds: u64,
    #[serde(default = "archive_size")]
    pub archive_batch_bytes: u64,
}
fn archive_interval() -> u64 {
    120
}
fn archive_size() -> u64 {
    64 * 1024 * 1024
}
impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let mut c: Self = serde_json::from_slice(&std::fs::read(path)?)?;
        let parent = path.parent().unwrap_or(Path::new("."));
        c.universe_path = parent.join(&c.universe_path);
        c.spool_dir = parent.join(&c.spool_dir);
        for (env, field) in [
            ("QUANT_S3_BUCKET", &mut c.s3_bucket),
            ("QUANT_S3_PREFIX", &mut c.s3_prefix),
            ("AWS_REGION", &mut c.aws_region),
        ] {
            if let Ok(v) = std::env::var(env) {
                *field = v;
            }
        }
        if let Ok(v) = std::env::var("QUANT_SPOOL_DIR") {
            c.spool_dir = v.into();
        }
        ensure!(
            c.book_depth == 20 && c.analysis_interval_seconds == 1,
            "schema requires book_depth=20 and analysis_interval_seconds=1"
        );
        ensure!(
            !c.exchanges.is_empty()
                && c.exchanges
                    .iter()
                    .all(|s| ["bybit", "aster", "grvt", "hyperliquid"].contains(&s.as_str())),
            "unsupported exchange"
        );
        ensure!(
            (1..=20).contains(&c.symbols_per_connection) && c.queue_capacity > 0,
            "invalid queue/connection size"
        );
        ensure!(
            c.http_requests_per_second > 0. && c.http_requests_per_second.is_finite(),
            "invalid REST rate"
        );
        ensure!(
            c.journal_rotate_seconds > 0
                && c.journal_max_bytes > 0
                && c.max_spool_bytes > 0
                && c.journal_fsync_seconds >= 0.
                && c.journal_fsync_seconds.is_finite()
                && c.archive_batch_bytes > 0
                && c.archive_batch_seconds > 0,
            "invalid storage settings"
        );
        ensure!(
            c.aster_oi_poll_seconds > 0 && c.funding_poll_seconds > 0 && c.book_stale_seconds > 0,
            "invalid polling/stale intervals"
        );
        ensure!(
            c.quote_assets
                .iter()
                .all(|s| ["USDT", "USDC"].contains(&s.as_str())),
            "unsupported quote asset"
        );
        ensure!(
            ["percentage_points", "centibeeps"].contains(&c.grvt_live_funding_unit.as_str()),
            "invalid GRVT funding unit"
        );
        // Do not silently ignore a configured provider while claiming coverage.
        ensure!(
            c.external_liquidation_sources.is_empty(),
            "external liquidation providers are not implemented in this Rust stage; remove them or keep the existing collector until provider support is ported"
        );
        Ok(c)
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Instrument {
    pub exchange: String,
    pub native_symbol: String,
    pub native_base: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub settle_asset: String,
    pub category: String,
    pub multiplier: Decimal,
    pub tick_size: Option<Decimal>,
    pub qty_step: Option<Decimal>,
    pub min_qty: Option<Decimal>,
    pub funding_interval_hours: Option<Decimal>,
}
impl Instrument {
    pub fn key(&self) -> String {
        format!("{}:{}", self.exchange, self.native_symbol)
    }
    pub fn price(&self, v: &Value) -> Result<Option<Decimal>> {
        dec(v)?.map(|d| d.div(self.multiplier)).transpose()
    }
    pub fn qty(&self, v: &Value) -> Result<Option<Decimal>> {
        dec(v)?.map(|d| d.mul(self.multiplier)).transpose()
    }
}
pub struct Universe {
    pub assets: BTreeMap<String, String>,
    aliases: Value,
}
impl Universe {
    pub fn load(c: &Config) -> Result<Self> {
        let groups: BTreeMap<String, Vec<String>> =
            serde_json::from_slice(&std::fs::read(&c.universe_path)?)?;
        let mut assets = BTreeMap::new();
        for (group, coins) in groups {
            for coin in coins {
                ensure!(
                    assets.insert(coin.clone(), group.clone()).is_none(),
                    "duplicate asset {coin}"
                );
            }
        }
        Ok(Self {
            assets,
            aliases: c.aliases.clone(),
        })
    }
    pub fn instrument(
        &self,
        ex: &str,
        symbol: &str,
        base: &str,
        quote: &str,
        settle: &str,
    ) -> Result<Option<Instrument>> {
        let alias = &self.aliases[ex][base];
        let (asset, multiplier) = if !alias.is_null() {
            (
                text(&alias["asset"]),
                dec(&alias["multiplier"])?
                    .ok_or_else(|| anyhow::anyhow!("missing alias multiplier"))?,
            )
        } else {
            let upper = base.to_uppercase();
            let mut found = None;
            if self.assets.contains_key(&upper) {
                found = Some((upper.clone(), Decimal(SCALE)));
            }
            for prefix in ["1000000", "10000", "1000"] {
                if let Some(b) = upper.strip_prefix(prefix)
                    && self.assets.contains_key(b)
                {
                    found = Some((b.to_string(), prefix.parse()?));
                }
            }
            if ex == "hyperliquid" && base.starts_with('k') {
                let b = base[1..].to_uppercase();
                if self.assets.contains_key(&b) {
                    found = Some((b, "1000".parse()?));
                }
            }
            match found {
                Some(f) => f,
                None => return Ok(None),
            }
        };
        ensure!(multiplier.0 > 0, "invalid alias multiplier");
        let category = self
            .assets
            .get(&asset)
            .ok_or_else(|| anyhow::anyhow!("alias asset outside universe"))?
            .clone();
        Ok(Some(Instrument {
            exchange: ex.into(),
            native_symbol: symbol.into(),
            native_base: base.into(),
            base_asset: asset,
            quote_asset: quote.into(),
            settle_asset: settle.into(),
            category,
            multiplier,
            tick_size: None,
            qty_step: None,
            min_qty: None,
            funding_interval_hours: None,
        }))
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Level {
    pub price: Decimal,
    pub qty: Decimal,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Cell {
    String(String),
    Decimal(Decimal),
    Int(i64),
    Float(f64),
    Bool(bool),
    Strings(Vec<String>),
    Levels(Vec<Level>),
}
pub type Fields = BTreeMap<String, Cell>;
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Event {
    pub dataset: String,
    pub fields: Fields,
}
impl Event {
    pub fn new(
        dataset: &str,
        i: &Instrument,
        session: &str,
        raw: &str,
        time: Option<i64>,
        received: i64,
    ) -> Self {
        let mut e = Self {
            dataset: dataset.into(),
            fields: Fields::new(),
        };
        for (k, v) in [
            ("event_id", id(&format!("{raw}:{dataset}:{}", i.key()))),
            ("exchange", i.exchange.clone()),
            ("instrument_id", i.key()),
            ("base_asset", i.base_asset.clone()),
            ("category", i.category.clone()),
            ("quote_asset", i.quote_asset.clone()),
            ("session_id", session.into()),
            ("raw_id", raw.into()),
        ] {
            e.s(k, v);
        }
        e.n("received_at_ns", received);
        e.n("received_mono_ns", mono());
        e.n("schema_version", 2);
        if let Some(t) = time {
            e.n("event_time_ns", t);
        }
        e.fields.insert(
            "quality_flags".into(),
            Cell::Strings(if time.is_none() {
                vec!["event_time_unavailable".into()]
            } else {
                vec![]
            }),
        );
        e
    }
    pub fn s(&mut self, k: &str, v: impl Into<String>) {
        self.fields.insert(k.into(), Cell::String(v.into()));
    }
    pub fn n(&mut self, k: &str, v: i64) {
        self.fields.insert(k.into(), Cell::Int(v));
    }
    pub fn d(&mut self, k: &str, v: Option<Decimal>) {
        if let Some(v) = v {
            self.fields.insert(k.into(), Cell::Decimal(v));
        }
    }
    pub fn b(&mut self, k: &str, v: bool) {
        self.fields.insert(k.into(), Cell::Bool(v));
    }
    pub fn string(&self, k: &str) -> &str {
        match self.fields.get(k) {
            Some(Cell::String(s)) => s,
            _ => "",
        }
    }
    pub fn integer(&self, k: &str) -> Option<i64> {
        match self.fields.get(k) {
            Some(Cell::Int(v)) => Some(*v),
            _ => None,
        }
    }
    pub fn decimal(&self, k: &str) -> Option<Decimal> {
        match self.fields.get(k) {
            Some(Cell::Decimal(v)) => Some(*v),
            _ => None,
        }
    }
    pub fn boolean(&self, k: &str) -> bool {
        matches!(self.fields.get(k), Some(Cell::Bool(true)))
    }
    pub fn flag(&mut self, s: &str) {
        if let Some(Cell::Strings(v)) = self.fields.get_mut("quality_flags") {
            v.push(s.into());
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Raw {
    pub exchange: String,
    pub session_id: String,
    pub raw_id: String,
    pub received_at_ns: i64,
    pub received_mono_ns: i64,
    pub source: String,
    #[serde(default)]
    pub symbol: Option<String>,
    pub payload: String,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Envelope {
    pub raw: Option<Raw>,
    pub events: Vec<Event>,
}
pub fn coverage(ex: &str) -> &'static str {
    match ex {
        "bybit" => "exchange_all_liquidation",
        "aster" => "latest_per_symbol_1000ms",
        _ => "unavailable_public_api",
    }
}
pub fn required(v: Option<Decimal>) -> Result<Decimal> {
    v.ok_or_else(|| anyhow::anyhow!("required decimal missing"))
}
pub fn legacy_event(v: &Value) -> Result<Event> {
    let mut e = Event {
        dataset: text(&v["dataset"]),
        fields: Fields::new(),
    };
    let types = crate::schema::fields(&e.dataset)?;
    for (k, t) in types {
        let x = &v[k];
        if x.is_null() {
            continue;
        }
        let cell = match t {
            crate::schema::Kind::Decimal => Cell::Decimal(required(dec(x)?)?),
            crate::schema::Kind::Int | crate::schema::Kind::Int32 => {
                Cell::Int(int(x).ok_or_else(|| anyhow::anyhow!("invalid integer {k}"))?)
            }
            crate::schema::Kind::Float => Cell::Float(x.as_f64().unwrap_or(text(x).parse()?)),
            crate::schema::Kind::Bool => {
                Cell::Bool(x.as_bool().ok_or_else(|| anyhow::anyhow!("invalid bool"))?)
            }
            crate::schema::Kind::Strings => Cell::Strings(
                x.as_array()
                    .ok_or_else(|| anyhow::anyhow!("invalid strings"))?
                    .iter()
                    .map(text)
                    .collect(),
            ),
            crate::schema::Kind::Levels => Cell::Levels(
                x.as_array()
                    .ok_or_else(|| anyhow::anyhow!("invalid levels"))?
                    .iter()
                    .map(|l| {
                        Ok(Level {
                            price: required(dec(&l["price"])?)?,
                            qty: required(dec(&l["qty"])?)?,
                        })
                    })
                    .collect::<Result<_>>()?,
            ),
            crate::schema::Kind::String => Cell::String(text(x)),
        };
        e.fields.insert(k.into(), cell);
    }
    if e.dataset.is_empty() {
        bail!("legacy event missing dataset");
    }
    Ok(e)
}
