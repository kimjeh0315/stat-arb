//! Full BBO snapshots only: no trades, funding, depth reconstruction or derived signals.
use crate::{exchanges::Parser, model::*};
use anyhow::{Result, ensure};
use serde_json::{Value, json};

pub fn subscriptions(ex: &str, items: &[Instrument]) -> Vec<Value> {
    match ex {
        "bybit" => vec![json!({"op":"subscribe","args":items.iter().map(|i|format!("orderbook.1.{}",i.native_symbol)).collect::<Vec<_>>()})],
        "aster" => vec![json!({"method":"SUBSCRIBE","id":1,"params":items.iter().map(|i|format!("{}@bookTicker",i.native_symbol.to_lowercase())).collect::<Vec<_>>()})],
        "grvt" => vec![json!({"jsonrpc":"2.0","method":"subscribe","id":1,"params":{"stream":"v1.mini.s","selectors":items.iter().map(|i|format!("{}@200",i.native_symbol)).collect::<Vec<_>>()}})],
        _ => items.iter().map(|i|json!({"method":"subscribe","subscription":{"type":"bbo","coin":i.native_symbol}})).collect(),
    }
}

pub fn parse(p: &Parser, r: &Raw, v: &Value) -> Result<Vec<Event>> {
    ensure!(v["success"] != false && v.get("error").is_none() && v["channel"] != "error" && v.get("code").is_none(), "BBO subscription rejected: {v}");
    let (symbol, t, seq, values): (&str, Option<i64>, &Value, [&Value;4]) = match r.exchange.as_str() {
        "bybit" => {
            let topic = v["topic"].as_str().unwrap_or("");
            if !topic.starts_with("orderbook.1.") { return Ok(vec![]); }
            ensure!(v["type"] == "snapshot", "unexpected Bybit level-1 delta");
            let d = &v["data"];
            (d["s"].as_str().unwrap_or(""), int(&v["cts"]).or_else(||int(&v["ts"])).map(|t|t*1_000_000), &d["u"], [&d["b"][0][0], &d["b"][0][1], &d["a"][0][0], &d["a"][0][1]])
        },
        "aster" => {
            let d = v.get("data").unwrap_or(v);
            if d["e"] != "bookTicker" {return Ok(vec![]);}
            (d["s"].as_str().unwrap_or(""), int(&d["T"]).or_else(||int(&d["E"])).map(|t|t*1_000_000), &d["u"], [&d["b"],&d["B"],&d["a"],&d["A"]])
        },
        "grvt" => {
            if v["stream"] != "v1.mini.s" {return Ok(vec![]);}
            let d = &v["feed"];
            if let Some(rows) = d.as_array() {
                let mut out = vec![];
                for row in rows {let mut nested=v.clone();nested["feed"]=row.clone();out.extend(parse(p,r,&nested)?);}
                return Ok(out);
            }
            (d["instrument"].as_str().or_else(||v["selector"].as_str().map(|s|s.split('@').next().unwrap_or(""))).unwrap_or(""), int(&d["event_time"]), &v["sequence_number"], [&d["best_bid_price"],&d["best_bid_size"],&d["best_ask_price"],&d["best_ask_size"]])
        },
        "hyperliquid" => {
            if v["channel"] != "bbo" {return Ok(vec![]);}
            let d=&v["data"];
            (d["coin"].as_str().unwrap_or(""), int(&d["time"]).map(|t|t*1_000_000), &Value::Null, [&d["bbo"][0]["px"],&d["bbo"][0]["sz"],&d["bbo"][1]["px"],&d["bbo"][1]["sz"]])
        },
        _ => anyhow::bail!("unsupported BBO exchange"),
    };
    let i=p.items.get(symbol).ok_or_else(||anyhow::anyhow!("unknown BBO symbol {symbol}"))?;
    let mut e=Event::new("bbo",i,&p.session,&r.raw_id,t,r.received_at_ns);
    e.fields.remove("raw_id"); // No raw payload is persisted in this mode.
    let mut parsed=[None;4];
    for (n,(key,value)) in ["bid_price","bid_qty","ask_price","ask_qty"].iter().zip(values).enumerate() {
        let val=dec(value)?.map(|d| if n%2==0 {d.div(i.multiplier)} else {d.mul(i.multiplier)}).transpose()?;
        parsed[n]=val;e.d(key,val);
    }
    let valid=parsed.iter().all(|x|x.is_some_and(|d|d.0>0)) && parsed[0]<=parsed[2];
    e.b("quote_valid",valid);
    if !valid {e.flag("invalid_or_empty_bbo");}
    if !seq.is_null(){e.s("source_sequence",text(seq));}
    Ok(vec![e])
}

#[cfg(test)]
mod tests {
    use super::*;
    fn parser(ex: &str) -> Parser {
        let c: Config = serde_json::from_str(include_str!("../../config/collector-bbo.json")).unwrap();
        let i = Instrument { exchange: ex.into(), native_symbol:"BTC".into(), native_base:"BTC".into(), base_asset:"BTC".into(), quote_asset:"USDC".into(), settle_asset:"USDC".into(), category:"anchor".into(), multiplier:Decimal(1000*SCALE), tick_size:None, qty_step:None, min_qty:None, funding_interval_hours:None };
        Parser::new(&[i], &c)
    }
    #[test]
    fn four_venues_normalize_units_and_timestamps() {
        for (ex,v) in [
            ("bybit",json!({"topic":"orderbook.1.BTC","type":"snapshot","ts":2000,"cts":1000,"data":{"s":"BTC","u":4,"b":[["1000","2"]],"a":[["2000","3"]]}})),
            ("aster",json!({"e":"bookTicker","s":"BTC","T":1000,"u":4,"b":"1000","B":"2","a":"2000","A":"3"})),
            ("grvt",json!({"stream":"v1.mini.s","sequence_number":"4","feed":{"instrument":"BTC","event_time":"1000000000","best_bid_price":"1000","best_bid_size":"2","best_ask_price":"2000","best_ask_size":"3"}})),
            ("hyperliquid",json!({"channel":"bbo","data":{"coin":"BTC","time":1000,"bbo":[{"px":"1000","sz":"2"},{"px":"2000","sz":"3"}]}})),
        ] {
            let p=parser(ex);let r=p.raw(ex,"websocket",None,v.to_string(),2*SECOND);
            let rows=parse(&p,&r,&v).unwrap();let e=&rows[0];
            assert_eq!(rows.len(),1);assert!(e.boolean("quote_valid"));
            assert_eq!(e.decimal("bid_price"),Some(Decimal(SCALE)));
            assert_eq!(e.decimal("ask_qty"),Some(Decimal(3000*SCALE)));
            assert_eq!(e.integer("event_time_ns"),Some(SECOND));
            assert_eq!(crate::schema::batch("bbo",&rows).unwrap().num_rows(),1);
        }
    }
    #[test]
    fn empty_side_invalidates_quote_and_subscription_error_is_not_ignored() {
        let p=parser("hyperliquid");let v=json!({"channel":"bbo","data":{"coin":"BTC","time":1000,"bbo":[null,{"px":"2000","sz":"3"}]}});
        let r=p.raw("hyperliquid","websocket",None,v.to_string(),2*SECOND);
        let rows=parse(&p,&r,&v).unwrap();assert!(!rows[0].boolean("quote_valid"));
        assert!(rows[0].decimal("bid_price").is_none());
        assert!(parse(&p,&r,&json!({"channel":"error","data":"bad subscription"})).is_err());
    }
}
