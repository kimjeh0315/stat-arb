use crate::{
    analysis::Analysis,
    exchanges::{self, Http, Parser},
    journal::Journal,
    model::*,
};
use anyhow::{Context, Result, ensure};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    sync::{mpsc, watch},
    task::JoinSet,
};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};
async fn send(tx: &mpsc::Sender<Envelope>, e: Envelope) -> Result<()> {
    if e.raw.is_some() || !e.events.is_empty() {
        tx.send(e).await.context("journal writer stopped")?;
    }
    Ok(())
}
async fn persist(tx: &mpsc::Sender<Envelope>, a: &mut Analysis, mut e: Envelope) -> Result<()> {
    e.events.retain(|e| a.ingest(e));
    send(tx, e).await
}
async fn advance(tx: &mpsc::Sender<Envelope>, a: &mut Analysis, time: i64) -> Result<()> {
    send(
        tx,
        Envelope {
            raw: None,
            events: a.advance(time)?,
        },
    )
    .await
}
struct Incoming {
    payload: String,
    received: i64,
    source: String,
    symbol: Option<String>,
    kind: String,
}
async fn polls(
    h: Http,
    items: Vec<Instrument>,
    c: Arc<Config>,
    tx: mpsc::Sender<Incoming>,
    mut stop: watch::Receiver<bool>,
) -> Result<()> {
    while h.seed_groups.load(std::sync::atomic::Ordering::Acquire) > 0 {
        tokio::select! {_=stop.changed()=>return Ok(()),_=tokio::time::sleep(Duration::from_millis(100))=>{}}
    }
    let mut next_funding = tokio::time::Instant::now();
    let mut next_oi = next_funding;
    loop {
        if *stop.borrow() {
            break;
        }
        let t = tokio::time::Instant::now();
        let funding = t >= next_funding;
        let oi = items[0].exchange == "aster" && t >= next_oi;
        if funding || oi {
            for i in &items {
                if *stop.borrow() {
                    return Ok(());
                }
                for kind in ["funding", "oi"] {
                    if (kind == "funding" && !funding) || (kind == "oi" && !oi) {
                        continue;
                    }
                    let request = async {
                        if kind == "funding" {
                            exchanges::funding_request(
                                &h,
                                i,
                                now() - c.funding_lookback_hours as i64 * 3600 * SECOND,
                            )
                            .await
                        } else {
                            h.call(
                                &format!(
                                    "https://fapi.asterdex.com/fapi/v1/openInterest?symbol={}",
                                    i.native_symbol
                                ),
                                None,
                            )
                            .await
                        }
                    };
                    let result = tokio::select! {_=stop.changed()=>return Ok(()),r=request=>r};
                    match result {
                        Ok((payload, _)) => {
                            if tx
                                .send(Incoming {
                                    payload,
                                    received: now(),
                                    source: "rest".into(),
                                    symbol: Some(i.native_symbol.clone()),
                                    kind: kind.into(),
                                })
                                .await
                                .is_err()
                            {
                                return Ok(());
                            }
                        }
                        Err(e) => {
                            tracing::warn!(exchange=%i.exchange,symbol=%i.native_symbol,kind,error=%format!("{e:#}"),"REST poll failed")
                        }
                    }
                }
            }
            if funding {
                next_funding =
                    tokio::time::Instant::now() + Duration::from_secs(c.funding_poll_seconds);
            }
            if oi {
                next_oi =
                    tokio::time::Instant::now() + Duration::from_secs(c.aster_oi_poll_seconds);
            }
        }
        tokio::select! {_=stop.changed()=>return Ok(()),_=tokio::time::sleep(Duration::from_secs(1))=>{}}
    }
    Ok(())
}
fn ack(ex: &str, v: &Value) -> Option<String> {
    match ex {
        "bybit" if v["op"] == "subscribe" && v["success"] == true => Some("all".into()),
        "aster" if v["id"] == 1 && v.get("result").is_some() && v.get("code").is_none() => {
            Some("all".into())
        }
        "grvt" => v["result"]["stream"].as_str().map(str::to_string),
        "hyperliquid" if v["channel"] == "subscriptionResponse" => {
            Some(v["data"]["subscription"].to_string())
        }
        _ => None,
    }
}
async fn connection(
    items: &[Instrument],
    c: &Config,
    h: &Http,
    tx: &mpsc::Sender<Envelope>,
    a: &mut Analysis,
    seeded: &mut bool,
    mut stop: watch::Receiver<bool>,
) -> Result<()> {
    let ex = &items[0].exchange;
    let mut p = Parser::new(items, c);
    let config = WebSocketConfig::default().max_message_size(Some(32 * 1024 * 1024));
    let (socket, _) = tokio::time::timeout(
        Duration::from_secs(30),
        connect_async_with_config(exchanges::ws_url(ex), Some(config), true),
    )
    .await??;
    let (mut writer, mut reader) = socket.split();
    let (in_tx, mut in_rx) = mpsc::channel::<Incoming>(c.queue_capacity);
    let ws_tx = in_tx.clone();
    let mut reader_job: tokio::task::JoinHandle<Result<()>> = tokio::spawn(async move {
        while let Some(m) = reader.next().await {
            match m? {
                Message::Text(s) => {
                    ws_tx
                        .send(Incoming {
                            payload: s.to_string(),
                            received: now(),
                            source: "websocket".into(),
                            symbol: None,
                            kind: "ws".into(),
                        })
                        .await
                        .context("WS processor stopped")?;
                }
                Message::Close(_) => anyhow::bail!("remote closed websocket"),
                _ => {}
            }
        }
        anyhow::bail!("websocket stream ended")
    });
    // Child tasks are always aborted on disconnect, including failed snapshot fetches.
    struct Abort(tokio::task::AbortHandle);
    impl Drop for Abort {
        fn drop(&mut self) {
            self.0.abort();
        }
    }
    let _reader_guard = Abort(reader_job.abort_handle());
    for sub in exchanges::subscriptions(ex, items) {
        writer.send(Message::Text(sub.to_string().into())).await?;
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    send(tx, p.metadata(items)).await?;
    if ex == "aster" {
        for i in items {
            let url = format!(
                "https://fapi.asterdex.com/fapi/v1/depth?symbol={}&limit=1000",
                i.native_symbol
            );
            let request = h.call(&url, None);
            let (payload, v) = tokio::select! {_=stop.changed()=>return Ok(()),r=request=>r?};
            p.seed(&i.native_symbol, &v)?;
            let r = p.raw(
                ex,
                "book_snapshot",
                Some(i.native_symbol.clone()),
                payload,
                now(),
            );
            send(
                tx,
                Envelope {
                    raw: Some(r),
                    events: vec![],
                },
            )
            .await?;
        }
        if !*seeded {
            h.seed_groups
                .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
            *seeded = true;
        }
    }
    let poll_job = tokio::spawn(polls(
        h.clone(),
        items.to_vec(),
        Arc::new(c.clone()),
        in_tx.clone(),
        stop.clone(),
    ));
    let _poll_guard = Abort(poll_job.abort_handle());
    drop(in_tx);
    let mut timer = tokio::time::interval(Duration::from_millis(100));
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut heartbeat = tokio::time::interval(Duration::from_secs(20));
    let mut last_message = Instant::now();
    let mut acked = std::collections::HashSet::new();
    let expected = if ex == "grvt" {
        3
    } else if ex == "hyperliquid" {
        items.len() * 3
    } else {
        1
    };
    let mut connected = false;
    let mut messages = 0u64;
    let mut bytes = 0u64;
    let mut parse_us = 0u128;
    let mut log = Instant::now();
    let mut watermark = 0i64;
    loop {
        if *stop.borrow() {
            reader_job.abort();
            poll_job.abort();
            in_rx.close();
        }
        tokio::select! {biased;
            _=stop.changed(),if !*stop.borrow()=>continue,
            incoming=in_rx.recv()=>{let Some(incoming)=incoming else{if *stop.borrow(){break;}anyhow::bail!("all input workers exited")};last_message=Instant::now();
                // Preserve actual receipt time; delayed records cannot rewrite an already emitted window.
                let r=p.raw(ex,&incoming.source,incoming.symbol,incoming.payload,incoming.received);
                let late=r.received_at_ns<watermark;
                advance(tx,a,r.received_at_ns).await?;watermark=watermark.max(r.received_at_ns);
                let start=Instant::now();let v:Value=match serde_json::from_str(&r.payload){Ok(v)=>v,Err(e)=>{send(tx,Envelope{raw:Some(r),events:vec![]}).await?;return Err(e.into());}};
                let parsed=if incoming.kind=="ws"{p.parse(&r,&v)}else{p.rest_events(&r,&v,&incoming.kind)};
                let mut events=match parsed{Ok(e)=>e,Err(e)=>{send(tx,Envelope{raw:Some(r),events:vec![]}).await?;return Err(e);}};
                if late{for e in &mut events{e.flag("late_processing");} }
                for e in &mut events{e.n("received_mono_ns",r.received_mono_ns);}
                parse_us+=start.elapsed().as_micros();messages+=1;bytes+=r.payload.len() as u64;
                if let Some(key)=ack(ex,&v){acked.insert(key);if acked.len()>=expected&&!connected{connected=true;persist(tx,a,p.status(items,"connected","subscriptions acknowledged",r.received_at_ns)).await?;}}
                persist(tx,a,Envelope{raw:Some(r),events}).await?;
            },
            result=&mut reader_job,if !*stop.borrow()=>{result??;anyhow::bail!("websocket reader exited");},
            _=timer.tick()=>{
                // A reader backlog must be consumed before a wall-clock watermark is advanced.
                if in_rx.is_empty(){let t=now();advance(tx,a,t).await?;watermark=watermark.max(t);}
                ensure!(last_message.elapsed()<Duration::from_secs(60),"websocket idle timeout");
                if log.elapsed()>=Duration::from_secs(30){tracing::info!(exchange=%ex,symbols=items.len(),messages,bytes,parse_us,queue=in_rx.len(),journal_queue=tx.max_capacity()-tx.capacity(),"collector metrics");messages=0;bytes=0;parse_us=0;log=Instant::now();}
            },
            _=heartbeat.tick()=>{let ping=exchanges::ping(ex);if !ping.is_null(){writer.send(Message::Text(ping.to_string().into())).await?;}else{writer.send(Message::Ping(vec![].into())).await?;}},
        }
    }
    let t = now();
    advance(tx, a, t).await?;
    persist(tx, a, p.status(items, "stopped", "graceful shutdown", t)).await?;
    let _ = writer.close().await;
    Ok(())
}
async fn shard(
    items: Vec<Instrument>,
    c: Arc<Config>,
    h: Http,
    tx: mpsc::Sender<Envelope>,
    mut stop: watch::Receiver<bool>,
) -> Result<()> {
    let mut a = Analysis::new(&items, now(), c.book_stale_seconds);
    let mut delay = 1;
    let mut seeded = false;
    loop {
        if *stop.borrow() {
            break;
        }
        match connection(&items, &c, &h, &tx, &mut a, &mut seeded, stop.clone()).await {
            Ok(()) => break,
            Err(e) => {
                let t = now();
                advance(&tx, &mut a, t).await?;
                let p = Parser::new(&items, &c);
                persist(
                    &tx,
                    &mut a,
                    p.status(&items, "disconnected", &format!("{e:#}"), t),
                )
                .await?;
                tracing::warn!(exchange=%items[0].exchange,error=%format!("{e:#}"),retry_seconds=delay,"websocket reconnect");
            }
        }
        tokio::select! {_=stop.changed()=>break,_=tokio::time::sleep(Duration::from_secs(delay))=>{}}
        delay = (delay * 2).min(60);
    }
    Ok(())
}
pub async fn discovery(c: &Config) -> Result<Vec<(String, Vec<Instrument>, Http)>> {
    let u = Universe::load(c)?;
    let mut out = vec![];
    let mut reports = vec![];
    for ex in &c.exchanges {
        let h = Http::new(c.http_requests_per_second)?;
        let items = exchanges::discover(ex, c, &u, &h).await?;
        let mut report = exchanges::report(&items, &u);
        report["exchange"] = json!(ex);
        tracing::info!(exchange=%ex,instruments=items.len(),missing=%report["missing_assets"],"discovery complete");
        reports.push(report);
        out.push((ex.clone(), items, h));
    }
    crate::journal::atomic(
        &c.spool_dir.join("reports/discovery.json"),
        &serde_json::to_vec_pretty(&reports)?,
    )?;
    Ok(out)
}
pub async fn collect(
    c: Config,
    mut stop: watch::Receiver<bool>,
    stop_tx: watch::Sender<bool>,
) -> Result<()> {
    let found = discovery(&c).await?;
    let c = Arc::new(c);
    let (tx, mut rx) = mpsc::channel::<Envelope>(c.queue_capacity);
    let copy = c.clone();
    let (result_tx, result_rx) = tokio::sync::oneshot::channel();
    let writer_stop = stop_tx.clone();
    let writer = std::thread::spawn(move || {
        let result = (|| -> Result<()> {
            let mut j = Journal::new(&copy)?;
            while let Some(e) = rx.blocking_recv() {
                j.append(&e)?;
            }
            j.seal()
        })();
        if result.is_err() {
            let _ = writer_stop.send(true);
        }
        let _ = result_tx.send(result);
    });
    let mut jobs = JoinSet::new();
    for (_, items, h) in found {
        if items.first().is_some_and(|i| i.exchange == "aster") {
            h.seed_groups.store(
                items.len().div_ceil(c.symbols_per_connection),
                std::sync::atomic::Ordering::Release,
            );
        }
        for chunk in items.chunks(c.symbols_per_connection) {
            jobs.spawn(shard(
                chunk.to_vec(),
                c.clone(),
                h.clone(),
                tx.clone(),
                stop.clone(),
            ));
        }
    }
    drop(tx);
    let mut error = None;
    while !jobs.is_empty() {
        tokio::select! {result=jobs.join_next()=>{if let Some(result)=result{match result{Ok(Ok(()))=>{},Ok(Err(e))=>{error=Some(e);let _=stop_tx.send(true);},Err(e)=>{error=Some(e.into());let _=stop_tx.send(true);}}}},_=stop.changed()=>{}}
    }
    result_rx.await.context("journal thread panicked")??;
    writer
        .join()
        .map_err(|_| anyhow::anyhow!("journal thread panicked"))?;
    if let Some(e) = error {
        return Err(e);
    }
    Ok(())
}
