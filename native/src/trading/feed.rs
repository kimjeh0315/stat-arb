use crate::{bbo,exchanges::{self,Http,Parser},model::{self,Config,Instrument,Universe,SCALE}};
use super::engine::Quote;
use anyhow::{Result,Context};
use futures_util::{SinkExt,StreamExt};
use serde_json::Value;
use std::time::Duration;
use tokio::sync::{mpsc,watch};
use tokio_tungstenite::{connect_async,tungstenite::Message};
pub enum Update { Quote(Quote), Reset(Vec<String>) }
async fn connection(items:&[Instrument],c:&Config,tx:&mpsc::Sender<Update>,stop:&mut watch::Receiver<bool>)->Result<()> {
 let ex=&items[0].exchange;
 let (mut ws,_)=tokio::time::timeout(Duration::from_secs(20),connect_async(exchanges::ws_url(ex))).await??;
 let parser=Parser::new(items,c);
 for s in bbo::subscriptions(ex,items){ws.send(Message::Text(s.to_string().into())).await?;}
 let mut ping=tokio::time::interval(Duration::from_secs(20));
 loop {
  tokio::select! {
   _=stop.changed()=>return Ok(()),
   _=ping.tick()=>{let p=exchanges::ping(ex);if !p.is_null(){ws.send(Message::Text(p.to_string().into())).await?;}else{ws.send(Message::Ping(Vec::new().into())).await?;}},
   frame=tokio::time::timeout(Duration::from_secs(45),ws.next())=>{
    let frame=frame?.context("websocket closed")??;
    match frame {
     Message::Text(text)=>{
      let received=model::now();let v:Value=serde_json::from_str(&text)?;
      let raw=parser.raw(ex,"websocket",None,text.to_string(),received);
      for e in bbo::parse(&parser,&raw,&v)? {
       let id=e.string("instrument_id");
       if !e.boolean("quote_valid"){tx.send(Update::Reset(vec![id.into()])).await?;continue;}
       let i=items.iter().find(|i|i.key()==id).context("unknown instrument")?;
       let dec=|k:&str|e.decimal(k).map(|d|d.0 as f64/SCALE as f64).unwrap_or(f64::NAN);
       let q=Quote{id:id.into(),venue:ex.clone(),base:i.base_asset.clone(),quote:i.quote_asset.clone(),settle:i.settle_asset.clone(),bid:dec("bid_price"),ask:dec("ask_price"),bid_qty:dec("bid_qty"),ask_qty:dec("ask_qty"),received_ms:received/1_000_000,event_ms:e.integer("event_time_ns").unwrap_or(0)/1_000_000,qty_step:i.qty_step.and_then(|v|v.mul(i.multiplier).ok()).map(|d|d.0 as f64/SCALE as f64),min_qty:i.min_qty.and_then(|v|v.mul(i.multiplier).ok()).map(|d|d.0 as f64/SCALE as f64)};
       // A full queue fails this connection, invalidates its quotes and reconnects.
       tx.try_send(Update::Quote(q)).context("quote queue saturated")?;
      }
     },
     Message::Ping(p)=>ws.send(Message::Pong(p)).await?,
     Message::Close(_)=>anyhow::bail!("websocket close"),
     _=>{}
    }
   }
  }
 }
}
pub async fn start(c:&Config,tx:mpsc::Sender<Update>,stop:watch::Receiver<bool>)->Result<Vec<tokio::task::JoinHandle<()>>> {
 let u=Universe::load(c)?;let mut tasks=vec![];
 // Complete discovery before spawning tasks, so a metadata failure leaks no tasks.
 let mut groups=vec![];
 for ex in &c.exchanges {
  let items=exchanges::discover(ex,c,&u,&Http::new(c.http_requests_per_second)?).await?;
  tracing::info!(exchange=%ex,instruments=items.len(),"shadow discovery complete");
  for chunk in items.chunks(c.symbols_per_connection){groups.push(chunk.to_vec());}
 }
 for items in groups {
  let c=c.clone();let tx=tx.clone();let mut stop=stop.clone();
  tasks.push(tokio::spawn(async move {
   while !*stop.borrow() {
    let result=connection(&items,&c,&tx,&mut stop).await;
    if tx.send(Update::Reset(items.iter().map(Instrument::key).collect())).await.is_err(){return;}
    if *stop.borrow(){return;}
    if let Err(e)=result{tracing::warn!(exchange=%items[0].exchange,error=%e,"shadow feed reconnect");}
    tokio::select!{_=stop.changed()=>return,_=tokio::time::sleep(Duration::from_secs(5))=>{}}
   }
  }));
 }
 Ok(tasks)
}
