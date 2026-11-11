pub mod engine;
mod feed;
mod session;
mod upload;
use anyhow::{Result,Context,ensure};
use clap::Parser;
use serde_json::json;
use std::{io::{Write,BufRead},path::PathBuf,time::Duration};
use tokio::sync::{mpsc,watch};
#[derive(Parser)]
pub struct Cli {
 #[arg(long,default_value="config/trader-shadow.json")] config:PathBuf,
 #[arg(long)] check:bool,
 /// Upload complete JSONL snapshots to S3; never starts market feeds.
 #[arg(long)] upload_logs:bool,
 /// Offline NDJSON quotes; never calls private endpoints or submits orders.
 #[arg(long)] replay:Option<PathBuf>,
}
fn emit(w:&mut std::fs::File,v:&serde_json::Value)->Result<()> {serde_json::to_writer(&mut *w,v)?;w.write_all(b"\n")?;Ok(())}
pub async fn run()->Result<()> {
 let args=Cli::parse();let config:engine::Config=serde_json::from_slice(&std::fs::read(&args.config)?)?;config.validate()?;
 let base=args.config.parent().unwrap_or(std::path::Path::new("."));
 if args.upload_logs {return upload::run(&base.join(&config.output_dir), &base.join(&config.collector_config)).await;}
 if args.check {println!("{}",json!({"mode":"shadow","orders_enabled":false,"session_today":session::bounds(chrono::Utc::now())?,"config_valid":true,"target_per_leg":10.0}));return Ok(());}
 let dir=base.join(&config.output_dir);std::fs::create_dir_all(&dir)?;
 let mut engine=engine::Engine::new(config.clone())?;
 let now=chrono::Utc::now();
 if let Some(path)=args.replay {
  let mut output=std::fs::OpenOptions::new().write(true).create_new(true).open(dir.join(format!("replay-{}.jsonl",uuid::Uuid::new_v4())))?;
  let mut quotes=vec![];
  for line in std::io::BufReader::new(std::fs::File::open(path)?).lines(){let q:engine::Quote=serde_json::from_str(&line?)?;quotes.push(q);}
  quotes.sort_by_key(|q|q.received_ms);
  let mut next=None;let mut active_day=None;
  for q in quotes {
   let t=q.received_ms;let date=chrono::DateTime::from_timestamp_millis(t).context("invalid timestamp")?;
   ensure!(active_day.is_none_or(|d|d==date.date_naive()),"replay must contain one UTC day");active_day=Some(date.date_naive());
   let Some((start,end))=session::bounds(date)? else {continue;};
   let n=next.get_or_insert(start);
   // Replay updates become visible only after their receipt time.
   while *n<t && *n<=end {for row in engine.tick(*n,start,end){emit(&mut output,&row)?;}*n+=1000;}
   if (start..end).contains(&t){engine.quote(q);}
  }
  if let Some(day)=active_day {let (start,end)=session::bounds(day.and_hms_opt(12,0,0).unwrap().and_utc())?.context("no session")?;for row in engine.tick(end,start,end){emit(&mut output,&row)?;}}
  emit(&mut output,&json!({"type":"summary","result":engine.summary()}))?;output.sync_all()?;return Ok(());
 }
 if !session::is_open(now)? {tracing::info!("outside NYSE opening hour; no trading feed started");return Ok(());}
 let (start,end)=session::bounds(now)?.unwrap();
 // An exclusive date file prevents accidental duplicate sessions and resets of risk state.
 // A crash requires operator review; it is never silently restarted as a fresh account.
 let path=dir.join(format!("{}-shadow.jsonl",now.date_naive()));
 let mut output=std::fs::OpenOptions::new().write(true).create_new(true).open(path).context("session already exists or output unavailable; inspect previous session before restart")?;
 emit(&mut output,&json!({"type":"session_start","schema_version":1,"config":serde_json::from_slice::<serde_json::Value>(&std::fs::read(&args.config)?)?,"mode":"shadow","orders_enabled":false,"start":start,"end":end,"funding_included":false,"size_filters_complete":false,"fee_source":"research_config_not_account_verified"}))?;
 output.sync_all()?;
 let mut collector=crate::model::Config::load(&base.join(&config.collector_config))?;
 collector.bbo_only=true;collector.exchanges=config.venues.keys().cloned().collect();
 let (tx,mut rx)=mpsc::channel(4096);let (stop_tx,stop_rx)=watch::channel(false);
 let tasks=feed::start(&collector,tx,stop_rx).await?;
 let mut tick=tokio::time::interval(Duration::from_secs(1));tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
 #[cfg(unix)] let mut term=tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
 loop {
  tokio::select! {
   _=tokio::signal::ctrl_c()=>break,
   _=async{#[cfg(unix)]{term.recv().await;}#[cfg(not(unix))]{std::future::pending::<()>().await;}}=>break,
   update=rx.recv()=>match update {Some(feed::Update::Quote(q))=>engine.quote(q),Some(feed::Update::Reset(ids))=>engine.disconnect(&ids),None=>anyhow::bail!("all shadow feeds stopped")},
   _=tick.tick()=>{
    let t=chrono::Utc::now().timestamp_millis();for row in engine.tick(t,start,end){emit(&mut output,&row)?;}
    if t/1000%30==0 {let s=engine.summary();tracing::info!(summary=%s,"shadow metrics");emit(&mut output,&json!({"type":"metrics","time_ms":t,"result":s}))?;output.sync_data()?;}
    if t>=end {break;}
   }
  }
 }
 let _=stop_tx.send(true);for task in tasks{task.abort();let _=task.await;}
 emit(&mut output,&json!({"type":"session_end","result":engine.summary(),"unresolved_positions_require_review":true}))?;output.sync_all()?;
 tracing::info!(summary=%engine.summary(),"shadow session finished; no real orders were sent");
 Ok(())
}
