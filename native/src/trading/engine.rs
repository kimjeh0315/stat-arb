//! Shadow-only basis mean reversion. No private API, signing, or order submission.
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value,json};
use std::collections::{BTreeMap,VecDeque};

#[derive(Clone,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Venue {pub starting_equity:f64,pub taker_bps:f64,pub minimum_notional_floor:f64}
#[derive(Clone,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
 pub mode:String,pub collector_config:String,pub output_dir:String,
 pub window_seconds:usize,pub minimum_samples:usize,pub entry_z:f64,pub exit_z:f64,
 pub max_quote_age_ms:i64,pub max_exchange_age_ms:i64,pub max_quote_skew_ms:i64,
 pub entry_fraction:f64,pub max_simultaneous_pairs:usize,pub loss_limit_fraction:f64,
 pub max_holding_seconds:i64,pub stop_new_entries_before_end_seconds:i64,
 pub liquidate_before_end_seconds:i64,pub slippage_bps_per_execution:f64,
 pub minimum_edge_bps:f64,pub venues:BTreeMap<String,Venue>,
}
impl Config {
 pub fn validate(&self)->Result<()> {
  ensure!(self.mode=="shadow","only shadow mode is implemented; no real orders can be sent");
  ensure!(self.venues.len()==4 && ["bybit","aster","grvt","hyperliquid"].iter().all(|v|self.venues.contains_key(*v)),"four configured venues required");
  ensure!(self.minimum_samples>=30 && self.minimum_samples<=self.window_seconds && self.window_seconds<3600,"invalid rolling window");
  ensure!(self.entry_z.is_finite() && self.exit_z.is_finite() && self.entry_z>self.exit_z && self.exit_z>=0.,"invalid Z thresholds");
  ensure!(self.entry_fraction.is_finite() && self.entry_fraction>0. && self.entry_fraction<=0.2,"entry fraction must be <=20%");
  ensure!(self.loss_limit_fraction.is_finite() && self.loss_limit_fraction>0. && self.loss_limit_fraction<=0.1,"loss limit must be <=10%");
  ensure!((1..=2).contains(&self.max_simultaneous_pairs),"at most two disjoint venue pairs");
  ensure!(self.max_quote_age_ms>0 && self.max_quote_age_ms<=2000 && self.max_exchange_age_ms>0 && self.max_exchange_age_ms<=3000 && self.max_quote_skew_ms>=0 && self.max_quote_skew_ms<=1000,"unsafe quote freshness settings");
  ensure!(self.max_holding_seconds>0 && self.max_holding_seconds<=3600 && self.liquidate_before_end_seconds>=0 && self.stop_new_entries_before_end_seconds>self.liquidate_before_end_seconds && self.stop_new_entries_before_end_seconds<3600,"invalid session limits");
  ensure!([self.minimum_edge_bps,self.slippage_bps_per_execution].iter().all(|v|v.is_finite()&&*v>=0.),"invalid cost buffers");
  for v in self.venues.values(){ensure!(v.starting_equity.is_finite()&&v.starting_equity>0.&&v.taker_bps.is_finite()&&v.taker_bps>=0.&&v.minimum_notional_floor.is_finite()&&v.minimum_notional_floor>=0.,"invalid venue settings");}
  Ok(())
 }
}
#[derive(Clone,Debug,Serialize,Deserialize)]
pub struct Quote {
 pub id:String,pub venue:String,pub base:String,pub quote:String,pub settle:String,
 pub bid:f64,pub ask:f64,pub bid_qty:f64,pub ask_qty:f64,
 pub received_ms:i64,pub event_ms:i64,
 // Quantities are in base units, including contract multiplier conversion.
 pub qty_step:Option<f64>,pub min_qty:Option<f64>,
}
impl Quote {
 pub fn mid(&self)->f64 {(self.bid+self.ask)/2.}
 fn valid(&self,now:i64,c:&Config)->bool {
  [self.bid,self.ask,self.bid_qty,self.ask_qty].iter().all(|v|v.is_finite()&&*v>0.) && self.bid<=self.ask &&
  (0..=c.max_quote_age_ms).contains(&(now-self.received_ms)) &&
  (0..=c.max_exchange_age_ms).contains(&(now-self.event_ms)) &&
  self.qty_step.is_some_and(|s|s.is_finite()&&s>0.) && self.min_qty.is_none_or(|s|s.is_finite()&&s>=0.)
 }
}
#[derive(Default)]
struct History { samples:VecDeque<(i64,f64)> }
impl History {
 fn stats(&mut self,t:i64,c:&Config)->Option<(f64,f64)> {
  while self.samples.front().is_some_and(|(s,_)|*s<t-c.window_seconds as i64*1000){self.samples.pop_front();}
  if self.samples.back().is_some_and(|(s,_)|t-*s>2000){self.samples.clear();}
  if self.samples.len()<c.minimum_samples {return None;}
  let n=self.samples.len() as f64;
  let mean=self.samples.iter().map(|(_,v)|v).sum::<f64>()/n;
  let sd=(self.samples.iter().map(|(_,v)|(v-mean).powi(2)).sum::<f64>()/(n-1.)).sqrt();
  (sd.is_finite()&&sd>1e-12).then_some((mean,sd))
 }
}
#[derive(Clone,Serialize)]
struct Position {
 a:String,b:String,base:String,direction:f64,qty:f64,opened_ms:i64,
 mean:f64,sd:f64,a_entry:f64,b_entry:f64,entry_fee:f64,
}
pub struct Engine {
 pub config:Config,quotes:BTreeMap<String,Quote>,history:BTreeMap<String,History>,
 positions:Vec<Position>,realized:BTreeMap<String,f64>,last_tick:i64,
 pub halted:bool,pub rejection_counts:BTreeMap<String,u64>,
}
impl Engine {
 pub fn new(config:Config)->Result<Self>{config.validate()?;Ok(Self{config,quotes:BTreeMap::new(),history:BTreeMap::new(),positions:vec![],realized:BTreeMap::new(),last_tick:-1,halted:false,rejection_counts:BTreeMap::new()})}
 pub fn quote(&mut self,q:Quote){
  if !self.config.venues.contains_key(&q.venue){return;}
  if self.quotes.get(&q.id).is_some_and(|old|q.received_ms<old.received_ms || q.event_ms<old.event_ms){return;}
  self.quotes.insert(q.id.clone(),q);
 }
 pub fn disconnect(&mut self,ids:&[String]) {for id in ids {self.quotes.remove(id);} self.history.clear();}
 fn reject(&mut self,why:&str){*self.rejection_counts.entry(why.into()).or_default()+=1;}
 fn executable(&self,a:&Quote,b:&Quote,t:i64)->bool {
  a.valid(t,&self.config)&&b.valid(t,&self.config)&&(a.event_ms-b.event_ms).abs()<=self.config.max_quote_skew_ms
 }
 fn exit_values(&self,p:&Position,a:&Quote,b:&Quote)->(f64,f64,f64,f64){
  let slip=self.config.slippage_bps_per_execution/10000.;
  let (ax,bx)=if p.direction>0. {(a.ask*(1.+slip),b.bid*(1.-slip))}else{(a.bid*(1.-slip),b.ask*(1.+slip))};
  let ap=p.direction*(p.a_entry-ax)*p.qty-self.config.venues[&a.venue].taker_bps/10000.*(p.a_entry+ax)*p.qty;
  let bp=p.direction*(bx-p.b_entry)*p.qty-self.config.venues[&b.venue].taker_bps/10000.*(p.b_entry+bx)*p.qty;
  (ap,bp,ax,bx)
 }
 pub fn summary(&self)->Value {json!({"mode":"shadow","open_positions":self.positions,"realized_excluding_funding":self.realized,"halted":self.halted,"rejections":self.rejection_counts,"quotes":self.quotes.len(),"histories":self.history.len()})}
 pub fn tick(&mut self,t:i64,start:i64,end:i64)->Vec<Value>{
  let mut out=vec![];
  if t/1000<=self.last_tick {return out;} self.last_tick=t/1000;
  let mut total=self.realized.values().sum::<f64>();
  let mut all_marked=true;
  for p in &self.positions {
   if let Some((a,b))=self.quotes.get(&p.a).zip(self.quotes.get(&p.b)).filter(|(a,b)|self.executable(a,b,t)){
    let (ap,bp,_,_)=self.exit_values(p,a,b);total+=ap+bp;
   }else{all_marked=false;}
  }
  let limit=self.config.venues.values().map(|v|v.starting_equity).sum::<f64>()*self.config.loss_limit_fraction;
  if all_marked && total<=-limit {self.halted=true;}
  let old=std::mem::take(&mut self.positions);
  for p in old {
   let pair=self.quotes.get(&p.a).zip(self.quotes.get(&p.b));
   if let Some((a,b))=pair.filter(|(a,b)|self.executable(a,b,t)){
    let z=(a.mid()-b.mid()-p.mean)/p.sd; // Entry reference is frozen; moving mean cannot manufacture an exit.
    let reason=if self.halted {Some("loss_limit")}else if t>=end-self.config.liquidate_before_end_seconds*1000 {Some("session_end")}else if t-p.opened_ms>=self.config.max_holding_seconds*1000{Some("max_holding")}else if p.direction*z<=self.config.exit_z {Some("mean_reversion")}else{None};
    if let Some(reason)=reason {
     let available=if p.direction>0. {a.ask_qty.min(b.bid_qty)}else{a.bid_qty.min(b.ask_qty)};
     if available<p.qty {self.halted=true;out.push(json!({"type":"unresolved_shadow_exit","reason":"insufficient_top_size","position":p}));self.positions.push(p);continue;}
     let (ap,bp,ax,bx)=self.exit_values(&p,a,b);
     *self.realized.entry(a.venue.clone()).or_default()+=ap;
     *self.realized.entry(b.venue.clone()).or_default()+=bp;
     out.push(json!({"type":"shadow_exit","time_ms":t,"reason":reason,"position":p,"a_exit":ax,"b_exit":bx,"pnl_excluding_funding":ap+bp}));
    } else {self.positions.push(p);}
   }else{
    self.halted=true; // Do not pretend stale quotes can close a position.
    out.push(json!({"type":"unresolved_shadow_exit","reason":"stale_or_disconnected","position":p}));self.positions.push(p);
   }
  }
  if self.halted || !all_marked || t<start || t>=end-self.config.stop_new_entries_before_end_seconds*1000 {return out;}
  let quotes:Vec<_>=self.quotes.values().cloned().collect();
  let mut candidates=vec![];
  for i in 0..quotes.len(){for j in i+1..quotes.len(){
   let (a,b)=(&quotes[i],&quotes[j]);
   if a.venue==b.venue||a.base!=b.base {continue;}
   if a.quote!=b.quote||a.settle!=b.settle {self.reject("currency_mismatch");continue;}
   if !self.executable(a,b,t){self.reject("stale_invalid_or_skewed");continue;}
   let key=format!("{}|{}",a.id,b.id);let spread=a.mid()-b.mid();
   let sample_time=t/1000*1000;
   let h=self.history.entry(key).or_default();let stats=h.stats(sample_time,&self.config);
   // Score BEFORE adding current observation: no in-sample/look-ahead signal.
   h.samples.push_back((sample_time,spread));
   let Some((mean,sd))=stats else {self.reject("warmup");continue;};
   let z=(spread-mean)/sd;if z.abs()<self.config.entry_z {continue;}
   let d=z.signum();let va=&self.config.venues[&a.venue];let vb=&self.config.venues[&b.venue];
   let slip=self.config.slippage_bps_per_execution/10000.;
   let (ax,bx)=if d>0. {(a.bid*(1.-slip),b.ask*(1.+slip))}else{(a.ask*(1.+slip),b.bid*(1.-slip))};
   let cost=(a.ask-a.bid)+(b.ask-b.bid)+2.*(a.mid()*va.taker_bps+b.mid()*vb.taker_bps)/10000.+2.*slip*(a.mid()+b.mid());
   let edge=(spread-mean).abs()-cost;
   if edge<=self.config.minimum_edge_bps*(a.mid()+b.mid())/20000. {self.reject("cost");continue;}
   let cap_a=(va.starting_equity+self.realized.get(&a.venue).copied().unwrap_or(0.)).min(va.starting_equity).max(0.)*self.config.entry_fraction;
   let cap_b=(vb.starting_equity+self.realized.get(&b.venue).copied().unwrap_or(0.)).min(vb.starting_equity).max(0.)*self.config.entry_fraction;
   let available=if d>0. {a.bid_qty.min(b.ask_qty)}else{a.ask_qty.min(b.bid_qty)};
   let raw=(cap_a/ax).min(cap_b/bx).min(available);
   let Some(q)=common_quantity(raw,a.qty_step.unwrap(),b.qty_step.unwrap()) else {self.reject("quantity_step");continue;};
   if q<a.min_qty.unwrap_or(0.) || q<b.min_qty.unwrap_or(0.) || q*ax<va.minimum_notional_floor || q*bx<vb.minimum_notional_floor {self.reject("minimum_order");continue;}
   candidates.push((edge*q,a.clone(),b.clone(),Position{a:a.id.clone(),b:b.id.clone(),base:a.base.clone(),direction:d,qty:q,opened_ms:t,mean,sd,a_entry:ax,b_entry:bx,entry_fee:q*(ax*va.taker_bps+bx*vb.taker_bps)/10000.},z));
  }}
  candidates.sort_by(|a,b|b.0.total_cmp(&a.0));
  for (edge,a,b,p,z) in candidates {
   if self.positions.len()>=self.config.max_simultaneous_pairs {self.reject("position_limit");continue;}
   let overlaps=self.positions.iter().any(|p|p.base==a.base || [&p.a,&p.b].iter().any(|id|self.quotes.get(*id).is_some_and(|q|q.venue==a.venue||q.venue==b.venue)));
   if overlaps {self.reject("venue_or_asset_in_use");continue;}
   out.push(json!({"type":"shadow_entry","time_ms":t,"position":p,"z":z,"estimated_edge_excluding_funding":edge,"execution_assumption":"simultaneous_top_of_book_with_slippage","live_eligible":false}));
   self.positions.push(p);
  }
  out
 }
}
// Shadow sizing only. An eventual order adapter must use exchange decimal precision,
// tick-size and per-symbol minimum-notional filters, not this floating-point output.
fn common_quantity(raw:f64,a:f64,b:f64)->Option<f64>{
 const SCALE:f64=1e12;
 let ints=|v:f64|->Option<u128>{let n=v*SCALE;(n.is_finite()&&n>=1.&&n<=1e18&&(n-n.round()).abs()<1e-5).then_some(n.round() as u128)};
 let (a,b)=(ints(a)?,ints(b)?);let(mut x,mut y)=(a,b);while y!=0{(x,y)=(y,x%y);}
 let step=(a.checked_div(x)?.checked_mul(b)?) as f64/SCALE;
 let q=(raw/step).floor()*step;(q.is_finite()&&q>0.&&q<=raw).then_some(q)
}
#[cfg(test)] mod tests {
 use super::*;
 fn cfg()->Config {serde_json::from_str(include_str!("../../../config/trader-shadow.json")).unwrap()}
 #[test] fn fails_closed_on_live(){let mut c=cfg();c.mode="live".into();assert!(Engine::new(c).is_err());}
 #[test] fn does_not_round_up_or_unhedge(){assert!((common_quantity(0.11,0.02,0.03).unwrap()-0.06).abs()<1e-12);assert!(common_quantity(0.01,0.02,0.03).is_none());}
 #[test] fn history_excludes_current_and_resets_on_gap(){let mut c=cfg();c.minimum_samples=3;c.window_seconds=4;let mut h=History::default();for (t,v) in [(1000,1.),(2000,2.),(3000,3.)]{h.samples.push_back((t,v));}assert_eq!(h.stats(4000,&c),Some((2.,1.)));assert!(h.stats(6001,&c).is_none());assert!(h.samples.is_empty());}
 #[test] fn rejects_invalid_risk(){let mut c=cfg();c.entry_fraction=0.21;assert!(c.validate().is_err());let mut c=cfg();c.entry_z=f64::NAN;assert!(c.validate().is_err());}
 #[test] fn hyperliquid_target_equals_floor(){let c=cfg();let h=&c.venues["hyperliquid"];assert_eq!(h.starting_equity*c.entry_fraction,h.minimum_notional_floor);}
 fn quote(v:&str,t:i64,p:f64)->Quote {Quote{id:format!("{v}:COIN"),venue:v.into(),base:"COIN".into(),quote:"USDT".into(),settle:"USDT".into(),bid:p-0.001,ask:p+0.001,bid_qty:100.,ask_qty:100.,received_ms:t,event_ms:t,qty_step:Some(0.001),min_qty:Some(0.001)}}
 fn warm()->Engine {let mut c=cfg();c.window_seconds=30;c.minimum_samples=30;let mut e=Engine::new(c).unwrap();for n in 0..30 {let t=n*1000;e.quote(quote("aster",t,10.+if n%2==0 {0.01}else{-0.01}));e.quote(quote("bybit",t,10.));assert!(e.tick(t,0,3_600_000).is_empty());}e}
 #[test] fn entry_direction_and_realistic_exit(){let mut e=warm();e.quote(quote("aster",30000,11.));e.quote(quote("bybit",30000,10.));let rows=e.tick(30000,0,3_600_000);assert_eq!(rows.len(),1);assert_eq!(rows[0]["type"],"shadow_entry");assert_eq!(e.positions[0].direction,1.);assert!(e.positions[0].a_entry*e.positions[0].qty<=10.);e.quote(quote("aster",31000,10.));e.quote(quote("bybit",31000,10.));let rows=e.tick(31000,0,3_600_000);assert_eq!(rows[0]["type"],"shadow_exit");assert!(e.positions.is_empty());assert!(e.realized.values().sum::<f64>()>0.);}
 #[test] fn mismatched_currency_and_stale_quotes_cannot_enter(){let mut e=warm();let mut a=quote("aster",30000,11.);a.quote="USDC".into();e.quote(a);e.quote(quote("bybit",30000,10.));assert!(e.tick(30000,0,3_600_000).is_empty());assert!(e.positions.is_empty());let mut e=warm();e.quote(quote("aster",30000,11.));assert!(e.tick(33000,0,3_600_000).is_empty());}
 #[test] fn session_deadline_prevents_new_risk(){let mut e=warm();e.quote(quote("aster",30000,11.));e.quote(quote("bybit",30000,10.));assert!(e.tick(30000,0,330000).is_empty());}
 #[test] fn disconnect_does_not_invent_a_close(){let mut e=warm();e.quote(quote("aster",30000,11.));e.quote(quote("bybit",30000,10.));e.tick(30000,0,3_600_000);e.disconnect(&["aster:COIN".into()]);let rows=e.tick(31000,0,3_600_000);assert_eq!(rows[0]["type"],"unresolved_shadow_exit");assert!(e.halted);assert_eq!(e.positions.len(),1);}
}
