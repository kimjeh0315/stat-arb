use super::{Args, digest, market::Frame};
use serde::Serialize;
use std::collections::VecDeque;

#[derive(Clone, Copy, Debug)]
pub struct Signal {
    pub z: f64,
    pub deviation: f64,
}
/// Prior contiguous sampled observations only; current point is never part of its own baseline.
pub fn signals(frames: &[Frame], window: usize, step_ns: i64) -> Vec<Option<Signal>> {
    let mut history: VecDeque<f64> = VecDeque::new();
    let (mut mean, mut m2) = (0.0_f64, 0.0_f64);
    let mut previous = None;
    frames
        .iter()
        .map(|frame| {
            if previous.is_some_and(|t| frame.time_ns != t + step_ns) {
                history.clear();
                mean = 0.0;
                m2 = 0.0;
            }
            previous = Some(frame.time_ns);
            let basis = frame.basis();
            let signal = if history.len() == window {
                let sd = (m2.max(0.0) / window as f64).sqrt();
                (sd > 1e-9).then(|| Signal {
                    z: (basis - mean) / sd,
                    deviation: basis - mean,
                })
            } else {
                None
            };
            if history.len() == window {
                let old = history.pop_front().unwrap();
                let next_mean = mean + (mean - old) / (window - 1) as f64;
                m2 = (m2 - (old - mean) * (old - next_mean)).max(0.0);
                mean = next_mean;
            }
            history.push_back(basis);
            let delta = basis - mean;
            mean += delta / history.len() as f64;
            m2 += delta * (basis - mean);
            signal
        })
        .collect()
}
#[derive(Serialize)]
pub struct Trade {
    entry_time_ns: i64,
    exit_time_ns: i64,
    direction: i32,
    quantity_left: f64,
    quantity_right: f64,
    entry_left: f64,
    entry_right: f64,
    exit_left: f64,
    exit_right: f64,
    gross_pnl: f64,
    fees: f64,
    net_pnl: f64,
    net_bps_on_entry_gross: f64,
    reason: String,
}
#[derive(Clone, Copy)]
struct Position {
    time_ns: i64,
    direction: i32,
    left: f64,
    right: f64,
    ql: f64,
    qr: f64,
}
impl Position {
    fn open(frame: Frame, direction: i32, notional: f64) -> Self {
        let (left, right) = if direction == 1 {
            (frame.left.ask, frame.right.bid)
        } else {
            (frame.left.bid, frame.right.ask)
        };
        Self {
            time_ns: frame.time_ns,
            direction,
            left,
            right,
            ql: notional / left,
            qr: notional / right,
        }
    }
    fn close(self, frame: Frame, args: &Args, reason: &str) -> Trade {
        let (left, right) = if self.direction == 1 {
            (frame.left.bid, frame.right.ask)
        } else {
            (frame.left.ask, frame.right.bid)
        };
        let gross =
            self.direction as f64 * (self.ql * (left - self.left) - self.qr * (right - self.right));
        let fees = (self.ql * (self.left + left) * args.left_fee_bps
            + self.qr * (self.right + right) * args.right_fee_bps)
            / 10000.0;
        Trade {
            entry_time_ns: self.time_ns,
            exit_time_ns: frame.time_ns,
            direction: self.direction,
            quantity_left: self.ql,
            quantity_right: self.qr,
            entry_left: self.left,
            entry_right: self.right,
            exit_left: left,
            exit_right: right,
            gross_pnl: gross,
            fees,
            net_pnl: gross - fees,
            net_bps_on_entry_gross: (gross - fees) / (2.0 * args.notional_per_leg) * 10000.0,
            reason: reason.into(),
        }
    }
}
#[derive(Serialize)]
pub struct Run {
    pub variant_id: String,
    window_ms: usize,
    entry_z: f64,
    exit_z: f64,
    status: String,
    signal_samples: usize,
    cancelled_entries: usize,
    pending_at_end: bool,
    open_position: Option<serde_json::Value>,
    realized_pnl: f64,
    final_liquidation_equity: f64,
    max_drawdown: f64,
    trades: Vec<Trade>,
}
impl Run {
    pub fn summary(&self) -> serde_json::Value {
        serde_json::json!({"variant_id": self.variant_id, "window_ms": self.window_ms, "entry_z": self.entry_z, "exit_z": self.exit_z, "status": self.status, "trades": self.trades.len(), "realized_pnl": self.realized_pnl, "final_liquidation_equity": self.final_liquidation_equity, "max_drawdown": self.max_drawdown, "open_position": self.open_position, "signal_samples": self.signal_samples})
    }
}
pub fn evaluate(
    args: &Args,
    frames: &[Frame],
    signals: &[Option<Signal>],
    window: usize,
    entry: f64,
    exit: f64,
) -> Run {
    let identity = serde_json::json!({"sample_ms":args.sample_ms,"book_max_age_ms":args.book_max_age_ms,"window":window,"entry":entry,"exit":exit,"stop":args.stop_z,"min_deviation":args.min_deviation_bps,"hold":args.max_hold_seconds * 1_000_000_000,"latency":args.latency_ms * 1_000_000,"fees":[args.left_fee_bps,args.right_fee_bps],"notional":args.notional_per_leg});
    let mut run = Run {
        variant_id: digest(&serde_json::to_vec(&identity).unwrap())[..16].into(),
        window_ms: window,
        entry_z: entry,
        exit_z: exit,
        status: String::new(),
        signal_samples: 0,
        cancelled_entries: 0,
        pending_at_end: false,
        open_position: None,
        realized_pnl: 0.0,
        final_liquidation_equity: 0.0,
        max_drawdown: 0.0,
        trades: vec![],
    };
    let mut position: Option<Position> = None;
    // direction 0 means close. Decision time is kept separate from execution time.
    let mut pending: Option<(i64, i32, &str)> = None;
    let step_ns = args.sample_ms as i64 * 1_000_000;
    let mut previous = None;
    let mut peak = 0.0_f64;
    for (frame, signal) in frames.iter().copied().zip(signals) {
        let gap = previous.is_some_and(|t| frame.time_ns != t + step_ns);
        previous = Some(frame.time_ns);
        if gap {
            if pending.is_some_and(|(_, d, _)| d != 0) {
                pending = None;
                run.cancelled_entries += 1;
            }
            if position.is_some() && pending.is_none() {
                pending = Some((frame.time_ns + args.latency_ms * 1_000_000, 0, "data_gap"));
            }
        }
        let mut executed = false;
        if let Some((due, direction, reason)) = pending
            && frame.time_ns >= due
        {
            if direction == 0 {
                if let Some(p) = position.take() {
                    let t = p.close(frame, args, reason);
                    run.realized_pnl += t.net_pnl;
                    run.trades.push(t);
                }
            } else {
                position = Some(Position::open(frame, direction, args.notional_per_leg));
            }
            pending = None;
            executed = true;
        }
        if signal.is_some() {
            run.signal_samples += 1;
        }
        if pending.is_none() && !executed {
            if let Some(p) = position {
                let reason = if frame.time_ns - p.time_ns >= args.max_hold_seconds * 1_000_000_000 {
                    Some("max_hold")
                } else if signal.is_some_and(|s| s.z.abs() >= args.stop_z) {
                    Some("stop_z")
                } else if signal
                    .is_some_and(|s| s.z.abs() <= exit || s.z * p.direction as f64 >= 0.0)
                {
                    Some("mean_reversion")
                } else {
                    None
                };
                if let Some(reason) = reason {
                    pending = Some((frame.time_ns + args.latency_ms * 1_000_000, 0, reason));
                }
            } else if let Some(s) = signal
                && s.z.abs() >= entry
                && s.z.abs() < args.stop_z
                && s.deviation.abs() >= args.min_deviation_bps
            {
                pending = Some((
                    frame.time_ns + args.latency_ms * 1_000_000,
                    if s.z < 0.0 { 1 } else { -1 },
                    "entry",
                ));
            }
        }
        let equity =
            run.realized_pnl + position.map_or(0.0, |p| p.close(frame, args, "mark").net_pnl);
        peak = peak.max(equity);
        run.max_drawdown = run.max_drawdown.max(peak - equity);
        run.final_liquidation_equity = equity;
    }
    run.pending_at_end = pending.is_some();
    if let (Some(p), Some(last)) = (position, frames.last()) {
        run.open_position = Some(
            serde_json::json!({"entry_time_ns":p.time_ns,"last_mark_time_ns":last.time_ns,"direction":p.direction,"liquidation_pnl":p.close(*last,args,"mark").net_pnl,"unresolved_at_end":true}),
        );
    }
    run.status = if frames.is_empty() {
        "no_data"
    } else if run.signal_samples == 0 {
        "insufficient_warmup_or_zero_variance"
    } else if !run.trades.is_empty() {
        "traded"
    } else if position.is_some() {
        "open_position"
    } else {
        "no_closed_trades"
    }
    .into();
    run
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::research::market::Quote;
    use clap::Parser;
    fn args() -> Args {
        Args::parse_from([
            "zscore",
            "--start",
            "2026-10-25T00:00:00Z",
            "--end",
            "2026-10-26T00:00:00Z",
            "--left-instrument",
            "BTCUSDT",
            "--right-instrument",
            "BTCUSDT",
            "--output",
            "unused",
        ])
    }
    fn frame(t: i64, basis: f64) -> Frame {
        let p = (basis / 10000.0).exp() * 100.0;
        Frame {
            time_ns: t,
            left: Quote { bid: p, ask: p },
            right: Quote {
                bid: 100.0,
                ask: 100.0,
            },
        }
    }
    #[test]
    fn prior_window_and_gap() {
        let f = vec![frame(0, 0.0), frame(1, 2.0), frame(2, 4.0), frame(4, 100.0)];
        let s = signals(&f, 2, 1);
        assert!(s[0].is_none() && s[1].is_none() && s[3].is_none());
        assert!((s[2].unwrap().z - 3.0).abs() < 1e-8);
    }
    #[test]
    fn rolling_statistics_match_direct_calculation() {
        let frames: Vec<_> = (0..2000)
            .map(|t| frame(t, 500.0 + (t as f64 * 0.13).sin()))
            .collect();
        let computed = signals(&frames, 120, 1);
        for i in 120..frames.len() {
            let values: Vec<_> = frames[i - 120..i].iter().map(|f| f.basis()).collect();
            let mean = values.iter().sum::<f64>() / 120.0;
            let sd = (values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / 120.0).sqrt();
            assert!((computed[i].unwrap().z - (frames[i].basis() - mean) / sd).abs() < 1e-8);
        }
    }
    #[test]
    fn linear_short_and_four_fees() {
        let a = args();
        let p = Position {
            time_ns: 0,
            direction: -1,
            left: 100.0,
            right: 100.0,
            ql: 10.0,
            qr: 10.0,
        };
        let f = Frame {
            time_ns: 1,
            left: Quote {
                bid: 89.0,
                ask: 90.0,
            },
            right: Quote {
                bid: 100.0,
                ask: 101.0,
            },
        };
        let t = p.close(f, &a, "test");
        assert!((t.gross_pnl - 100.0).abs() < 1e-10);
        assert!((t.fees - 1.95).abs() < 1e-10);
    }
    #[test]
    fn delayed_entry_and_unresolved_end() {
        let mut a = args();
        a.latency_ms = 1;
        a.sample_ms = 1;
        let f = vec![frame(0, 0.0), frame(1_000_000, 10.0)];
        let s = vec![
            Some(Signal {
                z: 2.5,
                deviation: 10.0
            });
            2
        ];
        let r = evaluate(&a, &f, &s, 2, 2.0, 0.5);
        assert!(r.trades.is_empty());
        assert_eq!(r.open_position.unwrap()["entry_time_ns"], 1_000_000);
    }
    #[test]
    fn gap_cancels_entry() {
        let a = args();
        let f = vec![frame(0, 0.0), frame(200_000_000, 10.0)];
        let s = vec![
            Some(Signal {
                z: 2.5,
                deviation: 10.0,
            }),
            None,
        ];
        let r = evaluate(&a, &f, &s, 2, 2.0, 0.5);
        assert_eq!(r.cancelled_entries, 1);
        assert!(r.open_position.is_none());
    }
}
