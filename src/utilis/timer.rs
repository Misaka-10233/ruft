use rand;
use tokio::select;
use tokio::sync::mpsc;
use tokio::time::Duration;

use log::warn;

pub struct Signal;

pub struct Timer {
    pub tick: tokio::sync::mpsc::Receiver<Signal>,

    // reset channel
    _reset_sender: tokio::sync::mpsc::Sender<Signal>,

    _join_handle: tokio::task::JoinHandle<()>,
}

impl Timer {
    pub fn new(mut lower_bound_ms: u32, mut upper_bound_ms: u32) -> Self {
        if lower_bound_ms > upper_bound_ms {
            std::mem::swap(&mut lower_bound_ms, &mut upper_bound_ms);
        }
        // tick channel
        let (tick_sender, tick_receiver) = mpsc::channel::<Signal>(1);
        let mut interval = random_interval(lower_bound_ms, upper_bound_ms);

        // reset channel
        let (reset_sender, mut reset_receiver) = mpsc::channel::<Signal>(1);

        // 启动定时器
        let join_handle = tokio::spawn(async move {
            loop {
                select! {
                    _ = interval.tick() => {
                        if tick_sender.send(Signal).await.is_err() {
                            break;
                        }
                    }
                    recv = reset_receiver.recv() => {
                        if recv.is_none(){
                            break;
                        }
                    }
                }
                interval = random_interval(lower_bound_ms, upper_bound_ms);
            }
        });
        Self {
            tick: tick_receiver,
            _reset_sender: reset_sender,
            _join_handle: join_handle,
        }
    }

    // 重置定时器
    pub fn reset(&mut self) {
        if let Err(err) = self._reset_sender.try_send(Signal) {
            warn!("Error sending timer reset signal: {err}");
        }
    }

    // 停止定时器
    pub fn stop(self) {
        self._join_handle.abort();
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        self._join_handle.abort();
    }
}

// 生成一个随机的Duration，时间间隔在lower_bound_ms到upper_bound_ms之间
fn rand_duration(lower_bound_ms: u32, upper_bound_ms: u32) -> Duration {
    rand::random_range(lower_bound_ms..=upper_bound_ms) * Duration::from_millis(1)
}

// 生成一个随机的Interval，时间间隔在lower_bound_ms到upper_bound_ms之间
// 每次tick时，会发送一个Signal到tick channel
fn random_interval(lower_bound_ms: u32, upper_bound_ms: u32) -> tokio::time::Interval {
    let mut interval = tokio::time::interval_at(
        tokio::time::Instant::now(),
        rand_duration(lower_bound_ms, upper_bound_ms),
    );
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    interval
}
