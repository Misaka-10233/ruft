use rand;
use tokio::select;
use tokio::sync::mpsc;
use tokio::time::Duration;

use log::warn;

// 空信号类型：通道中只关心“发生了”，不需要携带数据。
pub struct Signal;

// 随机超时计时器：Raft 用随机选举超时降低多个节点同时竞选的概率。
pub struct Timer {
    // 超时事件接收端，调用方监听它来触发选举。
    pub tick: tokio::sync::mpsc::Receiver<Signal>,

    // 重置信号发送端，收到合法 Leader 消息或投票后会重置。
    _reset_sender: tokio::sync::mpsc::Sender<Signal>,

    // 后台任务句柄，Drop 时会中止计时任务。
    _join_handle: tokio::task::JoinHandle<()>,
}

impl Timer {
    // 创建一个上下界内随机触发的计时器；若上下界传反则自动交换。
    pub fn new(mut lower_bound_ms: u32, mut upper_bound_ms: u32) -> Self {
        if lower_bound_ms > upper_bound_ms {
            std::mem::swap(&mut lower_bound_ms, &mut upper_bound_ms);
        }
        // 超时通知通道。
        let (tick_sender, tick_receiver) = mpsc::channel::<Signal>(1);
        let mut interval = random_interval(lower_bound_ms, upper_bound_ms);

        // 重置通知通道。
        let (reset_sender, mut reset_receiver) = mpsc::channel::<Signal>(1);

        // 每次 tick 或 reset 后都重新生成随机间隔，保持选举超时分散。
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

    // 重置定时器；通道满时只记录告警，避免阻塞状态机。
    pub fn reset(&mut self) {
        if let Err(err) = self._reset_sender.try_send(Signal) {
            warn!("Error sending timer reset signal: {err}");
        }
    }

    // 主动停止后台计时任务。
    pub fn stop(self) {
        self._join_handle.abort();
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        self._join_handle.abort();
    }
}

// 生成上下界内的随机 Duration。
fn rand_duration(lower_bound_ms: u32, upper_bound_ms: u32) -> Duration {
    rand::random_range(lower_bound_ms..=upper_bound_ms) * Duration::from_millis(1)
}

// 生成随机 Interval；Delay 策略避免任务暂停后补发大量 tick。
fn random_interval(lower_bound_ms: u32, upper_bound_ms: u32) -> tokio::time::Interval {
    let period = rand_duration(lower_bound_ms, upper_bound_ms);
    let mut interval = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    interval
}
