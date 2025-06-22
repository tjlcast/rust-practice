
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::Duration;


#[tokio::main]
async fn main() {
    println!("Start in main");
    SleepFuture::new(Duration::from_secs(1)).await;
}

struct SleepFuture {
    duration: Duration,
    state: Arc<Mutex<State>>,
}

struct State {
    waker: Option<Waker>,
    inner_state: InnerState,
}

#[derive(PartialEq)]
enum InnerState {
    Init,
    Sleeping,
    Done,
}

impl SleepFuture {
    fn new(duration: Duration) -> Self {
        Self { 
            duration, 
            state: Arc::new(Mutex::new(State {
                waker: None,
                inner_state: InnerState::Init,
            } ))}
    }
}

impl Future for SleepFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // 获取状态锁
        let mut guard = self.state.lock().unwrap();

        println!("Polling...");

        // 如果状态为完成，则返回完成
        if guard.inner_state == InnerState::Done {
            return Poll::Ready(());
        }

        // 如果状态为初始化，则设置waker，并将状态设置为睡眠
        if guard.inner_state == InnerState::Init {
            guard.waker = Some(cx.waker().clone());
            guard.inner_state = InnerState::Sleeping;

            // 获取持续时间
            let duration = self.duration;
            // 克隆状态
            let state_cloned = Arc::clone(&self.state);

            // 创建新线程，睡眠指定时间后，将状态设置为完成，并唤醒waker
            thread::spawn(move || {
                println!("Start sleeping for {:?} seconds", duration);
                thread::sleep(duration);
                let mut guard = state_cloned.lock().unwrap();
                guard.inner_state = InnerState::Done;
                if let Some(waker) = guard.waker.take() {
                    waker.wake();
                }
                println!("Done sleeping");
            });
        }
         
        // 设置waker
        guard.waker = Some(cx.waker().clone());
        // 返回等待
        Poll::Pending
    }
}
