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
            })),
        }
    }
}

impl Future for SleepFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Q: 这里 poll 可能被多次调用
        let mut guard = self.state.lock().unwrap();

        println!("Polling...");

        if guard.inner_state == InnerState::Done {
            return Poll::Ready(());
        }

        if guard.inner_state == InnerState::Init {
            guard.waker = Some(cx.waker().clone());
            guard.inner_state = InnerState::Sleeping;

            let duration = self.duration;
            let state_cloned = Arc::clone(&self.state);

            thread::spawn(move || {
                // Q: 这里不能通过self（线程不安全）
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

        guard.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}
