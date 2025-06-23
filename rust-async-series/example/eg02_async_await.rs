use rust_async_series::FooFut;

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::Duration;


#[tokio::main]
async fn main() {
    let v = vec![1, 2, 3];
    let s = String::from("hello");

    FooFut::new(v, s).await;
}


pub struct FooFut {
    state: FooFutState,
    v: Vec<u32>,
    s: String,
}

enum FooFutState {
    Init,
    Sleep1(SleepFuture),
    Sleep2(SleepFuture),
    Done,
}

impl FooFut {
    pub fn new(v: Vec<u32>, s: String) -> Self {
        Self {
            state: FooFutState::Init,
            v,
            s,
        }
    }
}

impl Future for FooFut {
    type Output = u32;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            match self.as_mut().get_mut().state {
                FooFutState::Init => {
                    println!("{:?}", self.v);
                    let fut1 = SleepFuture::new(Duration::from_secs(2));
                    self.as_mut().get_mut().state = FooFutState::Sleep1(fut1);
                }
                FooFutState::Sleep1(ref mut fut1) => match Pin::new(fut1).poll(cx) {
                    Poll::Ready(_) => {
                        println!("{}", self.s);
                        let fut2 = SleepFuture::new(Duration::from_secs(4));
                        self.as_mut().get_mut().state = FooFutState::Sleep2(fut2);
                    }
                    Poll::Pending => return Poll::Pending,
                },
                FooFutState::Sleep2(ref mut fut2) => match Pin::new(fut2).poll(cx) {
                    Poll::Ready(_) => {
                        self.as_mut().get_mut().state = FooFutState::Done;
                    }
                    Poll::Pending => {
                        return Poll::Pending;
                    }
                },
                FooFutState::Done => {
                    return Poll::Ready(42);
                }
            }
        }
    }
}

pub async fn sleep(duration: Duration) {
    SleepFuture::new(duration).await
}

pub struct SleepFuture {
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
