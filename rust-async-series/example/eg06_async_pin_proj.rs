use pin_project_lite::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

#[tokio::main]
async fn main() {
    let fut = foo();
    // let ret = Map::new(fut, |i| i + 2).await;
    let ret = fut.map(|x| x * 2).await;
    println!("{}", ret);
}

async fn foo() -> i32 {
    42
}

trait FutureExt: Future {
    fn map<F, T>(self, f: F) -> Map<Self, F>
    where
        F: FnOnce(Self::Output) -> T,
        Self: Sized,
    {
        Map::new(self, f)
    }
}

impl<T: Future> FutureExt for T {}

pin_project! {
    struct Map<Fut, F> {
        #[pin]
        fut: Fut,
        f: Option<F>,
    }
}

impl<Fut, F> Map<Fut, F> {
    fn new(fut: Fut, f: F) -> Self {
        Self { fut, f: Some(f) }
    }
}

impl<Fut, F, T> Future for Map<Fut, F>
where
    Fut: Future,
    F: FnOnce(Fut::Output) -> T,
{
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // let fut = unsafe { &mut self.as_mut().get_unchecked_mut().fut };

        // let fut = unsafe { self.as_mut().map_unchecked_mut(|map| &mut map.fut) };

        let this = self.project();
        let fut = this.fut;

        let output = ready!(fut.poll(cx));

        // let f = unsafe { &mut self.as_mut().get_unchecked_mut().f.take() };
        let f = this.f;

        match f.take() {
            Some(fun) => Poll::Ready(fun(output)),
            None => panic!("poll after completions"),
        }
    }
}
