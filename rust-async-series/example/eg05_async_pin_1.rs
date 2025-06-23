
use std::future::Future;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

#[tokio::main]
async fn main() {
    let fut = foo();
    let ret = Map::new(fut, |i| i + 2).await;
    println!("{}", ret);
}

async fn foo() -> i32 {
    42
}

struct Map<Fut, F> {
    fut: Fut,
    f: Option<F>,
} 

impl<Fut, F> Map<Fut, F> {
    fn new(fut: Fut, f: F) -> Self {
        Self { fut, f: Some(f) }
    }
}

impl<Fut, F, T> Future for Map<Fut, F> 
where Fut: Future,
      F: FnOnce(Fut::Output) -> T,
{
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // let fut = unsafe { &mut self.as_mut().get_unchecked_mut().fut };

        let fut = unsafe { self.as_mut().map_unchecked_mut(|map| &mut map.fut)};
        let output = ready!(fut.poll(cx));

        let f = unsafe { &mut self.as_mut().get_unchecked_mut().f.take() };
        match f.take() {
            Some(fun) => Poll::Ready(fun(output)),
            None => panic!("poll after completions"),
        }
    }
}