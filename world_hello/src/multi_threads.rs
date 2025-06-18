use std::cell::RefCell;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

pub fn main() {
    example4();
}

fn example4() {
    thread_local!(static FOO: RefCell<u32> = RefCell::new(1));

    FOO.with(|f| {
        assert_eq!(*f.borrow(), 1);
        *f.borrow_mut() = 2;
    });

    // 每个线程开始时都会拿到线程局部变量的FOO的初始值
    let t = thread::spawn(move || {
        FOO.with(|f| {
            assert_eq!(*f.borrow(), 1);
            *f.borrow_mut() = 3;
        });
    });

    // 等待线程完成
    t.join().unwrap();

    // 尽管子线程中修改为了3，我们在这里依然拥有main线程中的局部值：2
    FOO.with(|f| {
        assert_eq!(*f.borrow(), 2);
    });
}

fn example3() {
    let mut handles = Vec::with_capacity(3);
    let barrier = Arc::new(Barrier::new(3));

    for _ in 0..3 {
        let b = barrier.clone();
        handles.push(thread::spawn(move || {
            println!("Before barrier");
            b.wait();
            println!("After barrier!");
        }))
    }

    for handle in handles {
        handle.join().unwrap();
    }
}

fn example2() {
    let handle = thread::spawn(|| {
        // Create another thread
        thread::spawn(|| {
            loop {
                println!("I am a new thread.");
            }
        })
    });

    handle.join().unwrap();
    println!("Child thread is finish!");

    thread::sleep(Duration::from_millis(90000));
}

fn example1() {
    let v = vec![1, 2, 3];

    let handle = thread::spawn(move || println!("Here's a vector: {:?}", v));

    handle.join().unwrap();
}

fn example() {
    let handle = thread::spawn(|| {
        for i in 1..10 {
            println!("hi number {} from the spawned thread!", i);
            thread::sleep(Duration::from_millis(1));
        }
    });

    handle.join().unwrap(); // 等待线程结束

    for i in 1..5 {
        println!("hi number {} from the main thread!", i);
        thread::sleep(Duration::from_millis(1));
    }
}
