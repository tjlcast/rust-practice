use std::ptr;

pub struct List<T> {
    head: Link<T>,
    tail: *mut Node<T>,
}

// type Link<T> = Option<Box<Node<T>>>;
type Link<T> = *mut Node<T>;

struct Node<T> {
    elem: T,
    real_next: Option<Box<Node<T>>>,
    next: Link<T>,
}

impl<T> List<T> {
    pub fn new() -> Self {
        List {
            head: ptr::null_mut(),
            tail: ptr::null_mut(),
        }
    }

    pub fn push(&mut self, elem: T) {
        unsafe {
            let new_tail = Box::into_raw(Box::new(Node {
                elem: elem,
                next: ptr::null_mut(),
            }));
            
        }
    }
}
