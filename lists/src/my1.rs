use std::ptr::null_mut;

pub struct LinkedList<T> {
    head: *mut Node<T>,
}

struct Node<T> {
    elem: T,
    next: *mut Node<T>,
}

impl<T> LinkedList<T> {
    pub fn new() -> Self {
        LinkedList { head: null_mut() }
    }

    pub fn push_front(&mut self, elem: T) {
        let new_code = Box::new(Node {
            elem,
            next: self.head,
        });
        let raw = Box::into_raw(new_code);
        self.head = raw;
    }

    pub fn pop_front(&mut self) -> Option<T> {
        unsafe {
            if self.head.is_null() {
                return None;
            }

            let node = Box::from_raw(self.head);
            self.head = node.next;
            Some(node.elem)
        }
    }
}
