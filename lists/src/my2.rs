use std::ptr::{NonNull, null_mut};

pub struct DoublyLinkedList<T> {
    head: Link<T>,
    tail: Link<T>,
}

type Link<T> = *mut Node<T>;

struct Node<T> {
    elem: T,
    next: Link<T>,
    prev: Link<T>,
}

impl<T> DoublyLinkedList<T> {
    pub fn new() -> Self {
        Self {
            head: null_mut(),
            tail: null_mut(),
        }
    }

    pub fn push_front(&mut self, elem: T) {
        let node = Box::into_raw(Box::new(Node {
            elem: elem,
            next: self.head,
            prev: null_mut(),
        }));

        unsafe {
            if !self.head.is_null() {
                (*self.head).prev = node;
            } else {
                self.tail = node;
            }

            self.head = node;
        }
    }

    pub fn push_back(&mut self, elem: T) {
        let node = Box::into_raw(Box::new(Node {
            elem,
            next: null_mut(),
            prev: self.tail,
        }));

        unsafe {
            if !self.tail.is_null() {
                (*self.tail).next = node;
            } else {
                self.head = node;
            }

            self.tail = node;
        }
    }

    pub fn pop_front(&mut self) -> Option<T> {
        unsafe {
            let node = NonNull::new(self.head)?;
            let boxed = Box::from_raw(node.as_ptr());
            self.head = boxed.next;

            if !self.head.is_null() {
                (*self.head).prev = null_mut();
            } else {
                self.tail = null_mut();
            }

            Some(boxed.elem)
        }
    }

    pub fn pop_back(&mut self) -> Option<T> {
        unsafe {
            let node = NonNull::new(self.tail)?;
            let boxed = Box::from_raw(node.as_ptr());
            self.tail = boxed.prev;

            if !self.tail.is_null() {
                (*self.tail).next = null_mut();
            } else {
                self.head = null_mut();
            }

            Some(boxed.elem)
        }
    }
}

pub struct Iter<'a, T> {
    next: *const Node<T>,
    _marker: std::marker::PhantomData<&'a T>,
}

pub struct IterMut<'a, T> {
    next: *mut Node<T>,
    _marker: std::marker::PhantomData<&'a mut T>,
}
