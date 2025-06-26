/*
1、Option::take() 是一个非常有用的方法，它的功能是 取出 Option 中的值并原地替换为 None，
同时返回被取出的原值。它的作用类似于 std::mem::replace(&mut option, None)，但更简洁和安全。

2、match option { None => None, Some(x) => Some(y) } 这段代码可以直接使用 map 方法代替，
map 会对 Some(x) 中的值进行映射，最终返回一个新的 Some(y) 值。

*/

pub struct List<T> {
    head: Link<T>,
}

type Link<T> = Option<Box<Node<T>>>;

struct Node<T> {
    elem: T,
    next: Link<T>,
}

impl<T> List<T> {
    pub fn new() -> Self {
        List { head: None }
    }

    pub fn push(&mut self, elem: T) {
        let new_node = Box::new(Node {
            elem: elem,
            next: self.head.take(),
        });

        self.head = Some(new_node);
    }

    pub fn pop(&mut self) -> Option<T> {
        self.head.take().map(|node| {
            self.head = node.next;
            node.elem
        })
    }
}

impl<T> Drop for List<T> {
    fn drop(&mut self) {
        let mut cur_node = self.head.take();
        while let Some(mut node) = cur_node {
            cur_node = node.next.take();
        }
    }
}
