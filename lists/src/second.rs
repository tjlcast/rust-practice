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

    pub fn peek(&self) -> Option<&T> {
        // E0507：map 需要获取 Option 内部值的所有权（移动 self.head），但 self 是共享引用 (&self)，不允许移动。
        // E0515：即使能移动，返回的 &node.elem 也指向闭包内的局部变量 node，闭包结束后 node 会被销毁，导致悬垂引用。
        // self.head.map(|node| {
        //     &node.elem
        // })
        self.head.as_ref().map(|node| &node.elem)
    }

    pub fn peek_mut(&mut self) -> Option<&mut T> {
        self.head.as_mut().map(|node| &mut node.elem)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peek() {
        let mut list = List::new();
        assert_eq!(list.peek(), None);
        assert_eq!(list.peek_mut(), None);
        list.push(1);
        list.push(2);
        list.push(3);

        assert_eq!(list.peek(), Some(&3));
        assert_eq!(list.peek_mut(), Some(&mut 3));

        list.peek_mut().map(|value| *value = 42);

        assert_eq!(list.peek(), Some(&42));
        assert_eq!(list.pop(), Some(42));
    }
}
