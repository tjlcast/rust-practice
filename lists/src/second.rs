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

pub struct IntoIter<T>(List<T>);

impl<T> List<T> {
    pub fn into_iter(self) -> IntoIter<T> {
        IntoIter(self)
    }
}

impl<T> Iterator for IntoIter<T> {
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        self.0.pop()
    }
}

pub struct Iter<'a, T> {
    next: Option<&'a Node<T>>,
}

impl<T> List<T> {
    pub fn iter<'a>(&'a self) -> Iter<'a, T> {
        Iter {
            // next: self.head.as_ref().map(|node| &*node),
            // 失败：这个是因为编译器无法推断出最后的类型，
            // self.head: Option<Box<Node<T>>>
            // self.head.as_ref: Option<&Box<Node<T>>>
            // node: &Box<Node<T>>
            // *node: Node<T>，但是编译器认为是 Box<Node<T>>
            // &*node: &Node<T>
            next: self.head.as_ref().map(|node: &Box<Node<T>>| &**node),
            // next: self.head.as_deref(),
        }
    }
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = &'a T;
    fn next(&mut self) -> Option<Self::Item> {
        self.next.map(|node| {
            // 另一种可以的写法
            // self.next = node.next.as_ref().map(|node| &**node);

            self.next = node.next.as_deref();
            &node.elem
        })
    }
}

pub struct IterMut<'a, T> {
    next: Option<&'a mut Node<T>>,
}

impl<T> List<T> {
    pub fn iter_mut<'a>(&mut self) -> IterMut<'_, T> {
        IterMut {
            next: self.head.as_deref_mut(),
        }
    }
}

impl<'a, T> Iterator for IterMut<'a, T> {
    type Item = &'a mut T;
    fn next(&mut self) -> Option<Self::Item> {
        // 在map的调用的时候，会把Option中的类型进行传参（copy）
        // Option 和不可变引用 &T 是可以 Copy
        // 可变引用 &mut T 不可以 Copy
        self.next.take().map(|node| {
            self.next = node.next.as_deref_mut();
            &mut node.elem
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn into_iter() {
        let mut list = List::new();
        list.push(1);
        list.push(2);
        list.push(3);

        let mut iter = list.into_iter();
        assert_eq!(iter.next(), Some(3));
        assert_eq!(iter.next(), Some(2));
        assert_eq!(iter.next(), Some(1));
        assert_eq!(iter.next(), None);
    }

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

        // 实际上 &mut value 是一个模式匹配，
        // 它用 &mut value 模式去匹配一个可变的引用，
        // 此时匹配出来的 value 显然是一个值，而不是可变引用
        // list.peek_mut().map(|&mut value| value = 42);

        assert_eq!(list.peek(), Some(&42));
        assert_eq!(list.pop(), Some(42));
    }

    #[test]
    fn iter() {
        let mut list = List::new();
        list.push(1);
        list.push(2);
        list.push(3);

        let mut iter = list.iter();
        assert_eq!(iter.next(), Some(&3));
        assert_eq!(iter.next(), Some(&2));
        assert_eq!(iter.next(), Some(&1));
    }

    #[test]
    fn iter_mut() {
        let mut list = List::new();
        list.push(1);
        list.push(2);
        list.push(3);

        let mut iter = list.iter_mut();
        assert_eq!(iter.next(), Some(&mut 3));
        assert_eq!(iter.next(), Some(&mut 2));
        assert_eq!(iter.next(), Some(&mut 1));
    }
}

// run test: cargo test --test-threads=1 --lib -- --nocapture
// run test: cargo test --test-threads=1 --lib -- --show-output