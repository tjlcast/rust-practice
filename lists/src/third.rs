use std::rc::Rc;

pub struct List<T> {
    head: Link<T>,
}

type Link<T> = Option<Rc<Node<T>>>;

struct Node<T> {
    elem: T,
    next: Link<T>,
}

impl<T> List<T> {
    pub fn new() -> Self {
        List { head: None }
    }

    /*
        let list = List::new();       // List<i32>
        let list_ref = &list;         // &List<i32>

        // 情况1：直接调用（自动引用）
        // 可以这样调用（自动引用 Deref）：
        let new_list = list_ref.prepend(1);  // 等价于 (&list_ref).prepend(1)

        // 情况2：显式解引用
        let new_list = (*list_ref).prepend(1);  // 先解引用再调用

        关键点在于：
            prepend 方法接收 &self（不可变引用）
            Rust 的方法调用会自动根据需要添加引用/解引用
            对引用再取引用仍然是合法的（&&List<T> 会自动解引用为 &List<T>）

    */
    pub fn prepend(&self, elem: T) -> List<T> {
        List {
            head: Some(Rc::new(Node {
                elem,
                next: self.head.clone(),
            })),
        }
    }

    pub fn tail(&self) -> List<T> {
        List {
            /*
            node.next 的访问：
                通过 Rc 的自动解引用 (Deref trait)，可以直接访问 Node 的字段
                node.next ⇒ 类型为 Option<Rc<Node<T>>>

            .clone() 操作：
                Option 实现了 Clone trait
                对 Option<Rc<Node<T>>> 调用 .clone() 会：
                    如果是 None ⇒ 返回 None (仍是 Option<Rc<Node<T>>>)
                    如果是 Some(rc) ⇒ 克隆 Rc (引用计数+1) ⇒ 返回 Some(new_rc)
             */
            head: self.head.as_ref().and_then(|node| node.next.clone()),
        }
    }

    pub fn head(&self) -> Option<&T> {
        self.head.as_ref().map(|node| &node.elem)
    }
}

impl<T> Drop for List<T> {
    fn drop(&mut self) {
        let mut head = self.head.take();
        while let Some(node) = head {
            // 方法 Rc::Try_unwrap ，该方法会判断当前的 Rc 是否只有一个强引用，若是，则返回 Rc 持有的值，否则返回一个错误
            if let Ok(mut node) = Rc::try_unwrap(node) {
                head = node.next.take();
            } else {
                break;
            }
        }
    }

    // fn drop(&mut self) {
    //     let mut cur_link = self.head.take();
    //     while let Some(mut boxed_node) = cur_link {
    //         /*
    //         但通过 Rc<Node> 只能获得 &Node（不可变引用）
    //         无法获取 &mut Node，因为：
    //             Rc 不提供内部可变性
    //             可能有其他 Rc 副本同时存在（违反 Rust 的可变引用独占规则）
    //
    //         共享所有权：Rc 允许多个指针指向同一个数据
    //         不可变性：Rc 只提供了共享引用（&T），不提供可变引用（&mut T），除非使用内部可变性类型如 RefCell
    //         无法获取内部所有权：你不能像 Box 那样使用 take() 来获取 Rc 内部值的所有权，因为可能有其他 Rc 也在引用这个值
    //          */
    //         cur_link = boxed_node.next.take();
    //     }
    // }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basics() {
        let list = List::new();
        assert_eq!(list.head(), None);

        let list = list.prepend(1).prepend(2).prepend(3);
        assert_eq!(list.head(), Some(&3));

        let list = list.tail();
        assert_eq!(list.head(), Some(&2));

        let list = list.tail();
        assert_eq!(list.head(), Some(&1));

        let list = list.tail();
        assert_eq!(list.head(), None);
    }
}

pub struct Iter<'a, T> {
    next: Option<&'a Node<T>>,
}

impl<T> List<T> {
    pub fn iter(&self) -> Iter<'_, T> {
        Iter {
            next: self.head.as_deref(),
        }
    }
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        self.next.map(|node| {
            self.next = node.next.as_deref();
            &node.elem
        })
    }
}

#[cfg(test)]
mod test1 {
    use super::*;

    #[test]
    fn iter() {
        let list = List::new().prepend(1).prepend(2).prepend(3);

        let mut iter = list.iter();
        assert_eq!(iter.next(), Some(&3));
        assert_eq!(iter.next(), Some(&2));
        assert_eq!(iter.next(), Some(&1));
    }
}
