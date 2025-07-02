use std::ptr::{NonNull, null_mut};

/*
Rust 的内存安全模型不允许两个可变引用或任意的悬垂引用，
但链表中节点互相引用（next 和 prev）天然具有这种双向的循环结构，
在安全 Rust 中几乎无法实现一个真正高性能的双向链表（不引入 Rc<RefCell<_>> 或类似结构）

因此我们：
    使用 *mut T 原始裸指针打破借用检查器；
    用 Box<T> 控制堆上内存分配和回收；
    手动实现链表操作逻辑和生命周期。
 */

// 使用 *mut Node<T> 作为 next 和 prev 类型，意味着我们完全控制指针操作；
type Link<T> = *mut Node<T>;

pub struct DoublyLinkedList<T> {
    head: Link<T>,
    tail: Link<T>,
}

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

    /*
    Box::new(...) 会在堆上创建一个节点；
    Box::into_raw(...) 会转成裸指针，我们负责后续手动回收；
    然后更新链表头部逻辑：
     */
    pub fn push_front(&mut self, elem: T) {
        // 消耗掉 Box<T> 的所有权，返回它在堆上分配的裸指针（*mut T）
        // 内存不再自动释放
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

    /*
    Box::from_raw 恢复 Box 所有权会自动回收节点；
    如果链表空了，head = null_mut，此时也要设置 tail = null_mut;
     */
    pub fn pop_front(&mut self) -> Option<T> {
        unsafe {
            let node = NonNull::new(self.head)?;
            // 执行 Box::from_raw(raw) 时，向编译器“声明”：我重新拥有这块内存的所有权；
            // 所以这个 Box<T> 是新的所有者，会在其生命周期结束时 drop 掉这块内存；
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

impl<T> Drop for DoublyLinkedList<T> {
    fn drop(&mut self) {
        unsafe { while let Some(_) = self.pop_front() {} }
    }
}

// 这个 Iter<'a, T> 在逻辑上持有一个 &'a T（只读借用），以便借用检查器知道它活多久、不能提前释放原始数据。
pub struct Iter<'a, T> {
    next: *const Node<T>,
    _marker: std::marker::PhantomData<&'a T>,
}

pub struct IterMut<'a, T> {
    next: *mut Node<T>,
    _marker: std::marker::PhantomData<&'a mut T>,
}

impl<T> DoublyLinkedList<T> {
    // 这个 '_' 就需要匹配 Iter<'a, T> 中的 'a，你结构体里必须带生命周期参数 'a，否则无法关联返回值和 &self 的生命周期。
    // 这个 'a 生命周期就是其引用的元素，保证了迭代器活多久，元素就活多久，不会出现悬停。
    // 如果没有 'a 生命周期，就不能实现这个方法签名。
    // 换句话说，Iter 的生命周期不能超过它所引用的链表数据的生命周期。
    pub fn iter(&self) -> Iter<'_, T> {
        // 这里，'_ 表示返回的 Iter 的生命周期与 &self 的生命周期相同。
        Iter {
            next: self.head,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn iter_mut(&mut self) -> IterMut<'_, T> {
        IterMut {
            next: self.head,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        unsafe {
            // 这里用 as_ref() 将裸指针转为 Option<&T>；
            // 这里的 'a 生命周期是怎么确定的？它来源于你 Iter<'a, T> 的定义中的 'a；就是通过self传入的。
            // 所以这个 &'a T 是一个手动标注的借用：你告诉编译器：“我正在使用一块 'a 生命周期内的内存”。
            self.next.as_ref().map(|node| {
                let val = &node.elem;
                self.next = node.next;
                val
            })
        }
    }
}

impl<'a, T> Iterator for IterMut<'a, T> {
    type Item = &'a mut T;

    fn next(&mut self) -> Option<Self::Item> {
        unsafe {
            self.next.as_mut().map(|node| {
                let val = &mut node.elem;
                self.next = node.next;
                val
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::DoublyLinkedList;

    #[test]
    fn test_doubly_linked_list_operations() {
        let mut list = DoublyLinkedList::new();

        // Test push operations
        list.push_front(2);
        list.push_front(1);
        list.push_back(3);
        list.push_back(4);

        // Test iteration
        let forward_values: Vec<_> = list.iter().collect();
        assert_eq!(forward_values, vec![&1, &2, &3, &4]);

        // Test mutable iteration and modification
        for val in list.iter_mut() {
            *val += 10;
        }

        // Verify modifications
        let modified_values: Vec<_> = list.iter().collect();
        assert_eq!(modified_values, vec![&11, &12, &13, &14]);

        // Test pop operations
        assert_eq!(list.pop_front(), Some(11));
        assert_eq!(list.pop_back(), Some(14));

        // Verify remaining elements
        let remaining_values: Vec<_> = list.iter().collect();
        assert_eq!(remaining_values, vec![&12, &13]);
    }
}
