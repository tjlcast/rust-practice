#[derive(Clone)]
struct Node {
    elem: i32,
    next: Option<Box<Node>>,
}

impl Drop for Node {
    fn drop(&mut self) {
        println!("Dropping node with element: {}", self.elem);
    }
}

struct List {
    head: Option<Box<Node>>,
}

impl List {
    pub fn new() -> Self {
        List { head: None }
    }

    pub fn push(&mut self, elem: i32) {
        let new_node = Box::new(Node {
            elem: elem,
            next: std::mem::replace(&mut self.head, None),
        });

        self.head = Some(new_node);
    }

    pub fn pop(&mut self) -> Option<i32> {
        match std::mem::replace(&mut self.head, None) {
            None => None,
            Option::Some(mut node) => {
                self.head = std::mem::replace(&mut node.next, None);
                Some(node.elem)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_list_push_base() {
        let mut list = List::new();

        assert_eq!(list.pop(), None);

        list.push(1);
        list.push(2);
        list.push(3);

        assert_eq!(list.pop(), Some(3));
        assert_eq!(list.pop(), Some(2));
        assert_eq!(list.pop(), Some(1));

        list.push(4);
        list.push(5);

        assert_eq!(list.pop(), Some(5));
        assert_eq!(list.pop(), Some(4));

        assert_eq!(list.pop(), None);
    }

    #[test]
    fn test_long_list_drop() {
        let mut list = List::new();

        for i in 0..1000 {
            list.push(i);
        }

        drop(list)
    }
}
