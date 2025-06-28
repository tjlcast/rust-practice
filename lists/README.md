## Tips

### take\replace

主要用于替代 std::mem::replace 函数

**Origin**
``` rust 
let mut cur_link = mem::replace(&mut self.head, None);
while let Some(mut boxed_node) = cur_link {
    cur_link = mem::replace(&mut boxed_node.next, None);
}
```
注意：replace的第一个参数是可变引用，第二个参数是可变引用的值，返回值是可变引用的值

**New**
``` rust
let mut cur_link = self.head.take();
while let Some(mut boxed_node) = cur_link {
    cur_link = boxed_node.next.take();
}

# or

boxed_node.next.replace(Box::new(Node { value: 1, next: None });
```
注意: take 与 replace 的区别在于，take 会将可变引用的值置为 None，而 replace 则是换为T类型的值

``` rust
let mut opt = Some(1);
let old = opt.replace(2);

// 等价于
use std::mem;
let mut opt = Some(1);
let old = mem::replace(&mut opt, Some(2));

```


### map

map 可用于替换 match 在类型上的转换，返回 Option<T> 类型

**Origin**
``` rust
pub fn pop(&mut self) -> Option<i32> {
    match self.head.take() {
        None => None,
        Some(node) => {
            self.head = node.next;
            Some(node.elem)
        }
    }
}
```

**New**
``` rust
pub fn pop(&mut self) -> Option<i32> {
    self.head.take().map(|node| {
        self.head = node.next;
        node.elem
    })
}
```

### as_ref

**prob**
``` rust
pub fn peek(&self) -> Option<&T> {
    self.head.map(|node| {
        &node.elem
    })
}
```
注意，这里的node是传值，消耗掉 `Box<Node<T>>` 中的所有权，由于你是通过 map(|node| ...) 捕获的 node，它其实是个临时值，一旦 map 结束，这个值就会被释放（也就是 Box<Node<T>> 被 drop），所以不能返回对它内部字段的引用。

**new**
``` rust
pub fn peek(&self) -> Option<&T> {
    self.head.as_ref().map(|node| &node.elem)
}
```
说明： 比较好的解决办法就是让 map 作用在引用上，而不是直接作用在 self.head 上，为此我们可以使用 Option 的 as_ref 方法。


### as_deref 
``` rust
// next: Option<&'a Node<T>>
fn next(&mut self) -> Option<Self::Item> {
    self.next.map(|node| {
        self.next = node.next.as_ref().map(|node| &*node); // 得到Box<Node<T>>
        &node.elem
    })
}
```

**New**
``` rust
fn next(&mut self) -> Option<Self::Item> {
    self.next.map(|node| {
        self.next = node.next.as_deref(); // 得到Node<T>
        // 或者
        // self.next = node.next.as_ref().map(|node| &**node);
        &node.elem
    })
}
```