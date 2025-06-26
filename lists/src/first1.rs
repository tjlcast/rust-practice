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
            // Box 是一个包含私有字段的元组结构体，不能直接解构，只能通过解构 Option 来解构
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

/*
如果一个 `struct` 的成员既没有实现 `Clone` 也没有实现 `Copy`，那么直接使用 `=` 进行赋值会导致 **所有权的移动（move）**，而这在以下情况下会报错：

### **1. 如果 `self` 是引用（`&self` 或 `&mut self`）**
Rust 不允许从引用中直接移动所有权（因为这样会破坏引用的安全性）。例如：
```rust
struct Data {
    value: String, // String 没有 Copy，但实现了 Clone
}

impl Data {
    fn try_take(&self) {
        let v = self.value; // ❌ 错误！尝试移动 `self.value`，但 `self` 是 `&self`
    }
}
```
**错误信息：**
```
error[E0507]: cannot move out of `self.value` which is behind a shared reference
  --> src/main.rs:8:17
   |
8  |         let v = self.value;
   |                 ^^^^^^^^^^
   |                 |
   |                 move occurs because `self.value` has type `String`, which does not implement the `Copy` trait
   |                 help: consider borrowing here: `&self.value`
```
**解决方法：**
- **借用**（`&self.value`）而不是移动：
  ```rust
  let v = &self.value; // ✅ 借用，不移动
  ```
- **克隆**（如果类型实现了 `Clone`）：
  ```rust
  let v = self.value.clone(); // ✅ 克隆一份新的
  ```
- **使用 `std::mem::replace`**（如果 `&mut self` 可用）：
  ```rust
  fn try_take(&mut self) {
      let v = std::mem::replace(&mut self.value, String::new()); // ✅ 交换所有权
  }
  ```

---

### **2. 如果 `self` 是所有权（`self`）**
如果 `self` 是 **所有权**（而不是引用），则可以移动成员：
```rust
impl Data {
    fn take_ownership(self) -> String {
        self.value // ✅ 合法，因为 `self` 是所有权
    }
}
```
这里 `self.value` 被移动出去，调用后 `self` 不再可用。

---

### **3. 如果类型实现了 `Copy`**
如果成员实现了 `Copy`（如 `i32`, `bool`, `f64` 等），则 `=` 会自动复制，不会移动：
```rust
struct Data {
    num: i32, // i32 实现了 Copy
}

impl Data {
    fn copy_num(&self) {
        let n = self.num; // ✅ 合法，因为 `i32` 是 `Copy`，自动复制
    }
}
```

---

### **总结**
| 情况 | 能否 `let x = self.member` | 解决方案 |
|------|----------------|-----------|
| **成员没有 `Clone`/`Copy`，`self` 是引用（`&self`）** | ❌ 不能 | 改用 **借用**（`&self.member`）或 **`std::mem::replace`** |
| **成员没有 `Clone`/`Copy`，`self` 是所有权（`self`）** | ✅ 可以 | 直接移动 |
| **成员实现了 `Clone`** | ✅ 可以（需显式 `.clone()`） | `let x = self.member.clone()` |
| **成员实现了 `Copy`** | ✅ 可以（自动复制） | `let x = self.member` |

### **关键点**
- **移动（move）** 会转移所有权，导致原变量失效。
- **引用（`&`）** 不允许移动，只能借用或克隆。
- **`Copy` 类型**（如 `i32`）会自动复制，不会移动。
- **`Clone` 类型**（如 `String`）可以显式克隆。
- **`std::mem::replace`** 是处理 `&mut self` 时的常用技巧，可以安全地取出值并用新值替换。

如果你的 `struct` 成员既不能 `Clone` 也不能 `Copy`，并且你需要从 `&self` 或 `&mut self` 中取出它，通常需要重构代码，避免直接移动所有权。

*/
