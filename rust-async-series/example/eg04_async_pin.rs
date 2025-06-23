struct SelfRef {
    name: String,
    name_ref: *const String,
}

impl SelfRef {
    /**
     * 这个new会在 --release 模式下出现错误;
     * 本质: 其中 name_ref 绑定到栈对象 ret 的 name 地址上，new 方法结束后返回栈对象会 move 到新的栈对象（ownship转移）。这时候 name_ref 的值仍然是原来的地址，故失败。
     * 原因是: 在 --release 模式下，#[inline(always)]会"强烈建议"编译器进行内联优化（不一定），将 `SelfRef::new` 的实现直接嵌入到调用点；
     */
    #[inline(always)]
    fn new(name: impl Into<String>) -> Self {
        let mut ret = Self {
            name: name.into(),
            name_ref: std::ptr::null(),
        };
        ret.name_ref = &ret.name;
        ret
    }
}

fn main() {
    let s1 = SelfRef::new("hello");
    let s2 = SelfRef::new("world");

    println!("s1: name: {}, name_ref: {}", s1.name, unsafe {
        &*s1.name_ref
    });
    println!("s2: name: {}, name_ref: {}", s2.name, unsafe {
        &*s2.name_ref
    });
}
