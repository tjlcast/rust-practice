pub struct SelfRef {
    value: String,
}

pub fn example() {
    let s = "aaa".to_string();
    let v = SelfRef { value: s };
}

#[derive(Debug)]
pub struct WhatAboutThis<'a> {
    name: String,
    nickName: Option<&'a str>,
}

pub fn example1() {
    let mut tricky = WhatAboutThis {
        name: "Annablelle".to_string(),
        nickName: None,
    };
    tricky.nickName = Some("Annie");

    println!("{:?}", tricky);
}


#[derive(Debug)]
pub struct SelfRef2 {
    value: String,
    pointer_to_value: *const String,
}

impl SelfRef2 {
    pub fn new(txt: &str) -> Self {
        SelfRef2{
            value: String::from(txt),
            pointer_to_value: std::ptr::null(),
        }
    }

    pub fn init(&mut self) {
        let self_ref: *const String = &self.value;
        self.pointer_to_value = self_ref;
    }

    pub fn value(&self) -> &str {
        &self.value
    }

    pub fn pointer_to_value(&self) -> &String {
        assert!(!self.pointer_to_value.is_null());
        unsafe { &*(self.pointer_to_value)}
    }
}

pub fn example2 () {
    let mut t = SelfRef2::new("hello world");
    t.init();
    // 打印值和指针地址
    println!("{}, {:p}", t.value(), t.pointer_to_value());
}