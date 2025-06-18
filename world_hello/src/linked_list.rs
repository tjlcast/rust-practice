use std::rc::Rc;

pub fn example1() {
    let five = Rc::new(5);

    let weak_five = Rc::downgrade(&five);

    let strong_five: Option<Rc<_>> = weak_five.upgrade();
    assert_eq!(*strong_five.unwrap(), 5);

    drop(five);

    let strong_five: Option<Rc<_>> = weak_five.upgrade();
    assert_eq!(strong_five, None);
}
