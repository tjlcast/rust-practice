#[cfg(test)]
mod tests {
    #[test]
    fn test_iterator() {
        let arr = vec![1, 2, 34, 2];
        for v in arr {
            println!("Value: {}", v);
        }

        // into_iter 会move原始变量
        // for v in arr {
        //     println!("Value: {}", v);
        // }
    }

    #[test]
    fn test_iterator_with_ref() {
        let arr = vec![1, 2, 34, 2];
        for v in &arr {
            println!("Value: {}", v);
        }

        // 这里可以再次使用 arr，因为我们是通过引用来迭代的
        for v in &arr {
            println!("Value again: {}", v);
        }
    }

    #[test]
    fn test_iterator_with_mut_ref() {
        let mut arr = vec![1, 2, 3, 4];
        for v in &mut arr {
            *v += 1; // 修改每个元素
            println!("Value after increment: {}", v);
        }

        // 再次使用 arr，注意这里的 arr 已经被修改了
        for v in &arr {
            println!("Value after modification: {}", v);
        }
    }

    #[test]
    fn test_iterator_with_enumerate() {
        let arr = vec![1, 2, 3, 4];
        for (index, value) in arr.iter().enumerate() {
            println!("Index: {}, Value: {}", index, value);
        }

        // enumerate 返回的是一个迭代器，包含索引和值
        // 可以再次使用 arr，因为我们是通过引用来迭代的
        for (index, value) in arr.iter().enumerate() {
            println!("Index again: {}, Value again: {}", index, value);
        }
    }

    #[test]
    fn test_iterator_with_map() {
        let arr = vec![1, 2, 3, 4];
        let doubled: Vec<i32> = arr.iter().map(|x| x * 2).collect();
        for value in doubled {
            println!("Doubled Value: {}", value);
        }

        // map 返回的是一个新的迭代器，原始的 arr 不受影响
        for value in &arr {
            println!("Original Value: {}", value);
        }

        arr.iter().for_each(|x| println!("{}", x));
    }

    #[test]
    fn test_iterator_with_filter() {
        let arr = vec![1, 2, 3, 4, 5];
        let even_numbers: Vec<i32> = arr.iter().filter(|&&x| x % 2 == 0).copied().collect();
        for value in even_numbers {
            println!("Even Value: {}", value);
        }

        // filter 返回的是一个新的迭代器，原始的 arr 不受影响
        for value in &arr {
            println!("Original Value: {}", value);
        }
    }
}
