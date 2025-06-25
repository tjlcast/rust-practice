struct Counter {
    v: usize,
    count: usize,
}

impl Counter {
    fn new(v: usize) -> Counter {
        Counter { count: 0, v }
    }
}

impl Iterator for Counter {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        self.count += 1;

        if self.count > self.v {
            None
        } else {
            Some(self.count)
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_control_flow() {
        let number = 0;

        if number % 2 == 0 {
            println!("{} is even", number);
        } else {
            println!("{} is odd", number);
        }

        let result = match number {
            0 => "Zero",
            1 => "One",
            2 => "Two",
            _ => "Greater than Two",
        };
        println!("The number is: {}", result);

        let mut counter = 0;
        while counter < 5 {
            println!("Counter: {}", counter);
            counter += 1;
        }

        for i in 0..5 {
            println!("For loop iteration: {}", i);
        }

        let mut c = 1;
        loop {
            if c > 5 {
                break;
            }
            c += 1;
            println!("Loop iteration: {}", c);
        }
    }

    #[test]
    fn test_counter() {
        let mut counter = super::Counter::new(5);
        while let Some(value) = counter.next() {
            println!("Counter value: {}", value);
        }
    }
}
