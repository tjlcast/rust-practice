struct Rectangle {
    width: u32,
    height: u32,
}

enum Direction {
    Up(Option<i32>),
    Down(Option<i32>),
    Left(Option<i32>),
    Right(Option<i32>),
}

impl Direction {
    fn value(&self) -> Option<i32> {
        match self {
            Direction::Up(v) | Direction::Down(v) | Direction::Left(v) | Direction::Right(v) => *v,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*; // 引入父模块的 add 函数

    #[test]
    fn test_main() {
        let rect = Rectangle {
            width: 30,
            height: 50,
        };

        println!("Rectangle width: {}, height: {}", rect.width, rect.height);

        let up_d = Direction::Up(Some(10));
        show_direction(up_d);
        let down_d = Direction::Down(None);
        show_direction(down_d);
        let left_d = Direction::Left(Some(-1));
        show_direction(left_d);
        let right_d = Direction::Right(Some(100));
        show_direction(right_d);

        let up_d_none = Direction::Up(None);
        up_d_none.value().map_or_else(
            || println!("No value for Up directi1on"),
            |v| println!("Up direction value: {}", v),
        );
        let a = up_d_none.value().unwrap_or(0);
        println!("Up direction value with unwrap_or: {}", a);
    }

    fn show_direction(direction: Direction) {
        if let Direction::Up(Some(value)) = direction {
            println!("Moving up by {} units", value);
        } else if let Direction::Down(Some(value)) = direction {
            println!("Moving down by {} units", value);
        } else if let Direction::Left(Some(value)) = direction {
            println!("Moving left by {} units", value);
        } else if let Direction::Right(Some(value)) = direction {
            println!("Moving right by {} units", value);
        } else if let Direction::Up(None) = direction {
            println!("Moving up with no specific value");
        } else if let Direction::Down(None) = direction {
            println!("Moving down with no specific value");
        } else if let Direction::Left(None) = direction {
            println!("Moving left with no specific value");
        } else if let Direction::Right(None) = direction {
            println!("Moving right with no specific value");
        } else {
            println!("Unknown direction");
        }
    }
}
