enum Direction {
    East,
    West,
    North,
    South,
}

fn apply() {
    let direction = Direction::South;

    match direction {
        Direction::East => println!("East"),
        Direction::West | Direction::North => {
            println!("West or North");
        },
        _ => println!("South"),
    }

    println!("hello match demo");
}