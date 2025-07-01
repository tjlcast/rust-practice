pub mod first;
pub mod first1;
pub mod my1;
pub mod my2;
pub mod second;
pub mod third;
pub mod third1;
pub mod unsafe_code;
pub mod unsafe_list;

pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
