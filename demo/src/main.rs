struct WordIterator<'s> {
    position: usize,
    string: &'s str,
}

impl<'s> WordIterator<'s> {
    fn new(string: &'s str) -> WordIterator<'s> {
        WordIterator {
            position: 0,
            string,
        }
    }

    fn next_word(&mut self) -> Option<&str> {
        let start_of_word = &self.string[self.position..];
        let index_of_next_space = start_of_word.find(' ').unwrap_or(start_of_word.len());
        if start_of_word.len() != 0 {
            self.position += index_of_next_space + 1;
            return Some(&start_of_word[..index_of_next_space]);
        } else {
            return None;
        }
    }
}

fn main() {
    let text = String::from("Twas brillig, and the slithy toves // Did gyre and gimble in the wabe: // All mimsy were the borogoves, // And the mome raths outgrabe.");
    let mut word_iterator = WordIterator::new(&text);

    let word_a = word_iterator.next_word();
    assert_eq!(word_a, Some("Twas"));
    let word_b = word_iterator.next_word();
    assert_eq!(word_b, Some("brillig,"));
}
