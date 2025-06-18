use std::error::Error;
use std::fs;

#[derive(Debug)]
pub struct Config {
    _query: String,
    file_path: String,
}

impl Config {
    pub fn new(args: &[String]) -> Config {
        let query = args[0].clone();
        let file_path = args[1].clone();

        Config {
            _query: query,
            file_path,
        }
    }

    pub fn build(args: &[String]) -> Result<Config, &'static str> {
        if args.len() < 2 {
            return Err("not enough arguments");
        }

        let query = args[0].clone();
        let file_path = args[1].clone();

        Ok(Config {
            _query: query,
            file_path,
        })
    }
}

pub fn run(config: Config) -> Result<(), Box<dyn Error>> {
    // let contents =
    //     fs::read_to_string(config.file_path).expect("Should have been able to read the file");
    let contents = fs::read_to_string(config.file_path)?;

    println!("write content is: {contents}");

    for line in search(&config._query, &contents) {
        println!("{line}.");
    }

    Ok(())
}

pub fn search<'a>(query: &str, contents: &'a str) -> Vec<&'a str> {
    let mut items = Vec::new();

    for line in contents.lines() {
        if line.contains(query) {
            items.push(line);
        }
    }
    return items;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_result() {
        let query = "duct";
        let contents = "\
Rust:
safe, fast, productive.
Pick three.";

        assert_eq!(vec!["safe, fast, productive."], search(query, contents));
    }
}
