use futures::{SinkExt, TryStreamExt};
use std::{error::Error, net::SocketAddr};
use tokio::net::TcpStream;
use tokio_util::codec::FramedRead;
use tokio_util::codec::{FramedWrite, LinesCodec};

use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;

use std::env;

const RESPONSE_END: &str = "!!!end!!!";

pub struct Client {
    addr: SocketAddr,
}

impl Client {
    pub fn new(addr: SocketAddr) -> Self {
        Self { addr }
    }

    pub async fn execute_sql(&self, sql_cmd: &str) -> Result<(), Box<dyn Error>> {
        let mut stream = TcpStream::connect(&self.addr).await?;
        let (r, w) = stream.split();
        let mut sink = FramedWrite::new(w, LinesCodec::new());
        let mut stream = FramedRead::new(r, LinesCodec::new());

        // 发送命令并执行
        sink.send(sql_cmd).await?;

        // 拿到结果并打印
        while let Some(val) = stream.try_next().await? {
            if val == RESPONSE_END {
                break;
            }
            println!("{}", val);
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let addr = env::args().nth(1).unwrap_or("127.0.0.1:8080".to_string());
    println!("Try to connect to {}", addr);

    let addr = addr.parse::<SocketAddr>()?;
    let client = Client::new(addr);

    let mut editor = DefaultEditor::new()?;
    loop {
        let readline = editor.readline("sqldb> ");
        match readline {
            Ok(sql_cmd) => {
                let sql_cmd = sql_cmd.trim();
                if sql_cmd.len() > 0 {
                    if sql_cmd == "exit" || sql_cmd == "quit" {
                        break;
                    }
                    editor.add_history_entry(sql_cmd)?;
                    client.execute_sql(sql_cmd).await?;
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }

    Ok(())
}
