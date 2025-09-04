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
    stream: Option<TcpStream>,
    txn_version: Option<u64>,
}

impl Client {
    pub async fn new(addr: SocketAddr) -> Result<Self, Box<dyn Error>> {
        let stream = match TcpStream::connect(&addr).await {
            Ok(stream) => Some(stream),
            Err(e) => {
                eprintln!("Warning: Failed to connect to server: {}", e);
                None
            }
        };
        Ok(Self {
            addr,
            stream,
            txn_version: None,
        })
    }

    async fn reconnect(&mut self) -> Result<(), Box<dyn Error>> {
        match TcpStream::connect(&self.addr).await {
            Ok(stream) => {
                self.stream = Some(stream);
                println!("Successfully reconnected to {}", self.addr);
                Ok(())
            }
            Err(e) => {
                eprintln!("Failed to reconnect: {}", e);
                Err(e.into())
            }
        }
    }

    pub async fn execute_sql(&mut self, sql_cmd: &str) -> Result<(), Box<dyn Error>> {
        // 如果没有链接，尝试重新连接
        if self.stream.is_none() {
            println!("No connection, trying to reconnect...");
            self.reconnect().await?;
        }

        // 尝试发送命令
        let result = self.execute_sql_internal(sql_cmd).await;

        // 如果执行失败，尝试重连并再次执行
        if result.is_err() {
            eprintln!("Connection error, trying to reconnect...");
            self.reconnect().await?;
            return self.execute_sql_internal(sql_cmd).await;
        }

        result
    }

    async fn execute_sql_internal(&mut self, sql_cmd: &str) -> Result<(), Box<dyn Error>> {
        let stream = self.stream.as_mut().ok_or("No connection available")?;
        let (r, w) = stream.split();
        let mut sink = FramedWrite::new(w, LinesCodec::new());
        let mut stream = FramedRead::new(r, LinesCodec::new());

        // 发送命令并执行
        sink.send(sql_cmd).await?;

        // 拿到结果并打印
        while let Some(res) = stream.try_next().await? {
            if res == RESPONSE_END {
                break;
            }
            // 解析事务命令
            if res.starts_with("TRANSACTION") {
                let args = res.split(" ").collect::<Vec<_>>();
                if args[2] == "COMMIT" || args[2] == "ROLLBACK" {
                    self.txn_version = None;
                }
                if args[2] == "BEGIN" {
                    let version = args[1].parse::<u64>().unwrap();
                    self.txn_version = Some(version);
                }
            }
            println!("{}", res);
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let addr = env::args().nth(1).unwrap_or("127.0.0.1:8080".to_string());
    println!("Try to connect to {}", addr);

    let addr = addr.parse::<SocketAddr>()?;
    let mut client = Client::new(addr).await?;

    let mut editor = DefaultEditor::new()?;
    loop {
        let prompt = match client.txn_version {
            Some(version) => format!("sqldb[#{}]>", version),
            None => "sqldb>".into(),
        };
        let readline = editor.readline(&prompt);
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
