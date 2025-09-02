use sqldb_rs::sql;
use sqldb_rs::sql::engine::kv::KVEngine;
use sqldb_rs::storage::disk::DiskEngine;
use tokio::net::{TcpListener, TcpStream};
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec};

use futures::SinkExt;
use std::env;
use std::sync::{Arc, Mutex, MutexGuard};

use sqldb_rs::error::Result;

const DB_PATH: &str = "123";
const RESPONSE_END: &str = "!!!end!!!";

/// Possible requests our client can send us
enum SqlRequest {
    SQL(String),
    ListTables,
    TableInfo(String),
}

#[tokio::main]
async fn main() -> Result<()> {
    // 配置
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8080".to_string());

    // 初始化 TCP 服务
    let listener = TcpListener::bind(&addr).await?;
    println!("sqldb server start on, listening on: {addr}");

    // 初始化 DB 实例
    let p = tempfile::tempdir()?.into_path().join("sqldb-log");
    println!("sqldb store int path: {p:?}");
    let kvengine = KVEngine::new(DiskEngine::new(p.clone())?);
    let shared_engine = Arc::new(Mutex::new(kvengine));

    loop {
        match listener.accept().await {
            Ok((socket, _)) => {
                let db = shared_engine.clone();
                let mut server_session = ServerSession::new(db.lock()?)?;

                tokio::spawn(async move {
                    match server_session.handle_request(socket).await {
                        Ok(_) => todo!(),
                        Err(_) => todo!(),
                    }
                });
            }
            Err(e) => println!("error accepting socket; error = {e:?}"),
        }
    }
}

pub struct ServerSession<E: sql::engine::Engine> {
    session: sql::engine::Session<E>,
}

// tokio::spawn 需要保证任务中使用的所有数据在任务执行期间都有效。
// 由于异步任务可能在任意时间执行，Rust 要求所有捕获的
// 数据都是 'static 的（要么是拥有的数据，要么是静态引用）。
// tips: tokio::spawn 要求的是：任务捕获的所有数据必须能够独立存在，不依赖于外部作用域。(不在其他作用域中)
impl<E: sql::engine::Engine + 'static> ServerSession<E> {
    pub fn new(eng: MutexGuard<E>) -> Result<Self> {
        Ok(Self {
            session: eng.session()?,
        })
    }

    pub async fn handle_request(&mut self, socket: TcpStream) -> Result<()> {
        let mut lines = Framed::new(socket, LinesCodec::new());

        while let Some(result) = lines.next().await {
            match result {
                Ok(line) => {
                    // 解析并得到 SqlResquest
                    let req = SqlRequest::SQL(line);

                    // 执行请求
                    let res = match req {
                        SqlRequest::SQL(sql) => self.session.execute(&sql),
                        SqlRequest::ListTables => todo!(),
                        SqlRequest::TableInfo(_) => todo!(),
                    };

                    // 发送执行结果
                    let response = match res {
                        Ok(rs) => rs.to_string(),
                        Err(e) => e.to_string(),
                    };
                    if let Err(e) = lines.send(response.as_str()).await {
                        println!("error on sending response; error = {e:?}");
                    }

                    // 发送结束标志
                    if let Err(e) = lines.send(RESPONSE_END).await {
                        println!("error on sending response; error = {e:?}");
                    }
                }
                Err(e) => {
                    println!("error on decoding from socket; error = {e:?}");
                }
            }
        }

        Ok(())
    }
}
