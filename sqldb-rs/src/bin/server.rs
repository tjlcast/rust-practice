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
                }
                Err(e) => {
                    println!("error on decoding from socket; error = {e:?}");
                }
            }
        }

        Ok(())
    }
}

// fn handle_request(line: &str, db: &Arc<Database>) -> Response {
//     let request = match Request::parse(line) {
//         Ok(req) => req,
//         Err(e) => return Response::Error { msg: e },
//     };

//     let mut db = db.map.lock().unwrap();
//     match request {
//         Request::Get { key } => match db.get(&key) {
//             Some(value) => Response::Value {
//                 key,
//                 value: value.clone(),
//             },
//             None => Response::Error {
//                 msg: format!("no key {key}"),
//             },
//         },
//         Request::Set { key, value } => {
//             let previous = db.insert(key.clone(), value.clone());
//             Response::Set {
//                 key,
//                 value,
//                 previous,
//             }
//         }
//     }
// }

// impl Request {
//     fn parse(input: &str) -> Result<Request, String> {
//         let mut parts = input.splitn(3, ' ');
//         match parts.next() {
//             Some("GET") => {
//                 let key = parts.next().ok_or("GET must be followed by a key")?;
//                 if parts.next().is_some() {
//                     return Err("GET's key must not be followed by anything".into());
//                 }
//                 Ok(Request::Get {
//                     key: key.to_string(),
//                 })
//             }
//             Some("SET") => {
//                 let key = match parts.next() {
//                     Some(key) => key,
//                     None => return Err("SET must be followed by a key".into()),
//                 };
//                 let value = match parts.next() {
//                     Some(value) => value,
//                     None => return Err("SET needs a value".into()),
//                 };
//                 Ok(Request::Set {
//                     key: key.to_string(),
//                     value: value.to_string(),
//                 })
//             }
//             Some(cmd) => Err(format!("unknown command: {cmd}")),
//             None => Err("empty input".into()),
//         }
//     }
// }

// impl Response {
//     fn serialize(&self) -> String {
//         match *self {
//             Response::Value { ref key, ref value } => format!("{key} = {value}"),
//             Response::Set {
//                 ref key,
//                 ref value,
//                 ref previous,
//             } => format!("set {key} = `{value}`, previous: {previous:?}"),
//             Response::Error { ref msg } => format!("error: {msg}"),
//         }
//     }
// }
