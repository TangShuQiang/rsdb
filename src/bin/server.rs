use futures::SinkExt;
use rsdb::error::RSDBResult;
use rsdb::sql;
use rsdb::sql::engine::kv::KVEngine;
use rsdb::storage::disk::DiskEngine;
use tokio::net::{TcpListener, TcpStream};
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec};

use std::env;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};

// cargo run --bin server
const DB_PATH: &str = "/tmp/rsdb-test/redb-log";

enum SqlRequest {
    SQL(String),
    ListTables,
    TableInfo(String),
}

pub struct ServerSession<E: sql::engine::Engine> {
    session: sql::engine::Session<E>,
}

impl<E: sql::engine::Engine + 'static> ServerSession<E> {
    pub fn new(eng: MutexGuard<E>) -> RSDBResult<Self> {
        Ok(Self {
            session: eng.session()?,
        })
    }

    pub async fn handle_request(&mut self, socket: TcpStream) -> RSDBResult<()> {
        let mut lines = Framed::new(socket, LinesCodec::new());
        while let Some(result) = lines.next().await {
            match result {
                Ok(line) => {
                    // 解析并得到 SqlRequest
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
                        println!("error on sending response; error = {:?}", e);
                    }
                }
                Err(e) => {
                    println!("error on receiving line; error = {:?}", e);
                }
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> RSDBResult<()> {
    // 启动 TCP 服务
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8080".to_string());
    let listener = TcpListener::bind(&addr).await?;
    println!("rsdb server started, listening on {}", addr);
    // 初始化 DB
    let p = PathBuf::from(DB_PATH);
    let kvengine = KVEngine::new(DiskEngine::new(p.clone())?);
    let shared_engine = Arc::new(Mutex::new(kvengine));
    loop {
        match listener.accept().await {
            Ok((socket, _)) => {
                let db = shared_engine.clone();
                let mut ss = ServerSession::new(db.lock()?)?;
                tokio::spawn(async move {
                    match ss.handle_request(socket).await {
                        Ok(_) => {}
                        Err(_) => todo!(),
                    }
                });
            }
            Err(e) => {
                println!("error on accepting connection; error = {:?}", e);
            }
        }
    }
}
