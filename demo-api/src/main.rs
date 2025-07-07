use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use actix_web::{delete, get, post, web, App, HttpResponse, HttpServer, Responder};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]
struct User {
    id: u32,
    name: String,
}

type UserDB = Arc<Mutex<HashMap<u32, User>>>;

// GET / users - 获取所有用户
#[get("/users")]
async fn get_users(db: web::Data<UserDB>) -> impl Responder {
    let users = db.lock().unwrap();
    HttpResponse::Ok().json(users.values().cloned().collect::<Vec<User>>())
}

// GET / users / {id} - 获取指定用户
#[get("/users/{id}")]
async fn get_user(id: web::Path<u32>, db: web::Data<UserDB>) -> impl Responder {
    let mut users = db.lock().unwrap();
    match users.get(&id) {
        Some(user) => HttpResponse::Ok().json(user),
        None => HttpResponse::NotFound().body("User not fond"),
    }
}

// POST / users - 创建用户
#[post("/users")]
async fn create_user(user: web::Json<User>, db: web::Data<UserDB>) -> impl Responder {
    let mut users = db.lock().unwrap();
    let user_id = user.id;
    users.insert(user.id, user.into_inner());
    HttpResponse::Created().json(users.get(&user_id).unwrap())
}

// DELETE / users / {id} - 删除用户
#[delete("/users/{id}")]
async fn delete_user(id: web::Path<u32>, db: web::Data<UserDB>) -> impl Responder {
    let mut users = db.lock().unwrap();
    match users.remove(&id) {
        Some(_) => HttpResponse::Ok().json(format!("User {} deleted", id)),
        None => HttpResponse::NotFound().body("User not found"),
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // 初始化内存数据   库

    let user_db: UserDB = Arc::new(Mutex::new(HashMap::new()));

    // 插入测试数据
    user_db.lock().unwrap().insert(
        1,
        User {
            id: 1,
            name: "Alice".to_string(),
        },
    );

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(user_db.clone()))
            .service(get_users)
            .service(get_user)
            .service(create_user)
            .service(delete_user)
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
    // 启动服务器
}
