use actix_web::{App, HttpResponse, HttpServer, Responder, get, post, web};

use crate::api;

#[get("/")]
async fn hello() -> impl Responder {
    HttpResponse::Ok().body("Hello world!")
}

#[post("/echo")]
async fn echo(req_body: String) -> impl Responder {
    HttpResponse::Ok().body(req_body)
}

async fn manual_hello() -> impl Responder {
    HttpResponse::Ok().body("Hey there!")
}

// #[actix_web::main]
// async fn main() -> std::io::Result<()> {
//     HttpServer::new(|| {
//         App::new()
//             .service(hello)
//             .service(echo)
//             .route("/hey", web::get().to(manual_hello))
//     })
//     .bind(("127.0.0.1", 8080))?
//     .run()
//     .await
// }
// 将原来的 main 逻辑封装为 run 函数
pub async fn run() -> std::io::Result<()> {
    HttpServer::new(|| {
        App::new()
            .service(hello)
            .service(echo)
            .service(api::links::create_link)
            .service(api::links::get_from_link)
            .route("/hey", web::get().to(manual_hello))
    })
    .bind(("127.0.0.1", 8081))?
    .run()
    .await
}
