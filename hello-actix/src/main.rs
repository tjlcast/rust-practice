use hello_actix::app;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    app::run().await
}
