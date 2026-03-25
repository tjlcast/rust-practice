use anyhow::{anyhow, Result};

// fn main() {
//     println!("Hello, world!");
// }

use stream_client_rs::app;

#[tokio::main]
async fn main() -> Result<()> {
    app::run().await
}
