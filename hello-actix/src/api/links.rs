use actix_web::{HttpResponse, Responder, get, http::header, post, web::Json};
use nanoid::nanoid;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Link {
    tiny_code: String,
    origin_url: String,
}

#[derive(Deserialize, Clone)]
struct ApiAddLink {
    origin_url: String,
}

impl ApiAddLink {
    fn to_new_link(self) -> Link {
        Link {
            tiny_code: nanoid!(5),
            origin_url: self.origin_url,
        }
    }
}

#[post("/create")]
async fn create_link(link: Json<ApiAddLink>) -> impl Responder {
    let new_link = link.0.to_new_link();
    let new_code = new_link.tiny_code.clone();
    Json(new_code)
}

#[get("/{code}")]
async fn get_from_link() -> impl Responder {
    let url = "http://baidu.com";
    HttpResponse::Found()
        .append_header((header::LOCATION, url))
        .finish()
}

#[get("/links")]
async fn get_all_links() -> impl Responder {
    let mut links = Vec::new();
    links.push(Link {
        tiny_code: "111".to_string(),
        origin_url: String::from("http://baidu.com"),
    });
    links.push(Link {
        tiny_code: "222".to_string(),
        origin_url: String::from("http://google.com"),
    });
    Json(links)
}
