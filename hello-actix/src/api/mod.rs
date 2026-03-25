use serde::Serialize;

pub mod links;

#[derive(Serialize)]
pub struct ApiResult<T: Serialize> {
    pub ok: bool,
    pub err: Option<String>,
    pub data: Option<T>,
}

impl<T: Serialize> ApiResult<T> {
    pub fn success(r: Option<T>) -> ApiResult<T> {
        ApiResult {
            ok: true,
            err: None,
            data: r,
        }
    }

    pub fn error<E: ToString>(e: E) -> ApiResult<T> {
        ApiResult {
            ok: false,
            err: Some(e.to_string()),
            data: None,
        }
    }
}
