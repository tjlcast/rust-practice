


pub mod kv_store;
pub use kv_store::KvStore;

#[cfg(feature = "python")]
pub mod ffi;


fn main() {
    println!("Hello, world!");
}