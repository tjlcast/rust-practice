use memory_kv_store::KvStore;

#[test]
fn test_basic_set_get() {
    let mut store = KvStore::new();
    store.set("foo".to_string(), "bar".to_string());
    assert_eq!(store.get("foo"), Some("bar".to_string()));
}
