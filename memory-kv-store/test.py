from memory_kv_store import PyKvStore

store = PyKvStore()
store.set("hello", "world")
assert store.get("hello") == "world"
print("✔️ KV store works!")
