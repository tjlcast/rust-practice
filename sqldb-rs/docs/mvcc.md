> [code] + 使用用例的方式图文并茂的方式帮我讲解上面的代码

下面用“时间轴 + 场景故事”的方式，把上面这段 MVCC 代码拆成 5 个典型用例。每个用例先给一张“可视化时间轴”，再配一段“代码对应点”，让你一眼看懂事务在底层到底发生了什么。

---

## 用例 1：数据库刚启动，第一次 begin

### 时间轴
```
time ──────────────────────────────────────────►
  t0  数据库空库
  t1  txn1.begin() ──► 写 NextVersion=2
  t2  txn1 看到 active=[]
```

### 关键代码
```rust
// begin() 里第一次 get NextVersion，得到 None → 用 1
let next_version = match storage_engine.get(...) { None => 1 };
// 立即把 NextVersion 改成 2
storage_engine.set(MvccKey::NextVersion.encode()?, &(1+1))?;
// 此时 active_versions 为空
```

### 底层 KV 长什么样
| Key(hex)             | Value |
|----------------------|-------|
| `00` (NextVersion)   | `02`  |

---

## 用例 2：并发读写，快照隔离

### 时间轴
```
time ──────────────────────────────────────────►
  t0  txn1.begin() → ver=1
  t1  txn2.begin() → ver=2
  t2  txn1.set("A", "100")
  t3  txn2.get("A")  ──► 看不到 100
```

### 可视化
```
Txn1  ─┬─ begin ver=1
       └─ set A=100
Txn2  ────┬─ begin ver=2
          └─ get A  → 返回 None（因为 100 的 ver=1 对 ver=2 不可见）
```

### 关键代码
```rust
// txn1.set("A", "100")
storage_engine.set(
    MvccKey::Version(b"A", 1).encode()?,   // key: A+ver1
    bincode::serialize(&Some(b"100".to_vec()))?
)?;

// txn2.get("A") 扫描 0..=2，取最大可见版本
if state.is_visible(1) == false {   // 1 在 txn2 的 active 列表里
    continue;
}
```

---

## 用例 3：写写冲突检测

### 时间轴
```
time ──────────────────────────────────────────►
  t0  txn1.begin() ver=3
  t1  txn2.begin() ver=4
  t2  txn2.set("B", "200")  → 成功
  t3  txn1.set("B", "300")  → WriteConflict 错误
```

### 可视化
```
Txn1 ─┬─ begin ver=3
      └─ set B=300 ❌ 冲突
Txn2 ────┬─ begin ver=4
         └─ set B=200 ✅ 成功
```

### 关键代码
```rust
// txn1.write_inner() 扫描 Version("B", 0)..Version("B", u64::MAX)
// 发现 Version("B",4) 已经存在，且 !state.is_visible(4) → 报错
if !self.state.is_visible(4) {
    return Err(Error::WriteConflict);
}
```

---

## 用例 4：正常 commit

### 时间轴
```
time ──────────────────────────────────────────►
  t0  txn3.begin() ver=5
  t1  txn3.set("C", "cat")
  t2  txn3.commit()
```

### commit 时底层 KV 变化
| Key(hex)                          | Value |
|-----------------------------------|-------|
| `00`                              | `06`  |
| `31 05` (TxnActive5)              | 删除   |
| `32 05 43` (TxnWrite5,"C")        | 删除   |
| `33 43 05` (Version"C",5)         | 保留   |

### 关键代码
```rust
// commit() 里
storage_engine.delete(MvccKey::TxnActive(5).encode()?)?;
storage_engine.delete(MvccKey::TxnWrite(5, b"C").encode()?)?;
// Version(b"C",5) 不删，供快照读
```

---

## 用例 5：rollback 撤销删除

### 时间轴
```
time ──────────────────────────────────────────►
  t0  txn4.begin() ver=6
  t1  txn4.delete("D")      // 实际写 Version("D",6)=None
  t2  txn4.rollback()
```

### rollback 时底层 KV 变化
| Key(hex)                          | 操作 |
|-----------------------------------|------|
| `31 06` (TxnActive6)              | 删除 |
| `32 06 44` (TxnWrite6,"D")        | 删除 |
| `33 44 06` (Version"D",6)=None    | 删除 |

### 关键代码
```rust
// rollback() 里
match MvccKey::decode(key)? {
    MvccKey::TxnWrite(_, raw_key) =>
        delete_keys.push(MvccKey::Version(raw_key, 6).encode()?);
}
// 把上面三条全部删掉，等于“从未发生过”
```

---

## 小结图：MVCCKey 的 4 种编码

```
NextVersion   0x00
TxnActive(v)  0x31 | v (varint)
TxnWrite(v,k) 0x32 | v | k
Version(k,v)  0x33 | k | v
```

（`0x31/32/33` 是枚举序号，`serde`+`keycode_se` 会自动加前缀。）

---

把 5 张时间轴连起来，你就得到了一个“迷你 TiKV”的完整生命周期。  
希望这种“图 + 代码”的方式能让你一眼看懂每个事务在底层到底动了哪些 bit！