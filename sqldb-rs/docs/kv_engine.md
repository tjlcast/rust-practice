> [code] + 使用用例的方式图文并茂的方式帮我讲解上面的代码

下面用 4 个“故事化”的用例，把这段 KV-SQL 引擎代码拆成时间轴+示意图+代码锚点。  
每个场景都回答三件事：  
1. 用户视角发生了什么  
2. 底层 KV 怎样变  
3. 关键代码在哪  

---

## 用例 1：启动数据库 → 首次建表

### 用户操作
```sql
CREATE TABLE users (
    id    INT PRIMARY KEY,
    name  TEXT
);
```

### 时间轴
```
t0  进程启动
t1  KVEngine::new() 把底层 StorageEngine 包进 MVCC
t2  txn = begin() 拿到 ver=1
t3  create_table() 写入表元数据
t4  commit()
```

### KV 状态
| Key(hex)                | Value 说明 |
|-------------------------|------------|
| `00` (NextVersion)      | `02`       |
| `01 75 73 65 72 73` (Table"users") | 表结构序列化 |

### 关键代码
```rust
// KVEngine::new()
let engine = KVEngine::new(storage::Engine::new());
// create_table()
let key = Key::Table("users".into());
self.txn.set(bincode::serialize(&key)?, bincode::serialize(&table)?)?;
```

---

## 用例 2：并发插入行 → 快照隔离

### 用户操作
```
Session A: BEGIN; INSERT INTO users VALUES(1, 'Alice');
Session B: BEGIN; INSERT INTO users VALUES(2, 'Bob');
```

### 时间轴
```
t0  txnA ver=3 begin
t1  txnB ver=4 begin
t2  txnA create_row(1,'Alice')
t3  txnB create_row(2,'Bob')
t4  txnA commit()
t5  txnB commit()
```

### KV 状态（只画逻辑）
| Key(hex)                        | Value |
|---------------------------------|-------|
| `02 75 73 65 72 73 01` (Row"users",1) | Alice 的 Row |
| `02 75 73 65 72 73 02` (Row"users",2) | Bob 的 Row |

### 关键代码
```rust
// create_row()
let id = Key::Row("users".into(), Value::Int(1));
self.txn.set(bincode::serialize(&id)?, bincode::serialize(&row)?)?;
```
> 注意：此时 MVCC 会为每个 key 带上版本号，但 KVTransaction 完全感知不到，因为 MVCC 已经封装好了。

---

## 用例 3：一个事务里读自己写

### 用户操作
```
BEGIN;
INSERT INTO users VALUES(3, 'Cathy');
SELECT * FROM users;   -- 能看到刚插入的行
COMMIT;
```

### 时间轴
```
t0  txn ver=5 begin
t1  create_row(3,'Cathy') 写入 Row("users",3)=Cathy
t2  scan_table("users") 扫前缀 Row("users")*
    → 自己刚才写的行可见（MVCC 快照 = 本事务版本）
t3  commit()
```

### 关键代码
```rust
// scan_table()
let prefix = KeyPrefix::Row("users".into());
let results = self.txn.scan_prefix(bincode::serialize(&prefix)?)?;
```
> MVCC 保证：txn 版本=5，扫描时只会看到 ≤5 且未活跃的数据，所以能看到自己刚才写的 5。

---

## 用例 4：回滚事务 → 脏写消失

### 用户操作
```
BEGIN;
DELETE FROM users WHERE id=1;  -- 其实写入 Row("users",1)=None
ROLLBACK;
```

### 时间轴
```
t0  txn ver=6 begin
t1  create_row(1,None)   -- 逻辑删除
t2  rollback()           -- MVCC 把 Version("users",1,6) 整条删掉
t3  其他事务再读 id=1 仍能看到旧值
```

### KV 变化
| Key(hex)               | t1 存在 | t2 回滚后 |
|------------------------|---------|-----------|
| `02 75 73 65 72 73 01` | ✅None   | ❌删除     |

### 关键代码
```rust
// rollback()
self.txn.rollback()?;  // 最终调到 MvccTransaction::rollback()
```
> rollback 会扫描 TxnWrite(6,*) 把对应 Version(...) 以及自身标记全部清掉。

---

## 一张总览图：Key 空间布局

```
Key Space（前缀树视角）
├── 00                    NextVersion
├── 01 {table_name}       Table 元数据
└── 02 {table_name} {pk}  Row 数据（带 MVCC 版本）
```

所有 SQL 层可见的 key 都先被 `bincode` 序列化成 `enum Key` / `enum KeyPrefix`，再交给 MVCC 层；MVCC 再在其外部包上版本号。  
因此 KVTransaction 只需要关心逻辑 key，MVCC 负责并发、回滚、快照。

---

把 4 张时间轴串联起来，你就拥有了一个“支持 ACID 的迷你 SQL 引擎”的完整生命周期。
