# 获取所有用户
curl http://localhost:8080/users

# 获取特定用户
curl http://localhost:8080/users/1

# 创建新用户
curl -X POST -H "Content-Type: application/json" -d '{"id":2,"name":"Bob"}' --url http://localhost:8080/users

# 删除用户
curl -X DELETE http://localhost:8080/users/1


以下是转换后的 `curl` 命令，用于测试你的 Rust API 接口：

---

### **1. GET 请求（获取用户信息）**
```bash
curl -X GET "http://127.0.0.1:8080/users/1"
```
**预期响应**：
```json
{"id":1,"name":"Alice"}
```

---

### **2. POST 请求（创建用户）**
```bash
curl -X POST "http://127.0.0.1:8080/users" \
  -H "Content-Type: application/json" \
  -d '{"id": 2, "name": "Bob"}'
```
**预期响应**（返回提交的 JSON 数据）：
```json
{"id":2,"name":"Bob"}
```