打包成 Python 模块：

``` bash
pip install maturin
maturin develop --features python
```

使用：

``` python
from memory_kv_store import PyKvStore

store = PyKvStore()
store.set("hello", "world")
assert store.get("hello") == "world"
```



这个错误说明你在运行 `maturin develop` 时，**没有激活 Python 虚拟环境**，而 `maturin develop` 这个命令必须在虚拟环境中执行，它会尝试把 Rust 编译生成的 Python 扩展模块“注入”进当前的 Python 虚拟环境里。

---

## ✅ 正确解决方式

你有两个选择：

---

### ✅ **方式一：创建并激活一个虚拟环境（推荐）**

#### 🔧 创建虚拟环境

```bash
python -m venv .venv
```

#### 🟢 激活虚拟环境

* Linux / macOS:

  ```bash
  source .venv/bin/activate
  ```

* Windows:

  ```powershell
  .venv\Scripts\activate
  ```

#### ✅ 然后再运行：

```bash
maturin develop --features python
```

这就会将 `memory_kv_store` 模块安装到当前虚拟环境中，供你用 `import` 使用。

---

### ✅ **方式二：不使用虚拟环境，直接构建并用 pip 安装**

如果你不想搞虚拟环境，可以这样做：

#### 1. 构建 `.whl` 包：

```bash
maturin build --release --features python
```

构建成功后，会生成类似文件：

```
target/wheels/memory_kv_store-0.1.0-cp38-cp38-manylinux_2_17_x86_64.whl
```

#### 2. 用 pip 安装：

```bash
pip install target/wheels/memory_kv_store-0.1.0-...whl
```

---

## 📌 建议

* 如果你做的是开发工作，**方式一 + `maturin develop`** 更方便，不用反复安装；
* 如果你只是想测试或部署，**方式二 + `maturin build`** 更干净明确。

---

需要我为你生成一个包含虚拟环境创建、构建、安装、测试的一键脚本吗？
