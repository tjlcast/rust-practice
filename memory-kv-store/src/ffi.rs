use pyo3::prelude::*;
use crate::kv_store::KvStore;

#[pyclass]
pub struct PyKvStore {
    inner: KvStore,
}

#[pymethods]
impl PyKvStore {
    #[new]
    fn new() -> Self {
        PyKvStore {
            inner: KvStore::new(),
        }
    }

    fn set(&mut self, key: String, value: String) {
        self.inner.set(key, value);
    }

    fn get(&self, key: String) -> Option<String> {
        self.inner.get(&key)
    }

    fn delete(&mut self, key: String) -> Option<String> {
        self.inner.delete(&key)
    }
}

/// Python module entrypoint
#[pymodule]
fn memory_kv_store(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyKvStore>()?;
    Ok(())
}
