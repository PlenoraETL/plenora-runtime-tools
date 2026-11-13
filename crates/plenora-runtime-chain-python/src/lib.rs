//! Native binding; orchestration remains in plenora-runtime-chains.
#![forbid(unsafe_code)]
use plenora_runtime_chain_host::{RemoteClient, ScriptedConfig, ScriptedHost};
use pyo3::{exceptions::PyRuntimeError, prelude::*};
use std::sync::Arc;

#[pyclass]
struct NativeClient {
    host: Arc<ScriptedHost>,
    runtime: Arc<tokio::runtime::Runtime>,
}
#[pymethods]
impl NativeClient {
    #[new]
    fn new(config: &str) -> PyResult<Self> {
        let config: ScriptedConfig =
            serde_json::from_str(config).map_err(|_| PyRuntimeError::new_err("invalid"))?;
        let host = ScriptedHost::new(config).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .map_err(|_| PyRuntimeError::new_err("storage"))?;
        Ok(Self {
            host: Arc::new(host),
            runtime: Arc::new(runtime),
        })
    }
    fn call(&self, py: Python<'_>, request: &str) -> PyResult<String> {
        py.detach(|| {
            let result = self.runtime.block_on(self.host.call(request.as_bytes()));
            serde_json::to_string(&result).map_err(|_| PyRuntimeError::new_err("invalid"))
        })
    }
    fn migrate(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| {
            self.runtime
                .block_on(self.host.migrate())
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })
    }
}
#[pymodule]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<NativeClient>()?;
    module.add_class::<NativeRemoteClient>()?;
    module.add_class::<NativeEventCursor>()?;
    module.add("FUNCTIONS", plenora_runtime_chains::FUNCTIONS.to_vec())?;
    Ok(())
}

#[pyclass]
struct NativeEventCursor {
    cursor: plenora_runtime_chains::EventCursor,
}
#[pymethods]
impl NativeEventCursor {
    #[new]
    fn new(request: &str) -> PyResult<Self> {
        let request: plenora_runtime_chains::Request =
            serde_json::from_str(request).map_err(|_| PyRuntimeError::new_err("invalid"))?;
        let plenora_runtime_chains::Request::Events {
            run_id,
            cursor,
            limit,
        } = request
        else {
            return Err(PyRuntimeError::new_err("invalid"));
        };
        Ok(Self {
            cursor: plenora_runtime_chains::EventCursor::new(&run_id, cursor, limit)
                .map_err(native_error)?,
        })
    }
    fn request(&self) -> PyResult<String> {
        serde_json::to_string(&self.cursor.request())
            .map_err(|_| PyRuntimeError::new_err("invalid"))
    }
    fn accept(&mut self, py: Python<'_>, value: &str) -> PyResult<String> {
        py.detach(|| {
            let value =
                serde_json::from_str(value).map_err(|_| PyRuntimeError::new_err("invalid"))?;
            let events = self.cursor.accept(&value).map_err(native_error)?;
            serde_json::to_string(&events).map_err(|_| PyRuntimeError::new_err("invalid"))
        })
    }
}

#[pyclass]
struct NativeRemoteClient {
    client: RemoteClient,
    runtime: tokio::runtime::Runtime,
}
#[pymethods]
impl NativeRemoteClient {
    #[new]
    fn new(py: Python<'_>, server: &str, subject: &str) -> PyResult<Self> {
        py.detach(|| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .map_err(|_| PyRuntimeError::new_err("storage"))?;
            let client = runtime
                .block_on(RemoteClient::connect(server, subject))
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            Ok(Self { client, runtime })
        })
    }
    fn call(&self, py: Python<'_>, request: &str) -> PyResult<String> {
        py.detach(|| {
            let response = self
                .runtime
                .block_on(self.client.call(request.as_bytes().to_vec()))
                .unwrap_or_else(|error| plenora_runtime_chains::Response {
                    value: serde_json::Value::Null,
                    error: Some(error),
                });
            serde_json::to_string(&response).map_err(|_| PyRuntimeError::new_err("invalid"))
        })
    }
}

fn native_error(error: plenora_runtime_chains::Error) -> PyErr {
    let code = serde_json::to_value(error)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_else(|| "storage".into());
    PyRuntimeError::new_err(code)
}
