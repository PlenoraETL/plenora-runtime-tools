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
    module.add("FUNCTIONS", plenora_runtime_chains::FUNCTIONS.to_vec())?;
    Ok(())
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
