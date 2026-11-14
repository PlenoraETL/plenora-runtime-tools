//! Native binding; orchestration remains in plenora-runtime-chains.
#![forbid(unsafe_code)]
use plenora_runtime_chain_host::{DatabaseConfig, RemoteClient, ScriptedConfig, ScriptedHost};
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
    #[pyo3(signature = (config, profile="scripted"))]
    fn new(py: Python<'_>, config: &str, profile: &str) -> PyResult<Self> {
        py.detach(|| {
            let host = match profile {
                "scripted" => {
                    let config: ScriptedConfig = serde_json::from_str(config)
                        .map_err(|_| PyRuntimeError::new_err("invalid"))?;
                    ScriptedHost::new(config)
                }
                "database" => {
                    let config: DatabaseConfig = serde_json::from_str(config)
                        .map_err(|_| PyRuntimeError::new_err("invalid"))?;
                    ScriptedHost::database(config)
                }
                _ => return Err(PyRuntimeError::new_err("invalid")),
            }
            .map_err(native_error)?;
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .map_err(|_| PyRuntimeError::new_err("storage"))?;
            Ok(Self {
                host: Arc::new(host),
                runtime: Arc::new(runtime),
            })
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
                .map_err(native_error)
        })
    }
}
#[pyfunction]
fn failure_json(code: &str, value: &str) -> PyResult<String> {
    let error = serde_json::from_value(serde_json::Value::String(code.into()))
        .map_err(|_| PyRuntimeError::new_err("invalid"))?;
    let value = serde_json::from_str(value).map_err(|_| PyRuntimeError::new_err("invalid"))?;
    let response = plenora_runtime_chains::Response {
        value,
        error: Some(error),
    };
    serde_json::to_string(&response.failure()).map_err(|_| PyRuntimeError::new_err("invalid"))
}
#[pymodule]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(failure_json, module)?)?;
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
    #[pyo3(signature = (server, subject, observation_timeout_json="10000"))]
    fn new(
        py: Python<'_>,
        server: &str,
        subject: &str,
        observation_timeout_json: &str,
    ) -> PyResult<Self> {
        let observation_timeout_ms: u64 = serde_json::from_str(observation_timeout_json)
            .map_err(|_| native_error(plenora_runtime_chains::Error::Invalid))?;
        py.detach(|| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .map_err(|_| PyRuntimeError::new_err("storage"))?;
            let client = runtime
                .block_on(RemoteClient::connect_with_timeout(
                    server,
                    subject,
                    observation_timeout_ms,
                ))
                .map_err(native_error)?;
            Ok(Self { client, runtime })
        })
    }
    #[staticmethod]
    fn migrate() -> PyResult<()> {
        Err(native_error(plenora_runtime_chains::Error::Unsupported))
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
    PyRuntimeError::new_err(error.code())
}
