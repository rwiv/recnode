use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3_async_runtimes::tokio::future_into_py;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use std::str::FromStr;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

async fn download_impl(
    url: String,
    headers: HeaderMap,
    file_path: Option<String>,
    return_value: bool,
) -> Result<(u16, u64, Option<Vec<u8>>), Box<dyn std::error::Error>> {

    let client = reqwest::Client::new();
    let response = client.get(&url)
        .headers(headers)
        .send()
        .await?;

    let status = response.status().as_u16();
    let content = response.bytes().await?;
    let size = content.len() as u64;

    if let Some(path) = file_path {
        if status < 400 && size > 0 {
            let mut file = File::create(path).await?;
            file.write_all(&content).await?;
        }
    }

    let ret_content = if return_value {
        Some(content.to_vec())
    } else {
        None
    };

    Ok((status, size, ret_content))
}

// Python 노출 함수 Wrapper
#[pyfunction]
#[pyo3(signature = (url, headers, file_path=None, return_value=false))]
fn request_file<'a>(
    py: Python<'a>,
    url: String,
    headers: Bound<'a, PyDict>,
    file_path: Option<String>,
    return_value: bool,
) -> PyResult<Bound<'a, PyAny>> {

    let mut header_map = HeaderMap::new();
    for (key, value) in headers.iter() {
        let key_str: String = key.extract()?;
        let value_str: String = value.extract()?;

        let h_name = HeaderName::from_str(&key_str)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid header: {}", e)))?;
        let h_value = HeaderValue::from_str(&value_str)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid value: {}", e)))?;

        header_map.insert(h_name, h_value);
    }

    // future_into_py: Rust의 Future를 Python의 Awaitable로 변환
    future_into_py(py, async move {
        match download_impl(url, header_map, file_path, return_value).await {
            Ok((status, size, content)) => Ok((status, size, content)),
            // Rust 에러를 Python 예외(RuntimeError)로 변환
            Err(e) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Rust Error: {}", e))),
        }
    })
}

// 모듈 등록 (이름은 Cargo.toml의 name과 같아야 함)
#[pymodule]
fn rust_downloader(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(request_file, m)?)?;
    Ok(())
}
