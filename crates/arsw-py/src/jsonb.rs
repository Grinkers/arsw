use super::*;

#[pyfunction(signature = (value, default = None))]
pub(crate) fn jsonb_encode(
	py: Python<'_>,
	value: &Bound<'_, PyAny>,
	default: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyBytes>> {
	let json = PyModule::import(py, "json")?;
	let encoded = if let Some(default) = default {
		let kwargs = PyDict::new(py);
		kwargs.set_item("default", default)?;
		json.getattr("dumps")?.call((value,), Some(&kwargs))?
	} else {
		json.getattr("dumps")?.call1((value,))?
	};
	let text = encoded.extract::<String>()?;
	Ok(PyBytes::new(py, text.as_bytes()).unbind())
}

#[pyfunction(signature = (value, parse_int = None, parse_float = None, object_hook = None, array_hook = None))]
pub(crate) fn jsonb_decode(
	py: Python<'_>,
	value: &Bound<'_, PyAny>,
	parse_int: Option<&Bound<'_, PyAny>>,
	parse_float: Option<&Bound<'_, PyAny>>,
	object_hook: Option<&Bound<'_, PyAny>>,
	array_hook: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
	let data = if let Ok(text) = value.extract::<String>() {
		text
	} else if let Ok(bytes) = value.cast::<PyBytes>() {
		String::from_utf8_lossy(bytes.as_bytes()).into_owned()
	} else if let Ok(bytes) = value.cast::<PyByteArray>() {
		unsafe { String::from_utf8_lossy(bytes.as_bytes()).into_owned() }
	} else {
		return Err(pyo3::exceptions::PyTypeError::new_err("jsonb_decode expects bytes-like or str"));
	};

	let json = PyModule::import(py, "json")?;
	let kwargs = PyDict::new(py);
	if let Some(parse_int) = parse_int {
		kwargs.set_item("parse_int", parse_int)?;
	}
	if let Some(parse_float) = parse_float {
		kwargs.set_item("parse_float", parse_float)?;
	}
	if let Some(object_hook) = object_hook {
		kwargs.set_item("object_hook", object_hook)?;
	}

	let loaded = match json.getattr("loads")?.call((data.clone(),), Some(&kwargs)) {
		Ok(value) => value,
		Err(_) => return Ok(data.into_pyobject(py)?.unbind().into_any()),
	};
	if let Some(array_hook) = array_hook {
		if loaded.is_instance_of::<PyList>() {
			return Ok(array_hook.call1((loaded,))?.unbind());
		}
	}
	Ok(loaded.unbind())
}

#[pyfunction]
pub(crate) fn jsonb_detect(py: Python<'_>, value: &Bound<'_, PyAny>) -> bool {
	jsonb_decode(py, value, None, None, None, None).is_ok()
}
