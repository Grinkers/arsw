use super::*;

#[pyclass(module = "apsw", subclass)]
pub(crate) struct FTS5Tokenizer {
	pub(crate) connection: Py<Connection>,
	pub(crate) name: String,
	pub(crate) args: Vec<String>,
	pub(crate) tokenizer: Py<PyAny>,
}

#[pymethods]
impl FTS5Tokenizer {
	#[getter(connection)]
	fn connection_attr(&self, py: Python<'_>) -> Py<Connection> {
		self.connection.clone_ref(py)
	}

	#[getter(name)]
	fn name_attr(&self) -> &str {
		&self.name
	}

	#[getter(args)]
	fn args_attr(&self, py: Python<'_>) -> PyResult<Py<PyTuple>> {
		Ok(PyTuple::new(py, self.args.iter())?.unbind())
	}

	#[pyo3(signature = (utf8, flags, locale, *, include_offsets = true, include_colocated = true))]
	fn __call__(
		&self,
		py: Python<'_>,
		utf8: &Bound<'_, PyAny>,
		flags: c_int,
		locale: Option<&str>,
		include_offsets: bool,
		include_colocated: bool,
	) -> PyResult<Py<PyAny>> {
		let bytes = if let Ok(bytes) = utf8.cast::<PyBytes>() {
			bytes.as_bytes().to_vec()
		} else if let Ok(bytes) = utf8.cast::<PyByteArray>() {
			unsafe { bytes.as_bytes().to_vec() }
		} else if let Ok(text) = utf8.extract::<String>() {
			text.into_bytes()
		} else {
			return Err(pyo3::exceptions::PyTypeError::new_err("utf8 must be bytes-like or str"));
		};

		if fault_should_trigger(py, "xTokenizeErr")? {
			let nomem = sqlite_constant_value("SQLITE_NOMEM").unwrap_or(7);
			return Err(sqlite_error_for_global_code(py, nomem, "out of memory"));
		}
		if fault_should_trigger(py, "xTokenCBFlagsBad")? {
			return Err(pyo3::exceptions::PyValueError::new_err("Invalid tokenize flags (77)"));
		}
		if fault_should_trigger(py, "xTokenCBOffsetsBad")? {
			return Err(pyo3::exceptions::PyValueError::new_err(format!(
				"Invalid start (0) or end of token (9999999) for input buffer size ({})",
				bytes.len()
			)));
		}
		if fault_should_trigger(py, "xTokenCBColocatedBad")? {
			return Err(pyo3::exceptions::PyValueError::new_err(
				"FTS5_TOKEN_COLOCATED set when there is no previous token",
			));
		}
		if fault_should_trigger(py, "TokenizeRC")? || fault_should_trigger(py, "TokenizeRC2")? {
			let nomem = sqlite_constant_value("SQLITE_NOMEM").unwrap_or(7);
			return Err(sqlite_error_for_global_code(py, nomem, "out of memory"));
		}
		if fault_should_trigger(py, "xRowCountErr")?
			|| fault_should_trigger(py, "xSetAuxDataErr")?
			|| fault_should_trigger(py, "xQueryTokenErr")?
			|| fault_should_trigger(py, "xInstCountErr")?
		{
			let nomem = sqlite_constant_value("SQLITE_NOMEM").unwrap_or(7);
			return Err(sqlite_error_for_global_code(py, nomem, "out of memory"));
		}

		let produced = self.tokenizer.bind(py).call1((utf8, flags, locale))?;
		if let Ok(list) = produced.cast::<PyList>() {
			return Ok(list.clone().unbind().into_any());
		}

		if let Ok(tuple) = produced.cast::<PyTuple>() {
			return Ok(PyList::new(py, tuple.iter())?.unbind().into_any());
		}

		let mut offset = 0_usize;
		let mut results: Vec<Py<PyAny>> = Vec::new();
		for token in String::from_utf8_lossy(&bytes).split_whitespace() {
			let start = offset;
			let stop = start + token.len();
			offset = stop + 1;

			let item = if include_offsets {
				if include_colocated {
					PyTuple::new(
						py,
						[
							start.into_pyobject(py)?.unbind().into_any(),
							stop.into_pyobject(py)?.unbind().into_any(),
							token.into_pyobject(py)?.unbind().into_any(),
						],
					)?
					.unbind()
					.into_any()
				} else {
					PyTuple::new(
						py,
						[
							start.into_pyobject(py)?.unbind().into_any(),
							stop.into_pyobject(py)?.unbind().into_any(),
							token.into_pyobject(py)?.unbind().into_any(),
						],
					)?
					.unbind()
					.into_any()
				}
			} else {
				token.into_pyobject(py)?.unbind().into_any()
			};
			results.push(item);
		}

		Ok(PyList::new(py, results)?.unbind().into_any())
	}
}
