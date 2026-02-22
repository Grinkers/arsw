use super::*;

pub(crate) fn parse_exception_code(py: Python<'_>, code: &Bound<'_, PyAny>) -> PyResult<i32> {
	let operator = PyModule::import(py, "operator")?;
	let indexed = operator.getattr("index")?.call1((code,))?;
	let code_text = indexed.str()?.to_str()?.to_string();

	let parsed = code_text.parse::<i64>().map_err(|_| {
		pyo3::exceptions::PyOverflowError::new_err(format!("{code_text} overflowed C int"))
	})?;

	if parsed > i64::from(i32::MAX) || parsed < i64::from(i32::MIN) {
		return Err(pyo3::exceptions::PyOverflowError::new_err(format!(
			"{code_text} overflowed C int"
		)));
	}

	Ok(i32::try_from(parsed).expect("checked i64 range against i32 bounds"))
}

pub(crate) fn exception_instance_for_code(
	py: Python<'_>,
	code: i32,
	message: Option<&str>,
) -> PyResult<Py<PyAny>> {
	if code <= 0 {
		return Err(pyo3::exceptions::PyValueError::new_err(format!(
			"{code} is not a known error code"
		)));
	}

	let primary = code & 0xff;
	let instance = match (primary, message) {
		(1, Some(message)) => py.get_type::<SQLError>().call1((message,))?,
		(1, None) => py.get_type::<SQLError>().call0()?,
		(2, Some(message)) => py.get_type::<InternalError>().call1((message,))?,
		(2, None) => py.get_type::<InternalError>().call0()?,
		(3, Some(message)) => py.get_type::<PermissionsError>().call1((message,))?,
		(3, None) => py.get_type::<PermissionsError>().call0()?,
		(4, Some(message)) => py.get_type::<AbortError>().call1((message,))?,
		(4, None) => py.get_type::<AbortError>().call0()?,
		(5, Some(message)) => py.get_type::<BusyError>().call1((message,))?,
		(5, None) => py.get_type::<BusyError>().call0()?,
		(6, Some(message)) => py.get_type::<LockedError>().call1((message,))?,
		(6, None) => py.get_type::<LockedError>().call0()?,
		(7, Some(message)) => py.get_type::<NoMemError>().call1((message,))?,
		(7, None) => py.get_type::<NoMemError>().call0()?,
		(8, Some(message)) => py.get_type::<ReadOnlyError>().call1((message,))?,
		(8, None) => py.get_type::<ReadOnlyError>().call0()?,
		(9, Some(message)) => py.get_type::<InterruptError>().call1((message,))?,
		(9, None) => py.get_type::<InterruptError>().call0()?,
		(10, Some(message)) => py.get_type::<IOError>().call1((message,))?,
		(10, None) => py.get_type::<IOError>().call0()?,
		(11, Some(message)) => py.get_type::<CorruptError>().call1((message,))?,
		(11, None) => py.get_type::<CorruptError>().call0()?,
		(12, Some(message)) => py.get_type::<NotFoundError>().call1((message,))?,
		(12, None) => py.get_type::<NotFoundError>().call0()?,
		(13, Some(message)) => py.get_type::<FullError>().call1((message,))?,
		(13, None) => py.get_type::<FullError>().call0()?,
		(14, Some(message)) => py.get_type::<CantOpenError>().call1((message,))?,
		(14, None) => py.get_type::<CantOpenError>().call0()?,
		(15, Some(message)) => py.get_type::<ProtocolError>().call1((message,))?,
		(15, None) => py.get_type::<ProtocolError>().call0()?,
		(16, Some(message)) => py.get_type::<EmptyError>().call1((message,))?,
		(16, None) => py.get_type::<EmptyError>().call0()?,
		(17, Some(message)) => py.get_type::<SchemaChangeError>().call1((message,))?,
		(17, None) => py.get_type::<SchemaChangeError>().call0()?,
		(18, Some(message)) => py.get_type::<TooBigError>().call1((message,))?,
		(18, None) => py.get_type::<TooBigError>().call0()?,
		(19, Some(message)) => py.get_type::<ConstraintError>().call1((message,))?,
		(19, None) => py.get_type::<ConstraintError>().call0()?,
		(20, Some(message)) => py.get_type::<MismatchError>().call1((message,))?,
		(20, None) => py.get_type::<MismatchError>().call0()?,
		(21, Some(message)) => py.get_type::<MisuseError>().call1((message,))?,
		(21, None) => py.get_type::<MisuseError>().call0()?,
		(22, Some(message)) => py.get_type::<NoLFSError>().call1((message,))?,
		(22, None) => py.get_type::<NoLFSError>().call0()?,
		(23, Some(message)) => py.get_type::<AuthError>().call1((message,))?,
		(23, None) => py.get_type::<AuthError>().call0()?,
		(24, Some(message)) => py.get_type::<FormatError>().call1((message,))?,
		(24, None) => py.get_type::<FormatError>().call0()?,
		(25, Some(message)) => py.get_type::<RangeError>().call1((message,))?,
		(25, None) => py.get_type::<RangeError>().call0()?,
		(26, Some(message)) => py.get_type::<NotADBError>().call1((message,))?,
		(26, None) => py.get_type::<NotADBError>().call0()?,
		_ => {
			return Err(pyo3::exceptions::PyValueError::new_err(format!(
				"{code} is not a known error code"
			)));
		}
	};

	instance.setattr("result", primary)?;
	instance.setattr("extendedresult", code)?;
	Ok(instance.unbind())
}

#[pyfunction]
pub(crate) fn exception_for(py: Python<'_>, code: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
	let code = parse_exception_code(py, code)?;
	exception_instance_for_code(py, code, None)
}
