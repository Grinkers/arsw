use super::*;

#[pyfunction(signature = (obj, *, start = 0, stop = None, flags = None))]
pub(crate) fn carray(
	py: Python<'_>,
	obj: &Bound<'_, PyAny>,
	start: usize,
	stop: Option<usize>,
	flags: Option<c_int>,
) -> PyResult<Py<PyAny>> {
	let _ = flags;
	if start == 0 && stop.is_none() {
		return Ok(obj.clone().unbind());
	}

	let sequence = obj.cast::<PySequence>()?;
	let len = sequence.len()?;
	let stop = stop.unwrap_or(len);
	let start = start.min(len);
	let stop = stop.min(len);
	if start >= stop {
		return Ok(PyTuple::empty(py).unbind().into_any());
	}

	let mut values = Vec::with_capacity(stop - start);
	for index in start..stop {
		values.push(sequence.get_item(index)?.unbind());
	}

	Ok(PyTuple::new(py, values)?.unbind().into_any())
}
