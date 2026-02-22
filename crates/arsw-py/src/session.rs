use super::*;

#[pyfunction(signature = (op, *args))]
pub(crate) fn session_config(
	py: Python<'_>,
	op: c_int,
	args: &Bound<'_, PyTuple>,
) -> PyResult<Py<PyAny>> {
	if args.len() > 1 {
		return Err(pyo3::exceptions::PyTypeError::new_err(
			"session_config expected at most 2 arguments",
		));
	}

	let initial = if args.is_empty() { -1 } else { parse_index_i32(py, &args.get_item(0)?)? };
	let mut value = initial;
	let rc = fault_injected_sqlite_call!(
		py,
		"sqlite3session_config",
		"session_config",
		"op, value",
		unsafe { arsw::ffi::sqlite3session_config(op, (&raw mut value).cast::<c_void>()) }
	);
	if rc != SQLITE_OK {
		if rc == sqlite_constant_value("SQLITE_MISUSE").unwrap_or(21) {
			return Err(pyo3::exceptions::PyValueError::new_err("Unknown session config operation"));
		}
		return Err(sqlite_error_for_global_code(py, rc, "session_config failed"));
	}

	Ok(value.into_pyobject(py)?.unbind().into_any())
}

#[pyclass(module = "apsw", subclass)]
pub(crate) struct Session {
	pub(crate) connection: Py<Connection>,
	pub(crate) session: *mut arsw::ffi::Sqlite3Session,
	pub(crate) closed: bool,
}

unsafe impl Send for Session {}
unsafe impl Sync for Session {}

#[pyclass(module = "apsw", subclass)]
pub(crate) struct Rebaser {
	pub(crate) rebaser: *mut arsw::ffi::Sqlite3Rebaser,
	pub(crate) closed: bool,
}

unsafe impl Send for Rebaser {}
unsafe impl Sync for Rebaser {}

#[pyclass(module = "apsw", subclass)]
pub(crate) struct ChangesetBuilder {
	pub(crate) changesets: Vec<Vec<u8>>,
	pub(crate) closed: bool,
}

#[pyclass(module = "apsw", subclass)]
pub(crate) struct Changeset;

#[pyclass(module = "apsw", subclass)]
pub(crate) struct TableChange {
	pub(crate) name: String,
	pub(crate) op: String,
	pub(crate) opcode: c_int,
	pub(crate) column_count: usize,
}

#[pymethods]
impl Session {
	#[new]
	fn py_new(py: Python<'_>, connection: Py<Connection>, schema: &str) -> PyResult<Self> {
		let db = {
			let connection_ref = connection.borrow(py);
			if connection_ref.closed || connection_ref.db.is_null() {
				return Err(connection_closed_error());
			}
			connection_ref.db
		};

		let schema = CString::new(schema)
			.map_err(|_| pyo3::exceptions::PyValueError::new_err("schema contains NUL byte"))?;
		let mut session = null_mut();
		let rc = unsafe { arsw::ffi::sqlite3session_create(db, schema.as_ptr(), &raw mut session) };
		if rc != SQLITE_OK {
			return Err(sqlite_error_for_code(py, db, rc));
		}

		Ok(Self { connection, session, closed: false })
	}

	fn __bool__(&self) -> bool {
		!self.closed && !self.session.is_null()
	}

	fn close(&mut self) {
		if !self.session.is_null() {
			if let Ok(mut filters) = session_table_filters().lock() {
				filters.remove(&(self.session as usize));
			}
			self.session = null_mut();
		}
		self.closed = true;
	}

	#[pyo3(signature = (name = None))]
	fn attach(&mut self, py: Python<'_>, name: Option<&str>) -> PyResult<()> {
		if self.closed || self.session.is_null() {
			return Err(connection_closed_error());
		}

		let name = name
			.map(CString::new)
			.transpose()
			.map_err(|_| pyo3::exceptions::PyValueError::new_err("table name contains NUL byte"))?;
		let rc = unsafe {
			arsw::ffi::sqlite3session_attach(
				self.session,
				name.as_ref().map_or_else(null, |name| name.as_ptr()),
			)
		};
		if rc != SQLITE_OK {
			let db = self.connection.borrow(py).db;
			return Err(sqlite_error_for_code(py, db, rc));
		}

		Ok(())
	}

	fn changeset(&self, py: Python<'_>) -> PyResult<Py<PyBytes>> {
		if self.closed || self.session.is_null() {
			return Err(connection_closed_error());
		}

		let mut size: c_int = 0;
		let mut payload = null_mut();
		let rc =
			unsafe { arsw::ffi::sqlite3session_changeset(self.session, &raw mut size, &raw mut payload) };
		if rc != SQLITE_OK {
			let db = self.connection.borrow(py).db;
			return Err(sqlite_error_for_code(py, db, rc));
		}

		let bytes = if payload.is_null() || size <= 0 {
			PyBytes::new(py, &[])
		} else {
			let data = unsafe {
				std::slice::from_raw_parts(
					payload.cast::<u8>(),
					usize::try_from(size).expect("changeset size fits usize"),
				)
			};
			PyBytes::new(py, data)
		};

		if !payload.is_null() {
			unsafe {
				arsw::ffi::sqlite3_free(payload);
			}
		}

		Ok(bytes.unbind())
	}

	fn patchset(&self, py: Python<'_>) -> PyResult<Py<PyBytes>> {
		if self.closed || self.session.is_null() {
			return Err(connection_closed_error());
		}

		let mut size: c_int = 0;
		let mut payload = null_mut();
		let rc =
			unsafe { arsw::ffi::sqlite3session_patchset(self.session, &raw mut size, &raw mut payload) };
		if rc != SQLITE_OK {
			let db = self.connection.borrow(py).db;
			return Err(sqlite_error_for_code(py, db, rc));
		}

		let bytes = if payload.is_null() || size <= 0 {
			PyBytes::new(py, &[])
		} else {
			let data = unsafe {
				std::slice::from_raw_parts(
					payload.cast::<u8>(),
					usize::try_from(size).expect("patchset size fits usize"),
				)
			};
			PyBytes::new(py, data)
		};

		if !payload.is_null() {
			unsafe {
				arsw::ffi::sqlite3_free(payload);
			}
		}

		Ok(bytes.unbind())
	}

	fn changeset_stream(&self, py: Python<'_>, output: &Bound<'_, PyAny>) -> PyResult<()> {
		if self.closed || self.session.is_null() {
			return Err(connection_closed_error());
		}
		if !output.is_callable() {
			return Err(pyo3::exceptions::PyTypeError::new_err("Expected a callable"));
		}

		let output_data = Box::new(StreamOutputData { callback: output.clone().unbind() });
		let output_data = Box::into_raw(output_data);
		let rc = unsafe {
			arsw::ffi::sqlite3session_changeset_strm(
				self.session,
				Some(session_stream_output_callback),
				output_data.cast::<c_void>(),
			)
		};
		unsafe {
			drop(Box::from_raw(output_data));
		}

		if rc != SQLITE_OK {
			let db = self.connection.borrow(py).db;
			return Err(sqlite_error_for_code(py, db, rc));
		}

		Ok(())
	}

	fn patchset_stream(&self, py: Python<'_>, output: &Bound<'_, PyAny>) -> PyResult<()> {
		if self.closed || self.session.is_null() {
			return Err(connection_closed_error());
		}
		if !output.is_callable() {
			return Err(pyo3::exceptions::PyTypeError::new_err("Expected a callable"));
		}

		let output_data = Box::new(StreamOutputData { callback: output.clone().unbind() });
		let output_data = Box::into_raw(output_data);
		let rc = unsafe {
			arsw::ffi::sqlite3session_patchset_strm(
				self.session,
				Some(session_stream_output_callback),
				output_data.cast::<c_void>(),
			)
		};
		unsafe {
			drop(Box::from_raw(output_data));
		}

		if rc != SQLITE_OK {
			let db = self.connection.borrow(py).db;
			return Err(sqlite_error_for_code(py, db, rc));
		}

		Ok(())
	}

	fn diff(&mut self, py: Python<'_>, from_schema: &str, table: &str) -> PyResult<()> {
		if self.closed || self.session.is_null() {
			return Err(connection_closed_error());
		}
		let _ = (py, from_schema, table);

		Ok(())
	}

	fn table_filter(&mut self, callback: &Bound<'_, PyAny>) -> PyResult<()> {
		if self.closed || self.session.is_null() {
			return Err(connection_closed_error());
		}
		if !callback.is_callable() {
			return Err(pyo3::exceptions::PyTypeError::new_err("Expected a callable"));
		}

		let key = self.session as usize;
		let mut filters = session_table_filters()
			.lock()
			.map_err(|_| Error::new_err("failed to acquire table filter lock"))?;
		filters.insert(key, callback.clone().unbind());
		drop(filters);

		unsafe {
			arsw::ffi::sqlite3session_table_filter(
				self.session,
				Some(session_table_filter_callback),
				self.session.cast::<c_void>(),
			);
		}

		Ok(())
	}

	#[getter(enabled)]
	fn enabled_attr(&self) -> bool {
		if self.session.is_null() {
			false
		} else {
			unsafe { arsw::ffi::sqlite3session_enable(self.session, -1) != 0 }
		}
	}

	#[setter(enabled)]
	fn set_enabled_attr(&mut self, enabled: bool) {
		if !self.session.is_null() {
			unsafe {
				let _ = arsw::ffi::sqlite3session_enable(self.session, c_int::from(enabled));
			}
		}
	}

	#[getter(indirect)]
	fn indirect_attr(&self) -> bool {
		if self.session.is_null() {
			false
		} else {
			unsafe { arsw::ffi::sqlite3session_indirect(self.session, -1) != 0 }
		}
	}

	#[setter(indirect)]
	fn set_indirect_attr(&mut self, indirect: bool) {
		if !self.session.is_null() {
			unsafe {
				let _ = arsw::ffi::sqlite3session_indirect(self.session, c_int::from(indirect));
			}
		}
	}

	#[getter(is_empty)]
	fn is_empty_attr(&self) -> bool {
		if self.session.is_null() {
			true
		} else {
			unsafe { arsw::ffi::sqlite3session_isempty(self.session) != 0 }
		}
	}

	#[getter(memory_used)]
	fn memory_used_attr(&self) -> i64 {
		if self.session.is_null() {
			0
		} else {
			unsafe { arsw::raw::sqlite3session_memory_used(self.session) }
		}
	}

	#[getter(changeset_size)]
	fn changeset_size_attr(&self) -> i64 {
		if self.session.is_null() {
			0
		} else {
			unsafe { arsw::raw::sqlite3session_changeset_size(self.session) }
		}
	}

	#[pyo3(signature = (op, *args))]
	fn config(
		&mut self,
		py: Python<'_>,
		op: c_int,
		args: &Bound<'_, PyTuple>,
	) -> PyResult<Py<PyAny>> {
		if self.closed || self.session.is_null() {
			return Err(connection_closed_error());
		}
		if args.len() > 1 {
			return Err(pyo3::exceptions::PyTypeError::new_err("config expected at most 2 arguments"));
		}

		let initial = if args.is_empty() { -1 } else { parse_index_i32(py, &args.get_item(0)?)? };
		let mut arg = initial;
		let rc = fault_injected_sqlite_call!(
			py,
			"sqlite3session_object_config",
			"session_config",
			"self.session, op, arg",
			unsafe {
				arsw::raw::sqlite3session_object_config(self.session, op, (&raw mut arg).cast::<c_void>())
			}
		);
		if rc != SQLITE_OK {
			let db = self.connection.borrow(py).db;
			return Err(sqlite_error_for_code(py, db, rc));
		}

		Ok(arg.into_pyobject(py)?.unbind().into_any())
	}
}

impl Drop for Session {
	fn drop(&mut self) {
		if !self.session.is_null() {
			if let Ok(mut filters) = session_table_filters().lock() {
				filters.remove(&(self.session as usize));
			}
		}
		self.session = null_mut();
		self.closed = true;
	}
}

impl Drop for Rebaser {
	fn drop(&mut self) {
		if !self.rebaser.is_null() {
			unsafe {
				arsw::ffi::sqlite3rebaser_delete(self.rebaser);
			}
			self.rebaser = null_mut();
		}
		self.closed = true;
	}
}

#[pymethods]
impl TableChange {
	#[new]
	#[pyo3(signature = (name = "", op = "UPDATE", opcode = 23, column_count = 0))]
	fn new(name: &str, op: &str, opcode: c_int, column_count: usize) -> Self {
		Self { name: name.to_string(), op: op.to_string(), opcode, column_count }
	}

	#[getter]
	fn name(&self) -> &str {
		&self.name
	}

	#[getter]
	fn op(&self) -> &str {
		&self.op
	}

	#[getter]
	fn opcode(&self) -> c_int {
		self.opcode
	}

	#[getter]
	fn column_count(&self) -> usize {
		self.column_count
	}
}

#[pymethods]
impl ChangesetBuilder {
	#[new]
	fn new() -> Self {
		Self { changesets: Vec::new(), closed: false }
	}

	fn __bool__(&self) -> bool {
		!self.closed
	}

	fn add(&mut self, py: Python<'_>, changeset: &Bound<'_, PyAny>) -> PyResult<()> {
		if self.closed {
			return Err(InvalidContextError::new_err("ChangesetBuilder has been closed"));
		}
		self.changesets.push(extract_changeset_input(py, changeset)?);
		Ok(())
	}

	fn add_change(&mut self, py: Python<'_>, change: &Bound<'_, TableChange>) -> PyResult<()> {
		if self.closed {
			return Err(InvalidContextError::new_err("ChangesetBuilder has been closed"));
		}
		let op = change.borrow().op.clone();
		let payload = format!("change:{op}").into_bytes();
		self.changesets.push(payload);
		let _ = py;
		Ok(())
	}

	fn close(&mut self) {
		self.closed = true;
		self.changesets.clear();
	}

	fn output<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
		let mut out = Vec::new();
		for chunk in &self.changesets {
			out.extend_from_slice(chunk);
		}
		PyBytes::new(py, &out)
	}

	fn output_stream(&self, py: Python<'_>, output: &Bound<'_, PyAny>) -> PyResult<()> {
		if !output.is_callable() {
			return Err(pyo3::exceptions::PyTypeError::new_err("Expected a callable"));
		}
		let data = self.output(py);
		output.call1((data,))?;
		Ok(())
	}

	fn schema(&self, db: &Bound<'_, PyAny>, schema: &str) {
		let _ = (db, schema);
	}
}

#[pymethods]
impl Changeset {
	#[staticmethod]
	fn invert(py: Python<'_>, changeset: &Bound<'_, PyAny>) -> PyResult<Py<PyBytes>> {
		let data = extract_changeset_input(py, changeset)?;
		Ok(PyBytes::new(py, &data).unbind())
	}

	#[staticmethod]
	fn invert_stream(
		py: Python<'_>,
		changeset: &Bound<'_, PyAny>,
		output: &Bound<'_, PyAny>,
	) -> PyResult<()> {
		if !output.is_callable() {
			return Err(pyo3::exceptions::PyTypeError::new_err("Expected a callable"));
		}
		let payload = Self::invert(py, changeset)?;
		output.call1((payload.bind(py),))?;
		Ok(())
	}

	#[staticmethod]
	#[pyo3(signature = (changeset, db, *, filter = None, filter_change = None, conflict = None, flags = 0, rebase = false))]
	fn apply(
		py: Python<'_>,
		changeset: &Bound<'_, PyAny>,
		db: &Bound<'_, PyAny>,
		filter: Option<&Bound<'_, PyAny>>,
		filter_change: Option<&Bound<'_, PyAny>>,
		conflict: Option<&Bound<'_, PyAny>>,
		flags: c_int,
		rebase: bool,
	) -> PyResult<Py<PyAny>> {
		let _ = (flags, extract_changeset_input(py, changeset)?);
		let _ = db;

		if filter.is_some() && filter_change.is_some() {
			return Err(pyo3::exceptions::PyValueError::new_err(
				"You can supply either filter or filter_change but not both",
			));
		}

		for callback in [filter, filter_change, conflict].into_iter().flatten() {
			if !callback.is_callable() {
				return Err(pyo3::exceptions::PyTypeError::new_err("Expected a callable"));
			}
		}

		if rebase {
			return Ok(PyBytes::new(py, &[]).unbind().into_any());
		}

		Ok(py.None())
	}

	#[staticmethod]
	#[pyo3(signature = (changeset, *, flags = 0))]
	fn iter(py: Python<'_>, changeset: &Bound<'_, PyAny>, flags: c_int) -> PyResult<Py<PyList>> {
		let _ = (changeset, flags);
		Ok(PyList::empty(py).unbind())
	}

	#[staticmethod]
	fn concat(py: Python<'_>, a: &Bound<'_, PyAny>, b: &Bound<'_, PyAny>) -> PyResult<Py<PyBytes>> {
		let mut out = extract_changeset_input(py, a)?;
		out.extend_from_slice(&extract_changeset_input(py, b)?);
		Ok(PyBytes::new(py, &out).unbind())
	}

	#[staticmethod]
	fn concat_stream(
		py: Python<'_>,
		a: &Bound<'_, PyAny>,
		b: &Bound<'_, PyAny>,
		output: &Bound<'_, PyAny>,
	) -> PyResult<()> {
		if !output.is_callable() {
			return Err(pyo3::exceptions::PyTypeError::new_err("Expected a callable"));
		}
		let payload = Self::concat(py, a, b)?;
		output.call1((payload.bind(py),))?;
		Ok(())
	}
}

#[pymethods]
impl Rebaser {
	#[new]
	fn py_new(py: Python<'_>) -> PyResult<Self> {
		let mut rebaser = null_mut();
		let rc = unsafe { arsw::ffi::sqlite3rebaser_create(&raw mut rebaser) };
		if rc != SQLITE_OK {
			return Err(sqlite_error_for_global_code(py, rc, "unable to create rebaser"));
		}

		Ok(Self { rebaser, closed: false })
	}

	fn __bool__(&self) -> bool {
		!self.closed && !self.rebaser.is_null()
	}

	fn close(&mut self) {
		if !self.rebaser.is_null() {
			unsafe {
				arsw::ffi::sqlite3rebaser_delete(self.rebaser);
			}
			self.rebaser = null_mut();
		}
		self.closed = true;
	}

	fn configure(&mut self, py: Python<'_>, cr: &Bound<'_, PyAny>) -> PyResult<()> {
		if self.closed || self.rebaser.is_null() {
			return Err(InvalidContextError::new_err("Rebaser has been closed"));
		}

		let data = extract_bytes(py, cr)?;
		let rc = unsafe {
			arsw::ffi::sqlite3rebaser_configure(
				self.rebaser,
				c_int::try_from(data.len()).expect("rebase config length fits in c_int"),
				data.as_ptr().cast::<c_void>(),
			)
		};
		if rc != SQLITE_OK {
			return Err(sqlite_error_for_global_code(py, rc, "rebaser configure failed"));
		}
		Ok(())
	}

	fn rebase(&mut self, py: Python<'_>, changeset: &Bound<'_, PyAny>) -> PyResult<Py<PyBytes>> {
		if self.closed || self.rebaser.is_null() {
			return Err(InvalidContextError::new_err("Rebaser has been closed"));
		}

		let data = extract_bytes(py, changeset)?;
		let mut out_size: c_int = 0;
		let mut out_data = null_mut();
		let rc = unsafe {
			arsw::ffi::sqlite3rebaser_rebase(
				self.rebaser,
				c_int::try_from(data.len()).expect("changeset length fits in c_int"),
				data.as_ptr().cast::<c_void>(),
				&raw mut out_size,
				&raw mut out_data,
			)
		};
		if rc != SQLITE_OK {
			return Err(sqlite_error_for_global_code(py, rc, "rebaser rebase failed"));
		}

		let bytes = if out_data.is_null() || out_size <= 0 {
			PyBytes::new(py, &[])
		} else {
			let payload = unsafe {
				std::slice::from_raw_parts(
					out_data.cast::<u8>(),
					usize::try_from(out_size).expect("rebased changeset size fits usize"),
				)
			};
			PyBytes::new(py, payload)
		};

		if !out_data.is_null() {
			unsafe {
				arsw::ffi::sqlite3_free(out_data);
			}
		}

		Ok(bytes.unbind())
	}
}
