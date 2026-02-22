use super::*;

#[pyclass(module = "apsw", name = "zeroblob")]
pub(crate) struct ZeroBlob {
	pub(crate) length: usize,
}

#[pymethods]
impl ZeroBlob {
	#[new]
	fn py_new(length: usize) -> Self {
		Self { length }
	}

	fn length(&self) -> usize {
		self.length
	}
}

#[pyclass(module = "apsw", subclass)]
pub(crate) struct Blob {
	pub(crate) connection: Py<Connection>,
	pub(crate) blob: *mut arsw::ffi::Sqlite3Blob,
	pub(crate) closed: bool,
	pub(crate) position: usize,
	pub(crate) writable: bool,
	pub(crate) readonly_write_attempted: bool,
}

unsafe impl Send for Blob {}
unsafe impl Sync for Blob {}

impl Drop for Blob {
	fn drop(&mut self) {
		if !self.blob.is_null() {
			unsafe {
				arsw::ffi::sqlite3_blob_close(self.blob);
			}
			self.blob = null_mut();
		}
		self.closed = true;
	}
}

fn blob_closed_error() -> PyErr {
	pyo3::exceptions::PyValueError::new_err("Blob has been closed")
}

#[pymethods]
impl Blob {
	fn __enter__(slf: PyRef<'_, Self>) -> Py<Self> {
		slf.into()
	}

	fn __bool__(&self) -> bool {
		!self.closed && !self.blob.is_null()
	}

	fn __exit__(
		&mut self,
		py: Python<'_>,
		_etype: Option<&Bound<'_, PyAny>>,
		_evalue: Option<&Bound<'_, PyAny>>,
		_etraceback: Option<&Bound<'_, PyAny>>,
	) -> PyResult<bool> {
		self.close(py, false)?;
		Ok(false)
	}

	#[pyo3(signature = (force = false))]
	fn close(&mut self, py: Python<'_>, force: bool) -> PyResult<()> {
		if self.blob.is_null() {
			self.closed = true;
			return Ok(());
		}

		let blob = self.blob;
		self.blob = null_mut();
		self.closed = true;
		self.position = 0;
		let rc = fault_injected_sqlite_call!(py, "sqlite3_blob_close", "blob_close", "blob", unsafe {
			arsw::ffi::sqlite3_blob_close(blob)
		});
		if rc != SQLITE_OK && !force {
			let db = self.connection.borrow(py).db;
			return Err(sqlite_error_for_code(py, db, rc));
		}
		if self.readonly_write_attempted && !force {
			return Err(ReadOnlyError::new_err("Blob is read-only"));
		}

		Ok(())
	}

	fn length(&self) -> PyResult<usize> {
		if self.closed || self.blob.is_null() {
			return Err(blob_closed_error());
		}

		let bytes = unsafe { arsw::ffi::sqlite3_blob_bytes(self.blob) };
		Ok(usize::try_from(bytes).unwrap_or(0))
	}

	fn tell(&self) -> PyResult<usize> {
		if self.closed || self.blob.is_null() {
			return Err(blob_closed_error());
		}
		Ok(self.position)
	}

	#[pyo3(signature = (offset, whence = None))]
	fn seek(
		&mut self,
		py: Python<'_>,
		offset: &Bound<'_, PyAny>,
		whence: Option<&Bound<'_, PyAny>>,
	) -> PyResult<()> {
		if self.closed || self.blob.is_null() {
			return Err(blob_closed_error());
		}
		let offset = parse_index_i64(py, offset)?;
		let whence = match whence {
			Some(value) => parse_index_i32(py, value)?,
			None => 0,
		};
		let length = i64::try_from(self.length()?).expect("blob length fits in i64");
		let current = i64::try_from(self.position).expect("blob position fits in i64");
		let new_position = match whence {
			0 => offset,
			1 => current + offset,
			2 => length + offset,
			_ => {
				return Err(pyo3::exceptions::PyValueError::new_err("whence must be 0, 1, or 2"));
			}
		};

		if !(0..=length).contains(&new_position) {
			return Err(pyo3::exceptions::PyValueError::new_err("Resulting offset is out of bounds"));
		}

		self.position =
			usize::try_from(new_position).expect("bounds check ensures non-negative position");
		Ok(())
	}

	#[pyo3(signature = (length = None))]
	fn read(&mut self, py: Python<'_>, length: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyBytes>> {
		if self.closed || self.blob.is_null() {
			return Err(blob_closed_error());
		}
		let length = match length {
			Some(length) => parse_index_i64(py, length)?,
			None => -1,
		};
		if fault_should_trigger(py, "ConnectionReadError")? {
			return Err(pyo3::exceptions::PyOSError::new_err("Fault injection synthesized failure"));
		}

		let available = self.length()?.saturating_sub(self.position);
		let requested =
			if length < 0 { available } else { usize::try_from(length).unwrap_or(0).min(available) };

		if requested == 0 {
			return Ok(PyBytes::new(py, &[]).unbind());
		}

		let mut buffer = vec![0_u8; requested];
		let rc = fault_injected_sqlite_call!(
			py,
			"sqlite3_blob_read",
			"blob_read",
			"self.blob, out buffer, requested, position",
			unsafe {
				arsw::ffi::sqlite3_blob_read(
					self.blob,
					buffer.as_mut_ptr().cast(),
					c_int::try_from(requested).expect("requested bytes fit in c_int"),
					c_int::try_from(self.position).expect("blob position fits in c_int"),
				)
			}
		);
		if rc != SQLITE_OK {
			let db = self.connection.borrow(py).db;
			return Err(sqlite_error_for_code(py, db, rc));
		}

		self.position += requested;
		Ok(PyBytes::new(py, &buffer).unbind())
	}

	#[pyo3(signature = (buffer, offset = 0, length = -1))]
	fn read_into(
		&mut self,
		py: Python<'_>,
		buffer: &Bound<'_, PyAny>,
		offset: i64,
		length: i64,
	) -> PyResult<()> {
		if self.closed || self.blob.is_null() {
			return Err(blob_closed_error());
		}
		if offset < 0 {
			return Err(pyo3::exceptions::PyValueError::new_err("offset must be non-negative"));
		}
		if length < -1 {
			return Err(pyo3::exceptions::PyValueError::new_err("length must be -1 or non-negative"));
		}
		let offset = usize::try_from(offset).expect("offset checked as non-negative");
		let memoryview_type = PyModule::import(py, "builtins")?.getattr("memoryview")?;
		let memoryview = memoryview_type.call1((buffer,))?;
		let total_length = memoryview.len()?;
		if offset > total_length {
			return Err(pyo3::exceptions::PyValueError::new_err("offset is beyond end of buffer"));
		}

		let remaining = total_length - offset;
		let amount = if length < 0 {
			if offset != 0 {
				return Err(pyo3::exceptions::PyValueError::new_err(
					"offset must be zero when length is -1",
				));
			}
			remaining
		} else {
			usize::try_from(length).unwrap_or(0)
		};

		if amount > remaining {
			return Err(pyo3::exceptions::PyValueError::new_err(
				"requested read length exceeds remaining buffer space",
			));
		}

		let amount_obj =
			i64::try_from(amount).expect("amount fits in i64").into_pyobject(py)?.into_any();
		let bytes = self.read(py, Some(&amount_obj))?;
		for (index, byte) in bytes.bind(py).as_bytes().iter().enumerate() {
			memoryview.set_item(offset + index, byte)?;
		}
		Ok(())
	}

	#[pyo3(signature = (buffer = None, offset = 0, length = -1))]
	fn readinto(
		&mut self,
		py: Python<'_>,
		buffer: Option<&Bound<'_, PyAny>>,
		offset: i64,
		length: i64,
	) -> PyResult<()> {
		if self.closed || self.blob.is_null() {
			return Err(blob_closed_error());
		}
		let Some(buffer) = buffer else {
			return Err(pyo3::exceptions::PyTypeError::new_err("buffer must be supplied"));
		};
		self.read_into(py, buffer, offset, length)
	}

	fn reopen(&mut self, py: Python<'_>, rowid: &Bound<'_, PyAny>) -> PyResult<()> {
		if self.closed || self.blob.is_null() {
			return Err(blob_closed_error());
		}
		let rowid = parse_index_i64(py, rowid)?;

		let rc = fault_injected_sqlite_call!(
			py,
			"sqlite3_blob_reopen",
			"blob_reopen",
			"self.blob, rowid",
			unsafe { arsw::ffi::sqlite3_blob_reopen(self.blob, rowid) }
		);
		if rc != SQLITE_OK {
			let db = self.connection.borrow(py).db;
			return Err(sqlite_error_for_code(py, db, rc));
		}

		self.position = 0;
		Ok(())
	}

	fn write(&mut self, py: Python<'_>, data: &Bound<'_, PyAny>) -> PyResult<()> {
		if self.closed || self.blob.is_null() {
			return Err(blob_closed_error());
		}
		if !self.writable {
			self.readonly_write_attempted = true;
			return Err(ReadOnlyError::new_err("Blob is read-only"));
		}

		if fault_should_trigger(py, "BlobWriteTooBig")? {
			return Err(pyo3::exceptions::PyValueError::new_err(
				"Data would extend beyond the end of the blob",
			));
		}

		let bytes = if let Ok(bytes) = data.cast::<PyBytes>() {
			bytes.as_bytes().to_vec()
		} else if let Ok(bytes) = data.cast::<PyByteArray>() {
			unsafe { bytes.as_bytes().to_vec() }
		} else {
			return Err(pyo3::exceptions::PyTypeError::new_err("data must support the buffer protocol"));
		};

		let length = self.length()?;
		if self.position + bytes.len() > length {
			return Err(pyo3::exceptions::PyValueError::new_err(
				"Data would extend beyond the end of the blob",
			));
		}

		let rc = fault_injected_sqlite_call!(
			py,
			"sqlite3_blob_write",
			"blob_write",
			"self.blob, bytes, bytes.len(), position",
			unsafe {
				arsw::ffi::sqlite3_blob_write(
					self.blob,
					bytes.as_ptr().cast(),
					c_int::try_from(bytes.len()).expect("bytes length fits in c_int"),
					c_int::try_from(self.position).expect("blob position fits in c_int"),
				)
			}
		);
		if rc != SQLITE_OK {
			let db = self.connection.borrow(py).db;
			return Err(sqlite_error_for_code(py, db, rc));
		}

		self.position += bytes.len();
		Ok(())
	}
}
