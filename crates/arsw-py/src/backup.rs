use super::*;

#[pyclass(module = "apsw", subclass)]
pub(crate) struct Backup {
	pub(crate) connection: Py<Connection>,
	pub(crate) _source_connection: Py<Connection>,
	pub(crate) backup: *mut arsw::ffi::Sqlite3Backup,
	pub(crate) done: bool,
	pub(crate) closed: bool,
}

unsafe impl Send for Backup {}
unsafe impl Sync for Backup {}

impl Drop for Backup {
	fn drop(&mut self) {
		if !self.backup.is_null() {
			let backup = self.backup;
			let rc = unsafe { arsw::ffi::sqlite3_backup_finish(backup) };
			if rc != SQLITE_OK {
				let _ = Python::try_attach(|py| {
					let db = self.connection.borrow(py).db;
					let err = sqlite_error_for_code(py, db, rc);
					err.write_unraisable(py, None);
				});
			}
			self.backup = null_mut();
		}
		self.closed = true;
	}
}

#[pymethods]
impl Backup {
	fn __enter__(slf: PyRef<'_, Self>) -> Py<Self> {
		slf.into()
	}

	fn __bool__(&self) -> bool {
		!self.closed && !self.backup.is_null()
	}

	fn __exit__(
		&mut self,
		py: Python<'_>,
		_etype: Option<&Bound<'_, PyAny>>,
		_evalue: Option<&Bound<'_, PyAny>>,
		_etraceback: Option<&Bound<'_, PyAny>>,
	) -> PyResult<bool> {
		self.finish(py)?;
		Ok(false)
	}

	#[getter(done)]
	fn done_attr(&self) -> bool {
		self.done
	}

	#[getter(page_count)]
	fn page_count_attr(&self) -> i32 {
		if self.backup.is_null() {
			0
		} else {
			unsafe { arsw::ffi::sqlite3_backup_pagecount(self.backup) }
		}
	}

	#[getter(remaining)]
	fn remaining_attr(&self) -> i32 {
		if self.backup.is_null() {
			0
		} else {
			unsafe { arsw::ffi::sqlite3_backup_remaining(self.backup) }
		}
	}

	fn pagecount(&self) -> i32 {
		self.page_count_attr()
	}

	#[pyo3(signature = (npages = -1))]
	fn step(&mut self, py: Python<'_>, npages: c_int) -> PyResult<bool> {
		if self.closed || self.backup.is_null() {
			self.done = true;
			return Err(connection_closed_error());
		}

		let rc = fault_injected_sqlite_call!(
			py,
			"sqlite3_backup_step",
			"backup_step",
			"self.backup, npages",
			unsafe { arsw::ffi::sqlite3_backup_step(self.backup, npages) }
		);
		match rc {
			SQLITE_OK => {
				self.done = false;
				Ok(false)
			}
			SQLITE_DONE => {
				self.done = true;
				Ok(true)
			}
			SQLITE_BUSY | SQLITE_LOCKED => {
				let db = self.connection.borrow(py).db;
				Err(sqlite_error_for_code(py, db, rc))
			}
			_ => {
				let db = self.connection.borrow(py).db;
				Err(sqlite_error_for_code(py, db, rc))
			}
		}
	}

	fn finish(&mut self, py: Python<'_>) -> PyResult<()> {
		if self.backup.is_null() {
			self.closed = true;
			return Ok(());
		}

		let backup = self.backup;
		self.backup = null_mut();
		let rc = fault_injected_sqlite_call!(
			py,
			"sqlite3_backup_finish",
			"backup_finish",
			"backup handle",
			unsafe { arsw::ffi::sqlite3_backup_finish(backup) }
		);
		self.closed = true;
		self.done = true;

		if rc != SQLITE_OK {
			let db = self.connection.borrow(py).db;
			return Err(sqlite_error_for_code(py, db, rc));
		}

		Ok(())
	}

	#[pyo3(signature = (force = false))]
	fn close(&mut self, py: Python<'_>, force: bool) -> PyResult<()> {
		if self.backup.is_null() {
			self.closed = true;
			self.done = true;
			return Ok(());
		}

		let backup = self.backup;
		self.backup = null_mut();
		let rc = fault_injected_sqlite_call!(
			py,
			"sqlite3_backup_finish",
			"backup_close",
			"backup handle",
			unsafe { arsw::ffi::sqlite3_backup_finish(backup) }
		);
		self.closed = true;
		self.done = true;
		if rc != SQLITE_OK && !force {
			let db = self.connection.borrow(py).db;
			return Err(sqlite_error_for_code(py, db, rc));
		}

		Ok(())
	}
}
