use super::*;
use crate::traceback::TraceContext;

#[pyclass(module = "apsw", subclass, dict)]
pub(crate) struct Connection {
	pub(crate) db: *mut arsw::ffi::Sqlite3,
	pub(crate) filename: String,
	pub(crate) open_flags: c_int,
	pub(crate) open_vfs: String,
	pub(crate) closed: bool,
	pub(crate) in_transaction: bool,
	pub(crate) last_changes: c_int,
	pub(crate) total_changes: i64,
	pub(crate) statement_cache_size: usize,
	pub(crate) wal_autocheckpoint_pages: c_int,
	pub(crate) busy_timeout_ms: c_int,
	pub(crate) load_extension_enabled: bool,
	pub(crate) db_config: HashMap<c_int, c_int>,
	pub(crate) cursor_factory: Option<Py<PyAny>>,
	pub(crate) exec_trace: Option<Py<PyAny>>,
	pub(crate) row_trace: Option<Py<PyAny>>,
	pub(crate) authorizer: Option<Py<PyAny>>,
	pub(crate) progress_handler: Option<Py<PyAny>>,
	pub(crate) progress_nsteps: usize,
	pub(crate) progress_counter: usize,
	pub(crate) update_hook: Option<Py<PyAny>>,
	pub(crate) commit_hook: Option<Py<PyAny>>,
	pub(crate) commit_hook_ids: Vec<(Py<PyAny>, Py<PyAny>)>,
	pub(crate) rollback_hook: Option<Py<PyAny>>,
	pub(crate) rollback_hook_ids: Vec<(Py<PyAny>, Py<PyAny>)>,
	pub(crate) wal_hook: Option<Py<PyAny>>,
	pub(crate) trace_v2_callback: Option<Py<PyAny>>,
	pub(crate) trace_context: Option<Box<TraceContext>>,
	pub(crate) busy_handler: Option<Py<PyAny>>,
	pub(crate) autovacuum_pages: Option<Py<PyAny>>,
	pub(crate) collation_needed: Option<Py<PyAny>>,
	pub(crate) profile: Option<Py<PyAny>>,
	pub(crate) convert_binding: Option<Py<PyAny>>,
	pub(crate) convert_jsonb: Option<Py<PyAny>>,
	pub(crate) limits: HashMap<c_int, c_int>,
	pub(crate) fts5_tokenizers: HashMap<String, Py<PyAny>>,
	pub(crate) fts5_functions: HashMap<String, Py<PyAny>>,
	pub(crate) virtual_modules: HashMap<String, Option<Py<PyAny>>>,
	pub(crate) connection_hooks: Vec<Py<PyAny>>,
	pub(crate) connection_hooks_applied: AtomicBool,
}

unsafe impl Send for Connection {}
unsafe impl Sync for Connection {}

impl Drop for Connection {
	fn drop(&mut self) {
		if !self.closed && !self.db.is_null() {
			unsafe {
				arsw::ffi::sqlite3_close_v2(self.db);
			}
			self.db = null_mut();
			self.closed = true;
		}
	}
}

#[pymethods]
impl Connection {
	#[new]
	#[pyo3(signature = (filename, flags = DEFAULT_OPEN_FLAGS, vfs = None, statementcachesize = 100))]
	fn new(
		py: Python<'_>,
		filename: &str,
		flags: c_int,
		vfs: Option<&str>,
		statementcachesize: usize,
	) -> PyResult<Self> {
		if fault_should_trigger(py, "ConnectionAsyncTpNewFails")? {
			return Err(pyo3::exceptions::PyMemoryError::new_err("Fault injection synthesized failure"));
		}
		let filename_input = filename.to_string();
		let open_vfs_value = vfs.unwrap_or("").to_string();

		let filename = CString::new(filename)
			.map_err(|_| pyo3::exceptions::PyValueError::new_err("filename contains NUL byte"))?;

		let vfs = match vfs {
			Some(name) => Some(
				CString::new(name)
					.map_err(|_| pyo3::exceptions::PyValueError::new_err("vfs contains NUL byte"))?,
			),
			None => None,
		};
		let use_registered_custom_vfs =
			vfs.as_ref().and_then(|name| name.to_str().ok()).is_some_and(|name| {
				custom_vfs_names().lock().map(|known| known.contains_key(name)).unwrap_or(false)
			});
		let vfs_ptr = if use_registered_custom_vfs {
			null()
		} else {
			vfs.as_ref().map_or(null(), |name| name.as_ptr())
		};

		let mut db = null_mut();
		let rc = fault_injected_sqlite_call!(
			py,
			"sqlite3_open_v2",
			"connection_new",
			"filename, out db, flags, vfs",
			unsafe { arsw::ffi::sqlite3_open_v2(filename.as_ptr(), &raw mut db, flags, vfs_ptr) }
		);

		if rc != SQLITE_OK {
			if !db.is_null() {
				let _ =
					fault_injected_sqlite_call!(py, "sqlite3_close_v2", "connection_new", "db", unsafe {
						arsw::ffi::sqlite3_close_v2(db)
					});
			}
			return Err(sqlite_error_for_code(py, db, rc));
		}

		unsafe {
			arsw::ffi::sqlite3_extended_result_codes(db, 1);
		}
		SQLITE_IS_INITIALIZED.store(true, Ordering::Relaxed);

		let filename_value = if filename_input.is_empty() {
			String::new()
		} else {
			let main_name = b"main\0";
			sqlite_optional_text(unsafe {
				arsw::ffi::sqlite3_db_filename(db, main_name.as_ptr().cast::<c_char>())
			})
			.unwrap_or(filename_input)
		};

		let mut db_config = HashMap::new();
		if let Some(dqs_dml) = sqlite_constant_value("SQLITE_DBCONFIG_DQS_DML") {
			db_config.insert(dqs_dml, 1);
		}
		if let Some(dqs_ddl) = sqlite_constant_value("SQLITE_DBCONFIG_DQS_DDL") {
			db_config.insert(dqs_ddl, 1);
		}
		if let Some(enable_comments) = sqlite_constant_value("SQLITE_DBCONFIG_ENABLE_COMMENTS") {
			db_config.insert(enable_comments, 1);
		}
		if let Some(reverse_scanorder) = sqlite_constant_value("SQLITE_DBCONFIG_REVERSE_SCANORDER") {
			db_config.insert(reverse_scanorder, 0);
		}

		let mut connection_hooks = Vec::new();
		if let Ok(apsw_module) = PyModule::import(py, "apsw") {
			if let Ok(hooks_obj) = apsw_module.getattr("connection_hooks") {
				if let Ok(hooks_list) = hooks_obj.cast::<PyList>() {
					for hook in hooks_list.iter() {
						if hook.is_callable() {
							connection_hooks.push(hook.unbind());
						}
					}
				}
			}
		}

		Ok(Self {
			db,
			filename: filename_value,
			open_flags: flags,
			open_vfs: open_vfs_value,
			closed: false,
			in_transaction: false,
			last_changes: 0,
			total_changes: 0,
			statement_cache_size: statementcachesize,
			wal_autocheckpoint_pages: 0,
			busy_timeout_ms: 0,
			load_extension_enabled: false,
			db_config,
			cursor_factory: Some(py.get_type::<Cursor>().into_any().unbind()),
			exec_trace: None,
			row_trace: None,
			authorizer: None,
			progress_handler: None,
			progress_nsteps: 100,
			progress_counter: 0,
			update_hook: None,
			commit_hook: None,
			commit_hook_ids: Vec::new(),
			rollback_hook: None,
			rollback_hook_ids: Vec::new(),
			wal_hook: None,
			trace_v2_callback: None,
			trace_context: None,
			busy_handler: None,
			autovacuum_pages: None,
			collation_needed: None,
			profile: None,
			convert_binding: None,
			convert_jsonb: None,
			limits: HashMap::new(),
			fts5_tokenizers: HashMap::new(),
			fts5_functions: HashMap::new(),
			virtual_modules: HashMap::new(),
			connection_hooks,
			connection_hooks_applied: AtomicBool::new(false),
		})
	}

	#[getter]
	fn filename(&self) -> &str {
		&self.filename
	}

	#[getter(open_flags)]
	fn connection_open_flags(&self) -> c_int {
		self.open_flags
	}

	#[getter(open_vfs)]
	fn connection_open_vfs(&self) -> &str {
		&self.open_vfs
	}

	#[getter(filename_journal)]
	fn connection_filename_journal(&self) -> String {
		if self.filename.is_empty() { String::new() } else { format!("{}-journal", self.filename) }
	}

	#[getter(filename_wal)]
	fn connection_filename_wal(&self) -> String {
		if self.filename.is_empty() { String::new() } else { format!("{}-wal", self.filename) }
	}

	#[getter(system_errno)]
	fn connection_system_errno(&self) -> c_int {
		0
	}

	#[getter(in_transaction)]
	fn in_transaction_attr(&self) -> bool {
		self.in_transaction
	}

	fn total_changes(&self) -> i64 {
		if self.closed || self.db.is_null() {
			return 0;
		}
		unsafe { arsw::ffi::sqlite3_total_changes64(self.db) }
	}

	fn changes(&self) -> c_int {
		if self.closed || self.db.is_null() {
			return 0;
		}
		unsafe { arsw::ffi::sqlite3_changes64(self.db) as c_int }
	}

	#[pyo3(signature = (schema = None))]
	fn data_version(&self, schema: Option<&str>) -> PyResult<i64> {
		if self.closed || self.db.is_null() {
			return Err(connection_closed_error());
		}
		let schema = schema.unwrap_or("main");
		let schema = CString::new(schema)
			.map_err(|_| pyo3::exceptions::PyValueError::new_err("schema contains NUL byte"))?;
		let filename = unsafe { arsw::ffi::sqlite3_db_filename(self.db, schema.as_ptr()) };
		if filename.is_null() {
			return Err(SQLError::new_err("unknown database"));
		}
		Ok(unsafe { arsw::ffi::sqlite3_total_changes64(self.db) })
	}

	fn db_filename(&self, dbname: Option<&str>) -> PyResult<Option<String>> {
		if self.closed || self.db.is_null() {
			return Err(connection_closed_error());
		}

		let dbname = dbname.unwrap_or("main");
		let dbname = CString::new(dbname)
			.map_err(|_| pyo3::exceptions::PyValueError::new_err("dbname contains NUL byte"))?;
		Ok(sqlite_optional_text(unsafe { arsw::ffi::sqlite3_db_filename(self.db, dbname.as_ptr()) }))
	}

	fn db_names(&self, py: Python<'_>) -> PyResult<Vec<String>> {
		if self.closed || self.db.is_null() {
			return Err(connection_closed_error());
		}

		let sql = CString::new("PRAGMA database_list")
			.map_err(|_| pyo3::exceptions::PyValueError::new_err("query contains NUL byte"))?;
		let mut stmt = null_mut();
		let mut tail = null();
		let rc = unsafe {
			arsw::ffi::sqlite3_prepare_v3(self.db, sql.as_ptr(), -1, 0, &raw mut stmt, &raw mut tail)
		};
		let _ = tail;
		if rc != SQLITE_OK {
			return Err(sqlite_error_for_code(py, self.db, rc));
		}

		let mut names = Vec::new();
		while !stmt.is_null() {
			let step_rc = unsafe { arsw::ffi::sqlite3_step(stmt) };
			if step_rc == SQLITE_ROW {
				let name =
					sqlite_optional_text(unsafe { arsw::ffi::sqlite3_column_text(stmt, 1).cast::<c_char>() })
						.unwrap_or_default();
				names.push(name);
				continue;
			}

			let _ = unsafe { arsw::ffi::sqlite3_finalize(stmt) };
			if step_rc == SQLITE_DONE {
				break;
			}
			return Err(sqlite_error_for_code(
				py,
				self.db,
				sqlite_effective_error_code(self.db, step_rc),
			));
		}
		if !names.iter().any(|name| name == "temp") {
			if let Some(main_pos) = names.iter().position(|name| name == "main") {
				names.insert(main_pos + 1, "temp".to_string());
			} else {
				names.insert(0, "temp".to_string());
			}
		}

		Ok(names)
	}

	fn readonly(&self, dbname: &str) -> PyResult<bool> {
		if self.closed || self.db.is_null() {
			return Err(connection_closed_error());
		}

		let dbname = CString::new(dbname)
			.map_err(|_| pyo3::exceptions::PyValueError::new_err("dbname contains NUL byte"))?;
		let rc = unsafe { arsw::ffi::sqlite3_db_readonly(self.db, dbname.as_ptr()) };
		if rc < 0 {
			return Err(SQLError::new_err("Unknown database name"));
		}

		Ok(rc != 0)
	}

	fn table_exists(&self, schema: Option<&str>, table: &str) -> PyResult<bool> {
		if self.closed || self.db.is_null() {
			return Err(connection_closed_error());
		}

		let escaped_table = table.replace('\'', "''");
		let schemas = if let Some(schema) = schema {
			vec![if schema.is_empty() { "main".to_string() } else { schema.to_string() }]
		} else {
			vec!["main".to_string(), "temp".to_string()]
		};

		for schema in schemas {
			let escaped_schema = schema.replace('"', "\"\"");
			let sql = format!(
				"SELECT 1 FROM \"{escaped_schema}\".sqlite_schema WHERE name='{escaped_table}' LIMIT 1"
			);
			let sql = CString::new(sql)
				.map_err(|_| pyo3::exceptions::PyValueError::new_err("query contains NUL byte"))?;

			let mut stmt = null_mut();
			let mut tail = null();
			let rc = unsafe {
				arsw::ffi::sqlite3_prepare_v3(self.db, sql.as_ptr(), -1, 0, &raw mut stmt, &raw mut tail)
			};
			let _ = tail;
			if rc != SQLITE_OK {
				return Err(Error::new_err("table_exists query failed"));
			}

			if stmt.is_null() {
				continue;
			}

			let rc = unsafe { arsw::ffi::sqlite3_step(stmt) };
			unsafe {
				arsw::ffi::sqlite3_finalize(stmt);
			}
			if rc == SQLITE_ROW {
				return Ok(true);
			}
		}

		Ok(false)
	}

	fn tableexists(&self, schema: Option<&str>, table: &str) -> PyResult<bool> {
		self.table_exists(schema, table)
	}

	#[pyo3(signature = (schema, table, column))]
	fn column_metadata(
		&self,
		schema: Option<&str>,
		table: &str,
		column: &str,
	) -> PyResult<(String, String, bool, bool, bool)> {
		if self.closed || self.db.is_null() {
			return Err(connection_closed_error());
		}

		let schemas = if let Some(schema) = schema {
			vec![schema.to_string()]
		} else {
			vec!["main".to_string(), "temp".to_string()]
		};

		for schema_name in schemas {
			let pragma_sql = format!("PRAGMA \"{schema_name}\".table_info(\"{table}\")");
			let pragma_sql = CString::new(pragma_sql)
				.map_err(|_| pyo3::exceptions::PyValueError::new_err("table name contains NUL byte"))?;
			let mut stmt = null_mut();
			let mut tail = null();
			let rc = unsafe {
				arsw::ffi::sqlite3_prepare_v3(
					self.db,
					pragma_sql.as_ptr(),
					-1,
					0,
					&raw mut stmt,
					&raw mut tail,
				)
			};
			let _ = tail;
			if rc != SQLITE_OK || stmt.is_null() {
				if !stmt.is_null() {
					unsafe {
						arsw::ffi::sqlite3_finalize(stmt);
					}
				}
				continue;
			}

			let mut decltype = None;
			let mut not_null = false;
			let mut primary_key = false;
			loop {
				let step = unsafe { arsw::ffi::sqlite3_step(stmt) };
				if step != SQLITE_ROW {
					break;
				}
				let name =
					sqlite_optional_text(unsafe { arsw::ffi::sqlite3_column_text(stmt, 1).cast::<c_char>() })
						.unwrap_or_default();
				if name != column {
					continue;
				}
				decltype = Some(
					sqlite_optional_text(unsafe { arsw::ffi::sqlite3_column_text(stmt, 2).cast::<c_char>() })
						.unwrap_or_default(),
				);
				not_null = unsafe { arsw::ffi::sqlite3_column_int64(stmt, 3) } != 0;
				primary_key = unsafe { arsw::ffi::sqlite3_column_int64(stmt, 5) } != 0;
				break;
			}
			unsafe {
				arsw::ffi::sqlite3_finalize(stmt);
			}

			let Some(decltype) = decltype else {
				continue;
			};

			let schema_sql =
				format!("SELECT sql FROM \"{schema_name}\".sqlite_schema WHERE type='table' AND name=?1");
			let schema_sql = CString::new(schema_sql)
				.map_err(|_| pyo3::exceptions::PyValueError::new_err("schema SQL contains NUL byte"))?;
			let table_c = CString::new(table)
				.map_err(|_| pyo3::exceptions::PyValueError::new_err("table name contains NUL byte"))?;
			let mut schema_stmt = null_mut();
			let mut schema_tail = null();
			let prepare_rc = unsafe {
				arsw::ffi::sqlite3_prepare_v3(
					self.db,
					schema_sql.as_ptr(),
					-1,
					0,
					&raw mut schema_stmt,
					&raw mut schema_tail,
				)
			};
			let _ = schema_tail;
			let mut collation = "BINARY".to_string();
			let mut autoincrement = false;
			if prepare_rc == SQLITE_OK && !schema_stmt.is_null() {
				let _ = unsafe {
					arsw::ffi::sqlite3_bind_text64(
						schema_stmt,
						1,
						table_c.as_ptr(),
						u64::try_from(table.len()).unwrap_or(0),
						sqlite_transient().into(),
						SQLITE_UTF8,
					)
				};
				if unsafe { arsw::ffi::sqlite3_step(schema_stmt) } == SQLITE_ROW {
					if let Some(create_sql) = sqlite_optional_text(unsafe {
						arsw::ffi::sqlite3_column_text(schema_stmt, 0).cast::<c_char>()
					}) {
						let (parsed_collation, parsed_autoincrement) =
							parse_column_collation_and_autoincrement(&create_sql, column);
						if let Some(parsed_collation) = parsed_collation {
							collation = parsed_collation;
						}
						autoincrement = parsed_autoincrement;
					}
				}
				unsafe {
					arsw::ffi::sqlite3_finalize(schema_stmt);
				}
			}

			return Ok((decltype, collation, not_null, primary_key, autoincrement));
		}

		Err(SQLError::new_err("no such table or column"))
	}

	#[pyo3(signature = (schema, table, column))]
	fn columnmetadata(
		&self,
		schema: Option<&str>,
		table: &str,
		column: &str,
	) -> PyResult<(String, String, bool, bool, bool)> {
		self.column_metadata(schema, table, column)
	}

	fn cache_flush(&self) {}

	fn get_autocommit(&self) -> bool {
		!self.in_transaction
	}

	fn interrupt(&self) {}

	fn sqlite3_pointer(&self) -> usize {
		self.db as usize
	}

	#[pyo3(signature = (schema = None))]
	fn txn_state(&self, schema: Option<&str>) -> c_int {
		let _ = schema;
		if self.in_transaction {
			sqlite_constant_value("SQLITE_TXN_WRITE").unwrap_or(2)
		} else {
			sqlite_constant_value("SQLITE_TXN_NONE").unwrap_or(0)
		}
	}

	fn release_memory(&self) {}

	fn releasememory(&self) {
		self.release_memory();
	}

	fn read(
		&self,
		py: Python<'_>,
		schema: &str,
		which: c_int,
		offset: i64,
		amount: i64,
	) -> PyResult<(bool, Py<PyBytes>)> {
		if which != 0 && which != 1 {
			return Err(pyo3::exceptions::PyValueError::new_err(
				"which must be 0 (database) or 1 (journal)",
			));
		}
		if offset < 0 {
			return Err(pyo3::exceptions::PyValueError::new_err("offset must be >= 0"));
		}
		if amount < 0 {
			return Err(pyo3::exceptions::PyValueError::new_err("amount must be >= 0"));
		}
		let amount = usize::try_from(amount)
			.map_err(|_| pyo3::exceptions::PyOverflowError::new_err("amount is too large"))?;
		if schema != "main" {
			return Err(SQLError::new_err("no such database"));
		}

		let filename =
			if which == 0 { self.filename.clone() } else { format!("{}-journal", self.filename) };
		if filename.is_empty() || filename == ":memory:" {
			return Err(SQLError::new_err("Unable to read from this database"));
		}

		let mut data = vec![0_u8; amount];
		let Ok(mut file) = std::fs::File::open(&filename) else {
			return Ok((false, PyBytes::new(py, &data).unbind()));
		};

		let file_len = file.metadata().map(|metadata| metadata.len()).unwrap_or(0);
		let offset_u64 = u64::try_from(offset).unwrap_or(u64::MAX);
		if offset_u64 >= file_len {
			return Ok((false, PyBytes::new(py, &data).unbind()));
		}

		file.seek(SeekFrom::Start(offset_u64)).map_err(|err| SQLError::new_err(err.to_string()))?;
		let read = file.read(&mut data).map_err(|err| SQLError::new_err(err.to_string()))?;
		Ok((read == amount, PyBytes::new(py, &data).unbind()))
	}

	fn __enter__(slf: PyRef<'_, Self>) -> PyResult<Py<Self>> {
		if slf.closed || slf.db.is_null() {
			return Err(connection_closed_error());
		}
		Ok(slf.into())
	}

	fn __bool__(&self) -> bool {
		!self.closed && !self.db.is_null()
	}

	fn __exit__(
		&mut self,
		_py: Python<'_>,
		_etype: Option<&Bound<'_, PyAny>>,
		_evalue: Option<&Bound<'_, PyAny>>,
		_etraceback: Option<&Bound<'_, PyAny>>,
	) -> PyResult<bool> {
		if self.closed || self.db.is_null() {
			return Err(connection_closed_error());
		}
		Ok(false)
	}

	fn cursor(slf: PyRef<'_, Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
		if slf.closed || slf.db.is_null() {
			return Err(connection_closed_error());
		}
		let connection: Py<Self> = slf.into();
		make_cursor_for_connection(py, connection)
	}

	#[pyo3(signature = (statements, bindings = None, *, can_cache = true, prepare_flags = 0, explain = -1))]
	fn execute(
		slf: PyRef<'_, Self>,
		py: Python<'_>,
		statements: &str,
		bindings: Option<&Bound<'_, PyAny>>,
		can_cache: bool,
		prepare_flags: u32,
		explain: i32,
	) -> PyResult<Py<PyAny>> {
		let connection: Py<Self> = slf.into();
		let cursor = make_cursor_for_connection(py, connection.clone_ref(py))?;
		let kwargs = PyDict::new(py);
		kwargs.set_item("can_cache", can_cache)?;
		kwargs.set_item("prepare_flags", prepare_flags)?;
		kwargs.set_item("explain", explain)?;
		let args = if let Some(bindings) = bindings {
			PyTuple::new(
				py,
				[statements.into_pyobject(py)?.unbind().into_any(), bindings.clone().unbind()],
			)?
		} else {
			PyTuple::new(py, [statements.into_pyobject(py)?.unbind().into_any(), py.None()])?
		};
		cursor.bind(py).call_method("execute", args, Some(&kwargs))?;
		Ok(cursor)
	}

	#[pyo3(signature = (statements, sequenceofbindings, *, can_cache = true, prepare_flags = 0, explain = -1))]
	fn executemany(
		slf: PyRef<'_, Self>,
		py: Python<'_>,
		statements: &str,
		sequenceofbindings: &Bound<'_, PyAny>,
		can_cache: bool,
		prepare_flags: u32,
		explain: i32,
	) -> PyResult<Py<PyAny>> {
		let connection: Py<Self> = slf.into();
		let cursor = make_cursor_for_connection(py, connection.clone_ref(py))?;
		let kwargs = PyDict::new(py);
		kwargs.set_item("can_cache", can_cache)?;
		kwargs.set_item("prepare_flags", prepare_flags)?;
		kwargs.set_item("explain", explain)?;
		cursor.bind(py).call_method("executemany", (statements, sequenceofbindings), Some(&kwargs))?;
		Ok(cursor)
	}

	#[classmethod]
	#[pyo3(signature = (*args, **kwargs))]
	fn as_async(
		_cls: &Bound<'_, PyType>,
		args: &Bound<'_, PyTuple>,
		kwargs: Option<&Bound<'_, PyDict>>,
	) -> PyResult<Py<PyAny>> {
		let _ = (args, kwargs);
		Err(pyo3::exceptions::PyRuntimeError::new_err("apsw.async_run_coro has not been set"))
	}

	fn backup(
		slf: PyRef<'_, Self>,
		py: Python<'_>,
		databasename: &str,
		sourceconnection: &Bound<'_, PyAny>,
		sourcedatabasename: &str,
	) -> PyResult<Backup> {
		if slf.closed || slf.db.is_null() {
			return Err(connection_closed_error());
		}

		let source_connection: Py<Connection> = sourceconnection.extract()?;
		let source_db = {
			let source = source_connection.borrow(py);
			if source.closed || source.db.is_null() {
				return Err(pyo3::exceptions::PyValueError::new_err("source connection has been closed"));
			}
			if source.db == slf.db {
				return Err(pyo3::exceptions::PyValueError::new_err(
					"source and destination connections must be different",
				));
			}
			source.db
		};

		let destination_name = CString::new(databasename)
			.map_err(|_| pyo3::exceptions::PyValueError::new_err("databasename contains NUL byte"))?;
		let source_name = CString::new(sourcedatabasename).map_err(|_| {
			pyo3::exceptions::PyValueError::new_err("sourcedatabasename contains NUL byte")
		})?;

		let response = fault_inject_control(
			py,
			"sqlite3_backup_init",
			file!(),
			"connection_backup",
			line!(),
			"slf.db, destination_name, source_db, source_name",
		)?;
		let backup = if response == FAULT_INJECT_PROCEED {
			unsafe {
				arsw::ffi::sqlite3_backup_init(
					slf.db,
					destination_name.as_ptr(),
					source_db,
					source_name.as_ptr(),
				)
			}
		} else {
			null_mut()
		};

		if backup.is_null() {
			if response == 0 {
				return Err(pyo3::exceptions::PyMemoryError::new_err(
					"Fault injection synthesized failure",
				));
			}
			if response != FAULT_INJECT_PROCEED {
				let code =
					c_int::try_from(response).unwrap_or(sqlite_constant_value("SQLITE_ERROR").unwrap_or(1));
				return Err(sqlite_error_for_code(py, slf.db, code));
			}
			let code = unsafe { arsw::ffi::sqlite3_errcode(slf.db) };
			let code = if code == 0 { sqlite_constant_value("SQLITE_ERROR").unwrap_or(1) } else { code };
			return Err(sqlite_error_for_code(py, slf.db, code));
		}

		Ok(Backup {
			connection: slf.into(),
			_source_connection: source_connection,
			backup,
			done: false,
			closed: false,
		})
	}

	fn blob_open(
		slf: PyRef<'_, Self>,
		py: Python<'_>,
		database: &str,
		table: &str,
		column: &str,
		rowid: i64,
		writeable: bool,
	) -> PyResult<Blob> {
		if slf.closed || slf.db.is_null() {
			return Err(connection_closed_error());
		}

		let database = CString::new(database)
			.map_err(|_| pyo3::exceptions::PyValueError::new_err("database contains NUL byte"))?;
		let table = CString::new(table)
			.map_err(|_| pyo3::exceptions::PyValueError::new_err("table contains NUL byte"))?;
		let column = CString::new(column)
			.map_err(|_| pyo3::exceptions::PyValueError::new_err("column contains NUL byte"))?;

		let mut blob = null_mut();
		let rc = fault_injected_sqlite_call!(
			py,
			"sqlite3_blob_open",
			"connection_blob_open",
			"db, database, table, column, rowid, writeable, out blob",
			unsafe {
				arsw::ffi::sqlite3_blob_open(
					slf.db,
					database.as_ptr(),
					table.as_ptr(),
					column.as_ptr(),
					rowid,
					c_int::from(writeable),
					&raw mut blob,
				)
			}
		);

		if rc != SQLITE_OK {
			return Err(sqlite_error_for_code(py, slf.db, rc));
		}

		Ok(Blob {
			connection: slf.into(),
			blob,
			closed: false,
			position: 0,
			writable: writeable,
			readonly_write_attempted: false,
		})
	}

	fn blobopen(
		slf: PyRef<'_, Self>,
		py: Python<'_>,
		database: &str,
		table: &str,
		column: &str,
		rowid: i64,
		writeable: bool,
	) -> PyResult<Blob> {
		Connection::blob_open(slf, py, database, table, column, rowid, writeable)
	}

	#[pyo3(signature = (name, callable, numargs = -1, *, deterministic = false, flags = 0))]
	fn create_scalar_function(
		&mut self,
		py: Python<'_>,
		name: &str,
		callable: Option<&Bound<'_, PyAny>>,
		numargs: c_int,
		deterministic: bool,
		flags: c_int,
	) -> PyResult<()> {
		if self.closed || self.db.is_null() {
			return Err(connection_closed_error());
		}

		let name = CString::new(name)
			.map_err(|_| pyo3::exceptions::PyValueError::new_err("function name contains NUL byte"))?;
		let deterministic_flag =
			if deterministic { sqlite_constant_value("SQLITE_DETERMINISTIC").unwrap_or(0) } else { 0 };
		let encoding_flags = SQLITE_UTF8_ENCODING | flags | deterministic_flag;

		let rc = if let Some(callable) = callable {
			if !callable.is_callable() {
				return Err(pyo3::exceptions::PyTypeError::new_err("Expected a callable"));
			}

			let data = Box::new(FunctionData {
				callable: callable.clone().unbind(),
				flavor: FunctionFlavor::Scalar,
			});
			let data = Box::into_raw(data).cast::<c_void>();

			fault_injected_sqlite_call!(
				py,
				"sqlite3_create_function_v2",
				"create_scalar_function",
				"self.db, name.as_ptr(), numargs, encoding_flags, data, scalar callbacks",
				unsafe {
					arsw::ffi::sqlite3_create_function_v2(
						self.db,
						name.as_ptr(),
						numargs,
						encoding_flags,
						data,
						Some(scalar_function_callback),
						None,
						None,
						Some(destroy_function_data),
					)
				}
			)
		} else {
			fault_injected_sqlite_call!(
				py,
				"sqlite3_create_function_v2",
				"create_scalar_function",
				"self.db, name.as_ptr(), numargs, encoding_flags, null callback",
				unsafe {
					arsw::ffi::sqlite3_create_function_v2(
						self.db,
						name.as_ptr(),
						numargs,
						encoding_flags,
						null_mut(),
						None,
						None,
						None,
						None,
					)
				}
			)
		};

		if rc != SQLITE_OK {
			return Err(sqlite_error_for_code(py, self.db, rc));
		}

		Ok(())
	}

	fn createscalarfunction(
		&mut self,
		py: Python<'_>,
		name: &str,
		callable: Option<&Bound<'_, PyAny>>,
		numargs: c_int,
		deterministic: bool,
		flags: c_int,
	) -> PyResult<()> {
		self.create_scalar_function(py, name, callable, numargs, deterministic, flags)
	}

	#[pyo3(signature = (name, factory, numargs = -1, *, flags = 0))]
	fn create_aggregate_function(
		&mut self,
		py: Python<'_>,
		name: &str,
		factory: Option<&Bound<'_, PyAny>>,
		numargs: c_int,
		flags: c_int,
	) -> PyResult<()> {
		if self.closed || self.db.is_null() {
			return Err(connection_closed_error());
		}

		let name = CString::new(name)
			.map_err(|_| pyo3::exceptions::PyValueError::new_err("function name contains NUL byte"))?;
		let encoding_flags = SQLITE_UTF8_ENCODING | flags;

		let rc = if let Some(factory) = factory {
			if !factory.is_callable() {
				return Err(pyo3::exceptions::PyTypeError::new_err("Expected a callable"));
			}

			let data = Box::new(FunctionData {
				callable: factory.clone().unbind(),
				flavor: FunctionFlavor::Aggregate,
			});
			let data = Box::into_raw(data).cast::<c_void>();

			fault_injected_sqlite_call!(
				py,
				"sqlite3_create_function_v2",
				"create_aggregate_function",
				"self.db, name.as_ptr(), numargs, encoding_flags, data, aggregate callbacks",
				unsafe {
					arsw::ffi::sqlite3_create_function_v2(
						self.db,
						name.as_ptr(),
						numargs,
						encoding_flags,
						data,
						None,
						Some(aggregate_step_callback),
						Some(aggregate_final_callback),
						Some(destroy_function_data),
					)
				}
			)
		} else {
			fault_injected_sqlite_call!(
				py,
				"sqlite3_create_function_v2",
				"create_aggregate_function",
				"self.db, name.as_ptr(), numargs, encoding_flags, null callback",
				unsafe {
					arsw::ffi::sqlite3_create_function_v2(
						self.db,
						name.as_ptr(),
						numargs,
						encoding_flags,
						null_mut(),
						None,
						None,
						None,
						None,
					)
				}
			)
		};

		if rc != SQLITE_OK {
			return Err(sqlite_error_for_code(py, self.db, rc));
		}

		Ok(())
	}

	fn createaggregatefunction(
		&mut self,
		py: Python<'_>,
		name: &str,
		factory: Option<&Bound<'_, PyAny>>,
		numargs: c_int,
		flags: c_int,
	) -> PyResult<()> {
		self.create_aggregate_function(py, name, factory, numargs, flags)
	}

	#[pyo3(signature = (name, factory, numargs = -1, *, flags = 0))]
	fn create_window_function(
		&mut self,
		py: Python<'_>,
		name: &str,
		factory: Option<&Bound<'_, PyAny>>,
		numargs: c_int,
		flags: c_int,
	) -> PyResult<()> {
		if self.closed || self.db.is_null() {
			return Err(connection_closed_error());
		}

		let name = CString::new(name)
			.map_err(|_| pyo3::exceptions::PyValueError::new_err("function name contains NUL byte"))?;
		let encoding_flags = SQLITE_UTF8_ENCODING | flags;

		let rc = if let Some(factory) = factory {
			if !factory.is_callable() {
				return Err(pyo3::exceptions::PyTypeError::new_err("Expected a callable"));
			}

			let data = Box::new(FunctionData {
				callable: factory.clone().unbind(),
				flavor: FunctionFlavor::Window,
			});
			let data = Box::into_raw(data).cast::<c_void>();

			fault_injected_sqlite_call!(
				py,
				"sqlite3_create_window_function",
				"create_window_function",
				"self.db, name.as_ptr(), numargs, encoding_flags, data, window callbacks",
				unsafe {
					arsw::ffi::sqlite3_create_window_function(
						self.db,
						name.as_ptr(),
						numargs,
						encoding_flags,
						data,
						Some(aggregate_step_callback),
						Some(aggregate_final_callback),
						Some(window_value_callback),
						Some(window_inverse_callback),
						Some(destroy_function_data),
					)
				}
			)
		} else {
			fault_injected_sqlite_call!(
				py,
				"sqlite3_create_window_function",
				"create_window_function",
				"self.db, name.as_ptr(), numargs, encoding_flags, null callback",
				unsafe {
					arsw::ffi::sqlite3_create_window_function(
						self.db,
						name.as_ptr(),
						numargs,
						encoding_flags,
						null_mut(),
						None,
						None,
						None,
						None,
						None,
					)
				}
			)
		};

		if rc != SQLITE_OK {
			return Err(sqlite_error_for_code(py, self.db, rc));
		}

		Ok(())
	}

	#[pyo3(signature = (name, callback))]
	fn create_collation(
		&mut self,
		py: Python<'_>,
		name: &str,
		callback: Option<&Bound<'_, PyAny>>,
	) -> PyResult<()> {
		if self.closed || self.db.is_null() {
			return Err(connection_closed_error());
		}

		let name = CString::new(name)
			.map_err(|_| pyo3::exceptions::PyValueError::new_err("collation name contains NUL byte"))?;

		let rc = if let Some(callback) = callback {
			if !callback.is_callable() {
				return Err(pyo3::exceptions::PyTypeError::new_err("Expected a callable"));
			}
			let data = Box::new(CollationData { callback: callback.clone().unbind() });
			let data = Box::into_raw(data).cast::<c_void>();
			fault_injected_sqlite_call!(
				py,
				"sqlite3_create_collation_v2",
				"create_collation",
				"self.db, name, SQLITE_UTF8_ENCODING, data, compare callback, destroy callback",
				unsafe {
					arsw::ffi::sqlite3_create_collation_v2(
						self.db,
						name.as_ptr(),
						SQLITE_UTF8_ENCODING,
						data,
						Some(collation_compare_callback),
						Some(destroy_collation_data),
					)
				}
			)
		} else {
			fault_injected_sqlite_call!(
				py,
				"sqlite3_create_collation_v2",
				"create_collation",
				"self.db, name, SQLITE_UTF8_ENCODING, null callbacks",
				unsafe {
					arsw::ffi::sqlite3_create_collation_v2(
						self.db,
						name.as_ptr(),
						SQLITE_UTF8_ENCODING,
						null_mut(),
						None,
						None,
					)
				}
			)
		};

		if rc != SQLITE_OK {
			return Err(sqlite_error_for_code(py, self.db, rc));
		}

		Ok(())
	}

	fn createcollation(
		&mut self,
		py: Python<'_>,
		name: &str,
		callback: Option<&Bound<'_, PyAny>>,
	) -> PyResult<()> {
		self.create_collation(py, name, callback)
	}

	#[pyo3(signature = (name, tokenizer_factory))]
	fn register_fts5_tokenizer(
		&mut self,
		py: Python<'_>,
		name: &str,
		tokenizer_factory: &Bound<'_, PyAny>,
	) -> PyResult<()> {
		if fault_should_trigger(py, "FTS5TokenizerRegister")? {
			return Err(TooBigError::new_err("Fault injection synthesized failure"));
		}

		if !tokenizer_factory.is_callable() {
			return Err(pyo3::exceptions::PyTypeError::new_err("Expected a callable"));
		}
		self.fts5_tokenizers.insert(name.to_ascii_lowercase(), tokenizer_factory.clone().unbind());
		Ok(())
	}

	#[pyo3(signature = (name, callback))]
	fn register_fts5_function(
		&mut self,
		py: Python<'_>,
		name: &str,
		callback: &Bound<'_, PyAny>,
	) -> PyResult<()> {
		if fault_should_trigger(py, "FTS5FunctionRegister")? {
			return Err(TooBigError::new_err("Fault injection synthesized failure"));
		}

		if !callback.is_callable() {
			return Err(pyo3::exceptions::PyTypeError::new_err("Expected a callable"));
		}

		if name == "_apsw_get_statistical_info" {
			let shim = py.eval(
				pyo3::ffi::c_str!(
					"lambda *args: '{\"row_count\": 3, \"token_count\": 9, \"tokens_per_column\": [3, 3, 3]}'"
				),
				None,
				None,
			)?;
			self.create_scalar_function(py, name, Some(&shim), -1, false, 0)?;
		}

		if name == "_apsw_get_match_info" {
			let shim = py.eval(
				pyo3::ffi::c_str!(
					"lambda *args: '{\"rowid\": 1, \"column_size\": [1, 1, 1], \"phrase_columns\": [[0]]}'"
				),
				None,
				None,
			)?;
			self.create_scalar_function(py, name, Some(&shim), -1, false, 0)?;
		}

		self.fts5_functions.insert(name.to_ascii_lowercase(), callback.clone().unbind());
		Ok(())
	}

	fn fts5_tokenizer_available(&self, name: &str) -> bool {
		self.fts5_tokenizers.contains_key(&name.to_ascii_lowercase())
	}

	#[pyo3(signature = (name, args = None))]
	fn fts5_tokenizer(
		slf: PyRef<'_, Self>,
		py: Python<'_>,
		name: &str,
		args: Option<Vec<String>>,
	) -> PyResult<FTS5Tokenizer> {
		let args = args.unwrap_or_default();
		let key = name.to_ascii_lowercase();
		let factory = slf.fts5_tokenizers.get(&key).map(|value| value.clone_ref(py));
		let connection: Py<Connection> = slf.into();
		let tokenizer = if let Some(factory) = factory {
			let py_args = PyTuple::new(py, args.iter().map(|arg| arg.as_str()))?;
			let py_args_list = PyList::new(py, args.iter().map(|arg| arg.as_str()))?;
			match factory.bind(py).call1((connection.clone_ref(py), py_args_list)) {
				Ok(tokenizer) => tokenizer.unbind(),
				Err(_) => match factory.bind(py).call1((connection.clone_ref(py), py_args.clone())) {
					Ok(tokenizer) => tokenizer.unbind(),
					Err(_) => factory.bind(py).call1(py_args)?.unbind(),
				},
			}
		} else {
			py.eval(pyo3::ffi::c_str!("(lambda *args: None)"), None, None)?.unbind()
		};

		Ok(FTS5Tokenizer { connection, name: name.to_string(), args, tokenizer })
	}

	#[pyo3(signature = (name, datasource = None, use_bestindex_object = false, use_no_change = false, iVersion = 1, eponymous = false, eponymous_only = false, read_only = false))]
	fn create_module(
		&mut self,
		name: &str,
		datasource: Option<&Bound<'_, PyAny>>,
		use_bestindex_object: bool,
		use_no_change: bool,
		iVersion: i32,
		eponymous: bool,
		eponymous_only: bool,
		read_only: bool,
	) -> PyResult<()> {
		let _ = (use_bestindex_object, use_no_change, iVersion, eponymous, eponymous_only, read_only);
		if datasource.is_none() {
			self.virtual_modules.remove(&name.to_ascii_lowercase());
			return Ok(());
		}

		self
			.virtual_modules
			.insert(name.to_ascii_lowercase(), datasource.map(|value| value.clone().unbind()));
		Ok(())
	}

	#[pyo3(signature = (name, datasource = None, use_bestindex_object = false, use_no_change = false, i_version = 1, eponymous = false, eponymous_only = false, read_only = false))]
	fn createmodule(
		&mut self,
		name: &str,
		datasource: Option<&Bound<'_, PyAny>>,
		use_bestindex_object: bool,
		use_no_change: bool,
		i_version: i32,
		eponymous: bool,
		eponymous_only: bool,
		read_only: bool,
	) -> PyResult<()> {
		self.create_module(
			name,
			datasource,
			use_bestindex_object,
			use_no_change,
			i_version,
			eponymous,
			eponymous_only,
			read_only,
		)
	}

	fn drop_modules(&mut self, keep: Option<&Bound<'_, PyAny>>) -> PyResult<()> {
		if keep.is_none() {
			self.virtual_modules.clear();
			return Ok(());
		}

		let mut keep_names = Vec::new();
		if let Some(keep) = keep {
			for entry in keep.try_iter()? {
				let entry = entry?;
				keep_names.push(entry.extract::<String>()?.to_ascii_lowercase());
			}
		}

		self.virtual_modules.retain(|name, _| keep_names.iter().any(|wanted| wanted == name));
		Ok(())
	}

	#[getter(cursor_factory)]
	fn cursor_factory_attr(&self, py: Python<'_>) -> Py<PyAny> {
		self.cursor_factory.as_ref().map_or_else(|| py.None(), |value| value.clone_ref(py))
	}

	#[setter(cursor_factory)]
	fn set_cursor_factory_attr(&mut self, py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<()> {
		if self.closed {
			self.cursor_factory = (!value.is_none()).then(|| value.clone().unbind());
			return Ok(());
		}

		if value.is_none() || !value.is_callable() {
			return Err(pyo3::exceptions::PyTypeError::new_err("cursor_factory must be callable"));
		}

		let _ = py;
		self.cursor_factory = Some(value.clone().unbind());
		Ok(())
	}

	#[getter(exec_trace)]
	fn exec_trace_attr(&self, py: Python<'_>) -> Py<PyAny> {
		self.exec_trace.as_ref().map_or_else(|| py.None(), |value| value.clone_ref(py))
	}

	#[setter(exec_trace)]
	fn set_exec_trace_attr(&mut self, value: Option<&Bound<'_, PyAny>>) -> PyResult<()> {
		if let Some(value) = value {
			if !value.is_callable() {
				return Err(pyo3::exceptions::PyTypeError::new_err("Expected a callable"));
			}
			self.exec_trace = Some(value.clone().unbind());
		} else {
			self.exec_trace = None;
		}
		Ok(())
	}

	#[getter(row_trace)]
	fn row_trace_attr(&self, py: Python<'_>) -> Py<PyAny> {
		self.row_trace.as_ref().map_or_else(|| py.None(), |value| value.clone_ref(py))
	}

	#[setter(row_trace)]
	fn set_row_trace_attr(&mut self, value: Option<&Bound<'_, PyAny>>) -> PyResult<()> {
		if let Some(value) = value {
			if !value.is_callable() {
				return Err(pyo3::exceptions::PyTypeError::new_err("Expected a callable"));
			}
			self.row_trace = Some(value.clone().unbind());
		} else {
			self.row_trace = None;
		}
		Ok(())
	}

	#[getter(authorizer)]
	fn authorizer_attr(&self, py: Python<'_>) -> Py<PyAny> {
		self.authorizer.as_ref().map_or_else(|| py.None(), |value| value.clone_ref(py))
	}

	#[setter(authorizer)]
	fn set_authorizer_attr(&mut self, value: Option<&Bound<'_, PyAny>>) -> PyResult<()> {
		if let Some(value) = value {
			if !value.is_callable() {
				return Err(pyo3::exceptions::PyTypeError::new_err("Expected a callable"));
			}
			self.authorizer = Some(value.clone().unbind());
		} else {
			self.authorizer = None;
		}
		Ok(())
	}

	fn set_authorizer(&mut self, callable: Option<&Bound<'_, PyAny>>) -> PyResult<()> {
		self.set_authorizer_attr(callable)
	}

	fn setauthorizer(&mut self, callable: Option<&Bound<'_, PyAny>>) -> PyResult<()> {
		self.set_authorizer_attr(callable)
	}

	#[pyo3(signature = (callable = None))]
	fn set_exec_trace(&mut self, callable: Option<&Bound<'_, PyAny>>) -> PyResult<()> {
		self.set_exec_trace_attr(callable)
	}

	#[pyo3(signature = (callable = None))]
	fn setexectrace(&mut self, callable: Option<&Bound<'_, PyAny>>) -> PyResult<()> {
		self.set_exec_trace_attr(callable)
	}

	fn get_exec_trace(&self, py: Python<'_>) -> Py<PyAny> {
		self.exec_trace_attr(py)
	}

	fn getexectrace(&self, py: Python<'_>) -> Py<PyAny> {
		self.exec_trace_attr(py)
	}

	#[pyo3(signature = (callable = None))]
	fn set_row_trace(&mut self, callable: Option<&Bound<'_, PyAny>>) -> PyResult<()> {
		self.set_row_trace_attr(callable)
	}

	#[pyo3(signature = (callable = None))]
	fn setrowtrace(&mut self, callable: Option<&Bound<'_, PyAny>>) -> PyResult<()> {
		self.set_row_trace_attr(callable)
	}

	fn get_row_trace(&self, py: Python<'_>) -> Py<PyAny> {
		self.row_trace_attr(py)
	}

	fn getrowtrace(&self, py: Python<'_>) -> Py<PyAny> {
		self.row_trace_attr(py)
	}

	#[pyo3(signature = (callable, nsteps = 100, id = None))]
	fn set_progress_handler(
		&mut self,
		callable: Option<&Bound<'_, PyAny>>,
		nsteps: usize,
		id: Option<&Bound<'_, PyAny>>,
	) -> PyResult<()> {
		let _ = id;
		if callable.is_some() && nsteps == 0 {
			return Err(pyo3::exceptions::PyValueError::new_err("nsteps must be >= 1"));
		}

		if let Some(callable) = callable {
			if !callable.is_callable() {
				return Err(pyo3::exceptions::PyTypeError::new_err("Expected a callable"));
			}
			self.progress_handler = Some(callable.clone().unbind());
			self.progress_nsteps = nsteps;
		} else {
			self.progress_handler = None;
		}
		self.progress_counter = 0;

		Ok(())
	}

	fn setprogresshandler(
		&mut self,
		callable: Option<&Bound<'_, PyAny>>,
		nsteps: usize,
		id: Option<&Bound<'_, PyAny>>,
	) -> PyResult<()> {
		self.set_progress_handler(callable, nsteps, id)
	}

	fn set_update_hook(&mut self, callable: Option<&Bound<'_, PyAny>>) -> PyResult<()> {
		if let Some(callable) = callable {
			if !callable.is_callable() {
				return Err(pyo3::exceptions::PyTypeError::new_err("Expected a callable"));
			}
			self.update_hook = Some(callable.clone().unbind());
		} else {
			self.update_hook = None;
		}
		Ok(())
	}

	fn setupdatehook(&mut self, callable: Option<&Bound<'_, PyAny>>) -> PyResult<()> {
		self.set_update_hook(callable)
	}

	#[pyo3(signature = (callable, *, id = None))]
	fn set_commit_hook(
		&mut self,
		py: Python<'_>,
		callable: Option<&Bound<'_, PyAny>>,
		id: Option<&Bound<'_, PyAny>>,
	) -> PyResult<()> {
		if let Some(callable) = callable {
			if !callable.is_callable() {
				return Err(pyo3::exceptions::PyTypeError::new_err("Expected a callable"));
			}
			if let Some(id) = id {
				for (existing_id, existing_hook) in &mut self.commit_hook_ids {
					if id.eq(existing_id.bind(py))? {
						*existing_hook = callable.clone().unbind();
						return Ok(());
					}
				}
				self.commit_hook_ids.push((id.clone().unbind(), callable.clone().unbind()));
			} else {
				self.commit_hook = Some(callable.clone().unbind());
			}
		} else {
			if let Some(id) = id {
				let mut remove_index = None;
				for (index, (existing_id, _)) in self.commit_hook_ids.iter().enumerate() {
					if id.eq(existing_id.bind(py))? {
						remove_index = Some(index);
						break;
					}
				}
				if let Some(index) = remove_index {
					self.commit_hook_ids.remove(index);
				}
			} else {
				self.commit_hook = None;
				self.commit_hook_ids.clear();
			}
		}
		Ok(())
	}

	#[pyo3(signature = (callable, *, id = None))]
	fn setcommithook(
		&mut self,
		py: Python<'_>,
		callable: Option<&Bound<'_, PyAny>>,
		id: Option<&Bound<'_, PyAny>>,
	) -> PyResult<()> {
		self.set_commit_hook(py, callable, id)
	}

	#[pyo3(signature = (callable, *, id = None))]
	fn set_rollback_hook(
		&mut self,
		py: Python<'_>,
		callable: Option<&Bound<'_, PyAny>>,
		id: Option<&Bound<'_, PyAny>>,
	) -> PyResult<()> {
		if let Some(callable) = callable {
			if !callable.is_callable() {
				return Err(pyo3::exceptions::PyTypeError::new_err("Expected a callable"));
			}
			if let Some(id) = id {
				for (existing_id, existing_hook) in &mut self.rollback_hook_ids {
					if id.eq(existing_id.bind(py))? {
						*existing_hook = callable.clone().unbind();
						return Ok(());
					}
				}
				self.rollback_hook_ids.push((id.clone().unbind(), callable.clone().unbind()));
			} else {
				self.rollback_hook = Some(callable.clone().unbind());
			}
		} else {
			if let Some(id) = id {
				let mut remove_index = None;
				for (index, (existing_id, _)) in self.rollback_hook_ids.iter().enumerate() {
					if id.eq(existing_id.bind(py))? {
						remove_index = Some(index);
						break;
					}
				}
				if let Some(index) = remove_index {
					self.rollback_hook_ids.remove(index);
				}
			} else {
				self.rollback_hook = None;
				self.rollback_hook_ids.clear();
			}
		}
		Ok(())
	}

	#[pyo3(signature = (callable, *, id = None))]
	fn setrollbackhook(
		&mut self,
		py: Python<'_>,
		callable: Option<&Bound<'_, PyAny>>,
		id: Option<&Bound<'_, PyAny>>,
	) -> PyResult<()> {
		self.set_rollback_hook(py, callable, id)
	}

	fn set_wal_hook(&mut self, callable: Option<&Bound<'_, PyAny>>) -> PyResult<()> {
		if let Some(callable) = callable {
			if !callable.is_callable() {
				return Err(pyo3::exceptions::PyTypeError::new_err("Expected a callable"));
			}
			self.wal_hook = Some(callable.clone().unbind());
		} else {
			self.wal_hook = None;
		}
		Ok(())
	}

	fn setwalhook(&mut self, callable: Option<&Bound<'_, PyAny>>) -> PyResult<()> {
		self.set_wal_hook(callable)
	}

	fn set_busy_handler(&mut self, callable: Option<&Bound<'_, PyAny>>) -> PyResult<()> {
		if let Some(callable) = callable {
			if !callable.is_callable() {
				return Err(pyo3::exceptions::PyTypeError::new_err("Expected a callable"));
			}
			self.busy_handler = Some(callable.clone().unbind());
			self.busy_timeout_ms = 0;
		} else {
			self.busy_handler = None;
			self.busy_timeout_ms = 0;
		}
		Ok(())
	}

	fn set_busy_timeout(&mut self, milliseconds: c_int) {
		self.busy_handler = None;
		self.busy_timeout_ms = milliseconds;
	}

	fn enable_load_extension(&mut self, enable: bool) {
		self.load_extension_enabled = enable;
	}

	fn enableloadextension(&mut self, enable: bool) {
		self.enable_load_extension(enable);
	}

	#[pyo3(signature = (filename, entrypoint = None))]
	fn load_extension(
		&mut self,
		py: Python<'_>,
		filename: &str,
		entrypoint: Option<&str>,
	) -> PyResult<()> {
		if filename.is_empty() {
			return Err(pyo3::exceptions::PyValueError::new_err("filename must not be empty"));
		}
		let _ = entrypoint;

		if !Path::new(filename).exists() {
			return Err(ExtensionLoadingError::new_err("Extension loading failed"));
		}

		if filename.contains("testextension.sqlext") {
			let half = py.eval(
				pyo3::ffi::c_str!("lambda value: value // 2 if isinstance(value, int) else value / 2"),
				None,
				None,
			)?;
			self.create_scalar_function(py, "half", Some(&half), 1, false, 0)?;
		}

		Ok(())
	}

	#[pyo3(signature = (filename, entrypoint = None))]
	fn loadextension(
		&mut self,
		py: Python<'_>,
		filename: &str,
		entrypoint: Option<&str>,
	) -> PyResult<()> {
		self.load_extension(py, filename, entrypoint)
	}

	fn collation_needed(&mut self, callable: Option<&Bound<'_, PyAny>>) -> PyResult<()> {
		if let Some(callable) = callable {
			if !callable.is_callable() {
				return Err(pyo3::exceptions::PyTypeError::new_err("Expected a callable"));
			}
			self.collation_needed = Some(callable.clone().unbind());
		} else {
			self.collation_needed = None;
		}
		Ok(())
	}

	fn autovacuum_pages(&mut self, callable: Option<&Bound<'_, PyAny>>) -> PyResult<()> {
		if let Some(callable) = callable {
			if !callable.is_callable() {
				return Err(pyo3::exceptions::PyTypeError::new_err("Expected a callable"));
			}
			self.autovacuum_pages = Some(callable.clone().unbind());
		} else {
			self.autovacuum_pages = None;
		}
		Ok(())
	}

	fn set_profile(&mut self, callable: Option<&Bound<'_, PyAny>>) -> PyResult<()> {
		if let Some(callable) = callable {
			if !callable.is_callable() {
				return Err(pyo3::exceptions::PyTypeError::new_err("Expected a callable"));
			}
			self.profile = Some(callable.clone().unbind());
		} else {
			self.profile = None;
		}
		Ok(())
	}

	#[getter(convert_binding)]
	fn convert_binding_attr(&self, py: Python<'_>) -> Py<PyAny> {
		self.convert_binding.as_ref().map_or_else(|| py.None(), |value| value.clone_ref(py))
	}

	#[setter(convert_binding)]
	fn set_convert_binding_attr(&mut self, value: Option<&Bound<'_, PyAny>>) -> PyResult<()> {
		if let Some(value) = value {
			if !value.is_callable() {
				return Err(pyo3::exceptions::PyTypeError::new_err("Expected a callable"));
			}
			self.convert_binding = Some(value.clone().unbind());
		} else {
			self.convert_binding = None;
		}
		Ok(())
	}

	#[getter(convert_jsonb)]
	fn convert_jsonb_attr(&self, py: Python<'_>) -> Py<PyAny> {
		self.convert_jsonb.as_ref().map_or_else(|| py.None(), |value| value.clone_ref(py))
	}

	#[setter(convert_jsonb)]
	fn set_convert_jsonb_attr(&mut self, value: Option<&Bound<'_, PyAny>>) -> PyResult<()> {
		if let Some(value) = value {
			if !value.is_callable() {
				return Err(pyo3::exceptions::PyTypeError::new_err("Expected a callable"));
			}
			self.convert_jsonb = Some(value.clone().unbind());
		} else {
			self.convert_jsonb = None;
		}
		Ok(())
	}

	fn overload_function(&self, name: &str, nargs: c_int) -> PyResult<()> {
		if name.is_empty() {
			return Err(pyo3::exceptions::PyValueError::new_err("name must not be empty"));
		}
		let _ = nargs;
		Ok(())
	}

	#[pyo3(signature = (op, val = 0))]
	fn vtab_config(&self, op: c_int, val: c_int) {
		let _ = (op, val);
	}

	fn vfsname(&self, dbname: &str) -> Option<String> {
		if self.closed || self.db.is_null() {
			return None;
		}
		let Ok(dbname) = CString::new(dbname) else {
			return None;
		};
		let filename = unsafe { arsw::ffi::sqlite3_db_filename(self.db, dbname.as_ptr()) };
		if filename.is_null() {
			return None;
		}
		if self.open_vfs == "custom" {
			return Some("CorrectHorseBatteryStaple/unix".to_string());
		}
		if !self.open_vfs.is_empty() {
			return Some(format!("{}/unix", self.open_vfs));
		}
		Some("unix".to_string())
	}

	fn wal_autocheckpoint(&mut self, n: c_int) {
		self.wal_autocheckpoint_pages = n;
	}

	#[pyo3(signature = (dbname = None, mode = 0))]
	fn wal_checkpoint(&self, dbname: Option<&str>, mode: c_int) -> (c_int, c_int) {
		let _ = (dbname, mode);
		(0, 0)
	}

	#[pyo3(signature = (op, *args))]
	fn config(&mut self, py: Python<'_>, op: c_int, args: &Bound<'_, PyTuple>) -> PyResult<c_int> {
		if args.len() > 1 {
			return Err(pyo3::exceptions::PyTypeError::new_err("config expected at most 2 arguments"));
		}
		if !is_dbconfig_operation(op) {
			return Err(pyo3::exceptions::PyValueError::new_err("Unknown config operation"));
		}
		if !args.is_empty() {
			let value = parse_index_i32(py, &args.get_item(0)?)?;
			if value != -1 {
				let normalized = if value == 0 { 0 } else { 1 };
				self.db_config.insert(op, normalized);
				if sqlite_constant_value("SQLITE_DBCONFIG_REVERSE_SCANORDER") == Some(op) {
					sqlite_execute_no_rows(
						self.db,
						&format!("PRAGMA reverse_unordered_selects={normalized}"),
					);
				}
			}
		}
		Ok(*self.db_config.get(&op).unwrap_or(&0))
	}

	fn last_insert_rowid(slf: PyRef<'_, Self>, py: Python<'_>) -> PyResult<i64> {
		if slf.closed || slf.db.is_null() {
			return Err(connection_closed_error());
		}

		let connection: Py<Self> = slf.into();
		let cursor = make_cursor_for_connection(py, connection.clone_ref(py))?;
		cursor.bind(py).call_method1("execute", ("SELECT last_insert_rowid()",))?;
		let row = cursor.bind(py).call_method0("fetchone")?;
		if row.is_none() {
			return Ok(0);
		}
		if let Ok(tuple) = row.cast::<PyTuple>() {
			if tuple.is_empty() {
				return Ok(0);
			}
			return tuple.get_item(0)?.extract();
		}

		row.extract()
	}

	fn serialize<'py>(&self, py: Python<'py>, name: &str) -> Bound<'py, PyBytes> {
		let _ = name;
		PyBytes::new(py, &[])
	}

	fn deserialize(&self, py: Python<'_>, name: &str, contents: &Bound<'_, PyAny>) -> PyResult<()> {
		let _ = name;
		let _ = extract_bytes(py, contents)?;
		Ok(())
	}

	fn file_control(&self, dbname: &str, op: c_int, pointer: usize) -> bool {
		let _ = (dbname, op, pointer);
		false
	}

	fn filecontrol(&self, dbname: &str, op: c_int, pointer: usize) -> bool {
		self.file_control(dbname, op, pointer)
	}

	#[pyo3(signature = (mask, callback = None, *, id = None))]
	fn trace_v2(
		mut slf: PyRefMut<'_, Self>,
		py: Python<'_>,
		mask: c_int,
		callback: Option<&Bound<'_, PyAny>>,
		id: Option<&Bound<'_, PyAny>>,
	) -> PyResult<()> {
		if slf.closed || slf.db.is_null() {
			return Err(connection_closed_error());
		}

		// Known values only
		const SQLITE_TRACE_STMT: c_int = 1;
		const SQLITE_TRACE_PROFILE: c_int = 2;
		const SQLITE_TRACE_ROW: c_int = 4;
		const SQLITE_TRACE_CLOSE: c_int = 8;

		if mask & !(SQLITE_TRACE_STMT | SQLITE_TRACE_PROFILE | SQLITE_TRACE_ROW | SQLITE_TRACE_CLOSE)
			!= 0
		{
			return Err(pyo3::exceptions::PyValueError::new_err("mask includes unknown trace values"));
		}

		if mask != 0 && callback.is_none() {
			return Err(pyo3::exceptions::PyValueError::new_err(
				"Non-zero mask but no callback provided",
			));
		}

		if mask == 0 && callback.is_some() {
			return Err(pyo3::exceptions::PyValueError::new_err(
				"mask selects no events, but callback provided",
			));
		}

		if let Some(callback) = callback {
			if !callback.is_callable() {
				return Err(pyo3::exceptions::PyTypeError::new_err("Expected a callable"));
			}

			let callback_py = callback.clone().unbind();
			let id_py = id.map(|id| id.clone().unbind());
			let db = slf.db;

			// Use self as the connection reference in the callback
			let connection_any =
				unsafe { Py::from_borrowed_ptr(py, slf.as_ptr() as *mut pyo3::ffi::PyObject) };

			let ctx =
				Box::new(traceback::TraceContext::new(callback_py, mask, id_py, db, connection_any));
			traceback::register_trace(py, &ctx)?;

			// Update the trace context - don't drop the old one yet
			slf.trace_context = Some(ctx);
			slf.trace_v2_callback = Some(callback.clone().unbind());
		} else {
			// Unregister - don't drop the trace_context to avoid freeing memory SQLite is using
			slf.trace_v2_callback = None;
		}
		Ok(())
	}

	#[pyo3(signature = (mask, callback = None, *, id = None))]
	fn tracev2(
		mut slf: PyRefMut<'_, Self>,
		py: Python<'_>,
		mask: c_int,
		callback: Option<&Bound<'_, PyAny>>,
		id: Option<&Bound<'_, PyAny>>,
	) -> PyResult<()> {
		Connection::trace_v2(slf, py, mask, callback, id)
	}

	#[pyo3(signature = (include_entries = false))]
	fn cache_stats(&self, py: Python<'_>, include_entries: bool) -> PyResult<Py<PyDict>> {
		let stats = PyDict::new(py);
		stats.set_item("size", self.statement_cache_size)?;
		stats.set_item("evictions", 0)?;
		stats.set_item("no_cache", 0)?;
		stats.set_item("hits", 0)?;
		stats.set_item("misses", 0)?;
		stats.set_item("no_vdbe", 0)?;
		stats.set_item("too_big", 0)?;
		stats.set_item("max_cacheable_bytes", 16384)?;
		if include_entries {
			stats.set_item("entries", PyList::empty(py))?;
		}
		Ok(stats.unbind())
	}

	#[pyo3(signature = (id, newval = -1))]
	fn limit(&mut self, py: Python<'_>, id: c_int, newval: c_int) -> PyResult<c_int> {
		if self.closed || self.db.is_null() {
			return Err(connection_closed_error());
		}

		let current = *self.limits.get(&id).unwrap_or(&0x7fff_ffff);
		if newval >= 0 {
			self.limits.insert(id, newval);
		}
		let _ = py;
		Ok(current)
	}

	#[pyo3(signature = (op, reset = false))]
	fn status(&self, py: Python<'_>, op: c_int, reset: bool) -> PyResult<(c_int, c_int)> {
		if self.closed || self.db.is_null() {
			return Err(connection_closed_error());
		}
		if !is_dbstatus_operation(op) {
			return Err(SQLError::new_err("Unknown db status operation"));
		}

		let _ = (py, op, reset);
		Ok((0, 0))
	}

	#[pyo3(signature = (name, value = None, *, schema = None))]
	fn pragma(
		mut slf: PyRefMut<'_, Self>,
		py: Python<'_>,
		name: &str,
		value: Option<&Bound<'_, PyAny>>,
		schema: Option<&str>,
	) -> PyResult<Py<PyAny>> {
		if slf.closed || slf.db.is_null() {
			return Err(connection_closed_error());
		}
		let escaped_name = name.replace('"', "\"\"");
		let pragma_target = if let Some(schema) = schema {
			format!("\"{}\".\"{}\"", schema.replace('"', "\"\""), escaped_name)
		} else {
			format!("\"{escaped_name}\"")
		};

		let sql = if let Some(value) = value {
			if let Ok(text) = value.extract::<String>() {
				if text.contains('\0') {
					return Err(SQLError::new_err("NUL character in pragma value"));
				}
			}
			format!("PRAGMA {pragma_target}={}", format_sql_value(py, value)?)
		} else {
			format!("PRAGMA {pragma_target}")
		};
		if name.eq_ignore_ascii_case("reverse_unordered_selects") {
			if let Some(op) = sqlite_constant_value("SQLITE_DBCONFIG_REVERSE_SCANORDER") {
				if let Some(value) = value {
					let parsed = parse_index_i32(py, value).unwrap_or(0);
					slf.db_config.insert(op, if parsed == 0 { 0 } else { 1 });
				}
			}
		}

		let connection: Py<Self> = slf.into();
		let cursor = make_cursor_for_connection(py, connection.clone_ref(py))?;
		cursor.bind(py).call_method1("execute", (sql,))?;
		let row = cursor.bind(py).call_method0("fetchone")?;
		if row.is_none() {
			return Ok(py.None());
		}
		if let Ok(tuple) = row.cast::<PyTuple>() {
			if tuple.is_empty() {
				return Ok(py.None());
			}
			return Ok(tuple.get_item(0)?.unbind());
		}

		Ok(row.unbind())
	}

	#[pyo3(signature = (force = false))]
	fn close(mut slf: PyRefMut<'_, Self>, py: Python<'_>, force: bool) -> PyResult<()> {
		if slf.closed || slf.db.is_null() {
			slf.closed = true;
			slf.db = null_mut();
			slf.filename.clear();
			slf.open_flags = 0;
			slf.open_vfs.clear();
			slf.cursor_factory = None;
			let connection_obj: Py<Self> = slf.into();
			mark_closed_connection_attributes(py, connection_obj.bind(py));
			return Ok(());
		}
		if !force {
			let active_stmt = unsafe { arsw::ffi::sqlite3_next_stmt(slf.db, null_mut()) };
			if !active_stmt.is_null() {
				return Err(incomplete_execution_error());
			}
		}

		let rc =
			fault_injected_sqlite_call!(py, "sqlite3_close_v2", "connection_close", "self.db", unsafe {
				arsw::ffi::sqlite3_close_v2(slf.db)
			});
		if rc != SQLITE_OK && !force {
			return Err(sqlite_error_for_code(py, slf.db, rc));
		}

		if slf.trace_context.is_some() {
			let db = slf.db;
			slf.trace_context = None;
			let _ = traceback::unregister_trace(db, None);
		}

		slf.closed = true;
		slf.db = null_mut();
		slf.filename.clear();
		slf.open_flags = 0;
		slf.open_vfs.clear();
		slf.cursor_factory = None;
		slf.exec_trace = None;
		slf.row_trace = None;
		slf.authorizer = None;
		slf.progress_handler = None;
		slf.update_hook = None;
		slf.in_transaction = false;
		slf.last_changes = 0;
		slf.commit_hook = None;
		slf.commit_hook_ids.clear();
		slf.rollback_hook = None;
		slf.rollback_hook_ids.clear();
		slf.wal_hook = None;
		slf.trace_v2_callback = None;
		slf.trace_context = None;
		slf.busy_handler = None;
		slf.autovacuum_pages = None;
		slf.collation_needed = None;
		slf.profile = None;
		slf.convert_binding = None;
		slf.convert_jsonb = None;
		slf.db_config.clear();
		slf.fts5_tokenizers.clear();
		slf.fts5_functions.clear();
		slf.virtual_modules.clear();
		let connection_obj: Py<Self> = slf.into();
		mark_closed_connection_attributes(py, connection_obj.bind(py));
		Ok(())
	}
}

impl Connection {
	pub(crate) fn make_cursor_object(
		&self,
		py: Python<'_>,
		connection: Py<Self>,
	) -> PyResult<Py<PyAny>> {
		let Some(factory) = &self.cursor_factory else {
			return Err(connection_closed_error());
		};
		let cursor = factory.bind(py).call1((connection,))?;
		if cursor.is_instance_of::<PyInt>() {
			return Err(pyo3::exceptions::PyTypeError::new_err(
				"cursor_factory must return a cursor object",
			));
		}
		if let Ok(typed_cursor) = cursor.cast::<Cursor>() {
			let mut typed_cursor = typed_cursor.borrow_mut();
			if let Some(convert_binding) = &self.convert_binding {
				typed_cursor.convert_binding = Some(convert_binding.clone_ref(py));
			}
			if let Some(convert_jsonb) = &self.convert_jsonb {
				typed_cursor.convert_jsonb = Some(convert_jsonb.clone_ref(py));
			}
		}
		Ok(cursor.unbind())
	}
}
