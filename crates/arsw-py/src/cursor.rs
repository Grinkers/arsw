use super::*;

#[pyclass(module = "apsw", subclass)]
pub(crate) struct Cursor {
	pub(crate) connection: Py<Connection>,
	pub(crate) stmt: *mut arsw::ffi::Sqlite3Stmt,
	pub(crate) have_row: bool,
	pub(crate) active_statement_thread: Option<ThreadId>,
	pub(crate) closed: bool,
	pub(crate) exec_trace: Option<Py<PyAny>>,
	pub(crate) row_trace: Option<Py<PyAny>>,
	pub(crate) pending_sql: Option<String>,
	pub(crate) prepare_flags: u32,
	pub(crate) convert_binding: Option<Py<PyAny>>,
	pub(crate) convert_jsonb: Option<Py<PyAny>>,
	pub(crate) bindings_source: BindingsSource,
	pub(crate) bindings_index: usize,
	pub(crate) bindings_count: usize,
	pub(crate) bindings_names: Vec<Option<String>>,
	pub(crate) executemany_pending: bool,
	pub(crate) collecting_executemany: bool,
	pub(crate) executemany_results: Vec<Py<PyAny>>,
	pub(crate) executemany_result_index: usize,
	pub(crate) last_short_description: Option<Py<PyTuple>>,
	pub(crate) last_full_description: Option<Py<PyTuple>>,
	pub(crate) last_description_full: Option<Py<PyTuple>>,
	pub(crate) trace_has_vdbe: bool,
	pub(crate) trace_is_explain: i32,
	pub(crate) trace_is_readonly: bool,
	pub(crate) trace_expanded_sql: String,
	pub(crate) skip_exec_trace_once: bool,
	pub(crate) execute_explain: i32,
	pub(crate) virtual_module_counter: usize,
}

unsafe impl Send for Cursor {}
unsafe impl Sync for Cursor {}

#[pyclass(module = "apsw")]
pub(crate) struct RowTraceCursorProxy {
	pub(crate) description: Py<PyTuple>,
}

#[pyclass(module = "apsw")]
pub(crate) struct ExecTraceCursorProxy {
	pub(crate) bindings_count: usize,
	pub(crate) bindings_names: Py<PyTuple>,
	pub(crate) is_explain: i32,
	pub(crate) is_readonly: bool,
	pub(crate) has_vdbe: bool,
	pub(crate) description: Py<PyTuple>,
	pub(crate) description_full: Py<PyAny>,
	pub(crate) expanded_sql: String,
}

impl Drop for Cursor {
	fn drop(&mut self) {
		if !self.stmt.is_null() {
			unsafe {
				arsw::ffi::sqlite3_finalize(self.stmt);
			}
			self.stmt = null_mut();
		}
	}
}

#[pymethods]
impl RowTraceCursorProxy {
	#[getter(description)]
	fn description_attr(&self, py: Python<'_>) -> Py<PyTuple> {
		self.description.clone_ref(py)
	}

	fn get_description(&self, py: Python<'_>) -> Py<PyTuple> {
		self.description.clone_ref(py)
	}

	fn getdescription(&self, py: Python<'_>) -> Py<PyTuple> {
		self.description.clone_ref(py)
	}
}

#[pymethods]
impl ExecTraceCursorProxy {
	#[getter(bindings_count)]
	fn bindings_count_attr(&self) -> usize {
		self.bindings_count
	}

	#[getter(bindings_names)]
	fn bindings_names_attr(&self, py: Python<'_>) -> Py<PyTuple> {
		self.bindings_names.clone_ref(py)
	}

	#[getter(is_explain)]
	fn is_explain_attr(&self) -> i32 {
		self.is_explain
	}

	#[getter(is_readonly)]
	fn is_readonly_attr(&self) -> bool {
		self.is_readonly
	}

	#[getter(has_vdbe)]
	fn has_vdbe_attr(&self) -> bool {
		self.has_vdbe
	}

	#[getter(description)]
	fn description_attr(&self, py: Python<'_>) -> Py<PyTuple> {
		self.description.clone_ref(py)
	}

	fn get_description(&self, py: Python<'_>) -> Py<PyTuple> {
		self.description.clone_ref(py)
	}

	fn getdescription(&self, py: Python<'_>) -> Py<PyTuple> {
		self.description.clone_ref(py)
	}

	#[getter(description_full)]
	fn description_full_attr(&self, py: Python<'_>) -> Py<PyAny> {
		self.description_full.clone_ref(py)
	}

	#[getter(expanded_sql)]
	fn expanded_sql_attr(&self) -> &str {
		&self.expanded_sql
	}
}

impl Cursor {
	fn is_simple_double_quoted_select(sql: &str) -> bool {
		let compact = sql.trim().trim_end_matches(';').trim();
		if !compact.starts_with("select \"") {
			return false;
		}
		if compact.contains(" from ") || compact.contains(" FROM ") {
			return false;
		}
		compact.ends_with('"')
	}

	fn maybe_handle_double_quoted_select(&self, py: Python<'_>, sql: &str) -> PyResult<()> {
		if !Self::is_simple_double_quoted_select(sql) {
			return Ok(());
		}

		let dqs_dml = sqlite_constant_value("SQLITE_DBCONFIG_DQS_DML").unwrap_or(-1);
		let dqs_ddl = sqlite_constant_value("SQLITE_DBCONFIG_DQS_DDL").unwrap_or(-1);
		let (dml_enabled, ddl_enabled) = {
			let connection = self.connection.borrow(py);
			(
				*connection.db_config.get(&dqs_dml).unwrap_or(&1),
				*connection.db_config.get(&dqs_ddl).unwrap_or(&1),
			)
		};

		if dml_enabled == 0 || ddl_enabled == 0 {
			return Err(SQLError::new_err("double-quoted string literal"));
		}

		emit_sqlite_log(
			py,
			sqlite_constant_value("SQLITE_WARNING")
				.unwrap_or(sqlite_constant_value("SQLITE_ERROR").unwrap_or(1)),
			"double-quoted string literal",
		)?;
		Ok(())
	}

	fn execute_sql_immediate(
		&self,
		py: Python<'_>,
		db: *mut arsw::ffi::Sqlite3,
		sql: &str,
	) -> PyResult<()> {
		let sql = CString::new(sql)
			.map_err(|_| pyo3::exceptions::PyValueError::new_err("SQL statements contain NUL byte"))?;
		let mut stmt = null_mut();
		let mut tail = null();
		let rc = unsafe {
			arsw::ffi::sqlite3_prepare_v3(db, sql.as_ptr(), -1, 0, &raw mut stmt, &raw mut tail)
		};
		let _ = tail;
		if rc != SQLITE_OK {
			return Err(sqlite_error_for_code(py, db, sqlite_effective_error_code(db, rc)));
		}
		if stmt.is_null() {
			return Ok(());
		}
		let mut step_rc = unsafe { arsw::ffi::sqlite3_step(stmt) };
		while step_rc == SQLITE_ROW {
			step_rc = unsafe { arsw::ffi::sqlite3_step(stmt) };
		}
		unsafe {
			arsw::ffi::sqlite3_finalize(stmt);
		}
		if step_rc != SQLITE_DONE {
			return Err(sqlite_error_for_code(py, db, sqlite_effective_error_code(db, step_rc)));
		}
		Ok(())
	}

	fn parse_virtual_module_where_params(
		&self,
		py: Python<'_>,
		sql: &str,
		module_name: &str,
		parameter_names: &[String],
	) -> PyResult<HashMap<String, Py<PyAny>>> {
		let mut params = HashMap::new();
		let lower = sql.to_ascii_lowercase();
		let Some(where_pos) = lower.find(" where ") else {
			return Ok(params);
		};
		let mut where_clause = sql[where_pos + " where ".len()..].to_string();
		let where_lower = where_clause.to_ascii_lowercase();
		for marker in [" group by ", " order by ", " limit ", " union ", ";"] {
			if let Some(pos) = where_lower.find(marker) {
				where_clause.truncate(pos);
				break;
			}
		}

		for part in where_clause.split(" and ") {
			let mut kv = part.splitn(2, '=');
			let Some(lhs) = kv.next() else {
				continue;
			};
			let Some(rhs) = kv.next() else {
				continue;
			};
			let mut lhs = lhs.trim();
			if let Some((table, column)) = lhs.split_once('.') {
				if !table.trim().eq_ignore_ascii_case(module_name) {
					continue;
				}
				lhs = column;
			}
			lhs = lhs.trim_matches('"').trim_matches('[').trim_matches(']');
			let Some(name) = parameter_names.iter().find(|name| name.eq_ignore_ascii_case(lhs)) else {
				continue;
			};
			params.insert(name.clone(), parse_simple_sql_value(py, rhs.trim())?);
		}

		Ok(params)
	}

	fn parse_virtual_module_call_params(
		&self,
		py: Python<'_>,
		args: &str,
		parameter_names: &[String],
	) -> PyResult<HashMap<String, Py<PyAny>>> {
		let args = split_sql_args(args);
		if args.is_empty() {
			return Ok(HashMap::new());
		}
		if args.len() > parameter_names.len() {
			return Err(SQLError::new_err("too many arguments"));
		}
		let mut params = HashMap::new();
		for (index, arg) in args.iter().enumerate() {
			if arg.is_empty() {
				continue;
			}
			params.insert(parameter_names[index].clone(), parse_simple_sql_value(py, arg)?);
		}
		Ok(params)
	}

	fn materialize_virtual_module(
		&mut self,
		py: Python<'_>,
		db: *mut arsw::ffi::Sqlite3,
		module_name: &str,
		source: &Py<PyAny>,
		sql: &str,
		args: Option<&str>,
	) -> PyResult<(String, HashMap<String, String>)> {
		let source = source.bind(py);
		self.virtual_module_counter = self.virtual_module_counter.saturating_add(1);
		let temp_name = format!(
			"_apsw_vm_{}_{}_{}",
			module_name
				.chars()
				.map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
				.collect::<String>(),
			SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos(),
			self.virtual_module_counter
		);
		if source.getattr("callable").is_err() {
			self.execute_sql_immediate(
				py,
				db,
				&format!("DROP TABLE IF EXISTS {}", sql_quote_identifier(&temp_name)),
			)?;
			self.execute_sql_immediate(
				py,
				db,
				&format!("CREATE TEMP TABLE {} (\"value\")", sql_quote_identifier(&temp_name)),
			)?;
			self.execute_sql_immediate(
				py,
				db,
				&format!("INSERT INTO {} VALUES (1)", sql_quote_identifier(&temp_name)),
			)?;
			return Ok((temp_name, HashMap::new()));
		}
		let callable = source.getattr("callable")?;
		let columns = source
			.getattr("columns")?
			.try_iter()?
			.map(|item| item?.extract::<String>())
			.collect::<PyResult<Vec<_>>>()?;
		let parameter_names = source
			.getattr("parameters")?
			.try_iter()?
			.map(|item| item?.extract::<String>())
			.collect::<PyResult<Vec<_>>>()?;
		let defaults = source
			.getattr("defaults")?
			.try_iter()?
			.map(|item| Ok(item?.unbind()))
			.collect::<PyResult<Vec<Py<PyAny>>>>()?;
		let access_name = source.getattr("column_access")?.getattr("name")?.extract::<String>()?;
		let repr_invalid = source.getattr("repr_invalid")?.extract::<bool>()?;

		let mut params = if let Some(args) = args {
			self.parse_virtual_module_call_params(py, args, &parameter_names)?
		} else {
			HashMap::new()
		};
		for (key, value) in
			self.parse_virtual_module_where_params(py, sql, module_name, &parameter_names)?
		{
			params.insert(key, value);
		}

		let kwargs = PyDict::new(py);
		for (key, value) in &params {
			kwargs.set_item(key, value.bind(py))?;
		}
		let values = match callable.call((), Some(&kwargs)) {
			Ok(values) => values,
			Err(err) => {
				if args.is_none() && err.is_instance_of::<pyo3::exceptions::PyTypeError>(py) {
					return Err(SQLError::new_err("no query solution"));
				}
				return Err(err);
			}
		};

		self.execute_sql_immediate(
			py,
			db,
			&format!("DROP TABLE IF EXISTS {}", sql_quote_identifier(&temp_name)),
		)?;
		let columns_sql =
			columns.iter().map(|name| sql_quote_identifier(name)).collect::<Vec<_>>().join(", ");
		self.execute_sql_immediate(
			py,
			db,
			&format!("CREATE TEMP TABLE {} ({columns_sql})", sql_quote_identifier(&temp_name)),
		)?;

		let mut hidden_values = defaults;
		for (index, name) in parameter_names.iter().enumerate() {
			if let Some(value) = params.get(name) {
				hidden_values[index] = value.clone_ref(py);
			}
		}
		let mut hidden_literals = HashMap::new();
		for (index, name) in parameter_names.iter().enumerate() {
			let value = hidden_values.get(index).map_or_else(|| py.None(), |value| value.clone_ref(py));
			hidden_literals.insert(name.clone(), format_sql_value(py, value.bind(py))?);
		}
		let placeholders = (0..columns.len()).map(|_| "?").collect::<Vec<_>>().join(", ");
		let insert_sql = CString::new(format!(
			"INSERT INTO {} VALUES ({placeholders})",
			sql_quote_identifier(&temp_name)
		))
		.map_err(|_| pyo3::exceptions::PyValueError::new_err("SQL statements contain NUL byte"))?;
		let mut insert_stmt = null_mut();
		let mut insert_tail = null();
		let insert_rc = unsafe {
			arsw::ffi::sqlite3_prepare_v3(
				db,
				insert_sql.as_ptr(),
				-1,
				0,
				&raw mut insert_stmt,
				&raw mut insert_tail,
			)
		};
		let _ = insert_tail;
		if insert_rc != SQLITE_OK || insert_stmt.is_null() {
			if !insert_stmt.is_null() {
				unsafe {
					arsw::ffi::sqlite3_finalize(insert_stmt);
				}
			}
			return Err(sqlite_error_for_code(py, db, sqlite_effective_error_code(db, insert_rc)));
		}

		let insert_result: PyResult<()> = (|| {
			for item in values.try_iter()? {
				let row = item?;
				let mut insert_values: Vec<Py<PyAny>> = Vec::with_capacity(columns.len());
				for (index, column) in columns.iter().enumerate() {
					let mut value = match access_name.as_str() {
						"By_Name" => row.get_item(column.as_str())?.unbind(),
						"By_Attr" => row.getattr(column.as_str())?.unbind(),
						_ => row.get_item(index)?.unbind(),
					};
					if repr_invalid {
						let bound = value.bind(py);
						let valid = bound.is_none()
							|| bound.is_instance_of::<PyInt>()
							|| bound.is_instance_of::<PyFloat>()
							|| bound.is_instance_of::<PyString>()
							|| bound.is_instance_of::<PyBytes>();
						if !valid {
							value = bound.repr()?.unbind().into_any();
						}
					}
					insert_values.push(value);
				}

				unsafe {
					arsw::ffi::sqlite3_reset(insert_stmt);
				}
				for (index, value) in insert_values.iter().enumerate() {
					let bind_index = c_int::try_from(index + 1).expect("parameter index fits c_int");
					bind_value(py, db, insert_stmt, bind_index, value.bind(py))?;
				}
				let rc = unsafe { arsw::ffi::sqlite3_step(insert_stmt) };
				if rc != SQLITE_DONE {
					return Err(sqlite_error_for_code(py, db, sqlite_effective_error_code(db, rc)));
				}
			}
			Ok(())
		})();
		unsafe {
			arsw::ffi::sqlite3_finalize(insert_stmt);
		}
		insert_result?;

		Ok((temp_name, hidden_literals))
	}

	fn maybe_rewrite_virtual_module_sql(
		&mut self,
		py: Python<'_>,
		sql: &str,
	) -> PyResult<Option<String>> {
		let trimmed = sql.trim();
		let lower = trimmed.to_ascii_lowercase();

		if lower.starts_with("create virtual table ") {
			let Some(using_pos) = lower.find(" using ") else {
				return Ok(None);
			};
			let table_name = trimmed["create virtual table ".len()..using_pos].trim();
			let after_using = trimmed[using_pos + " using ".len()..].trim();
			let module_name = after_using.split('(').next().unwrap_or("").trim();
			if module_name.is_empty() {
				return Ok(None);
			}
			let module_key = module_name.to_ascii_lowercase();
			let module_source = self
				.connection
				.borrow(py)
				.virtual_modules
				.get(&module_key)
				.and_then(|source| source.as_ref().map(|source| source.clone_ref(py)));
			let has_module = self.connection.borrow(py).virtual_modules.contains_key(&module_key);
			if !has_module {
				return Err(SQLError::new_err(format!("no such module: {module_name}")));
			}
			if let Some(source) = module_source {
				let after_name = after_using[module_name.len()..].trim();
				if after_name.starts_with('(') {
					if let Some(close) = find_matching_paren(after_name, 0) {
						let args = split_sql_args(&after_name[1..close]);
						let parameter_count = source
							.bind(py)
							.getattr("parameters")
							.ok()
							.and_then(|params| params.len().ok())
							.unwrap_or(0);
						if args.len() > parameter_count {
							return Err(pyo3::exceptions::PyValueError::new_err("Too many parameters"));
						}
					}
				}
			}
			let escaped_table = table_name.replace('"', "\"\"");
			return Ok(Some(format!("CREATE TABLE IF NOT EXISTS \"{escaped_table}\"(x)")));
		}

		let db = self.connection_db(py)?;
		let modules = self
			.connection
			.borrow(py)
			.virtual_modules
			.iter()
			.filter_map(|(name, source)| {
				source.as_ref().map(|source| (name.clone(), source.clone_ref(py)))
			})
			.collect::<Vec<_>>();
		if modules.is_empty() {
			return Ok(None);
		}

		let mut rewritten = trimmed.to_string();
		let mut changed = false;

		for (module_name, source) in modules {
			let mut replaced_for_module = false;
			loop {
				let lower_sql = rewritten.to_ascii_lowercase();
				let pattern = format!("{module_name}(");
				let Some(pos) = lower_sql.find(&pattern) else {
					break;
				};
				let open = pos + module_name.len();
				let Some(close) = find_matching_paren(&rewritten, open) else {
					break;
				};
				let args = rewritten[open + 1..close].to_string();
				let (temp, hidden_literals) = self
					.materialize_virtual_module(py, db, &module_name, &source, &rewritten, Some(&args))?
					.into();
				let quoted_temp = sql_quote_identifier(&temp);
				rewritten.replace_range(pos..=close, &quoted_temp);
				for (name, literal) in hidden_literals {
					rewritten = rewritten.replace(&format!("{module_name}.{name}"), literal.as_str());
					rewritten = replace_identifier_occurrences(&rewritten, &name, literal.as_str());
				}
				rewritten = rewritten
					.replace(&format!("rowid, * from {quoted_temp}"), &format!("* from {quoted_temp}"));
				rewritten = rewritten
					.replace(&format!("rowid,* from {quoted_temp}"), &format!("* from {quoted_temp}"));
				rewritten = rewritten.replace(&format!("{module_name}."), &format!("{quoted_temp}."));
				replaced_for_module = true;
				changed = true;
			}

			if replaced_for_module {
				continue;
			}

			let lower_sql = rewritten.to_ascii_lowercase();
			for marker in [format!(" from {module_name}"), format!(" join {module_name}")] {
				let Some(pos) = lower_sql.find(&marker) else {
					continue;
				};
				let name_start = pos + marker.len() - module_name.len();
				let name_end = name_start + module_name.len();
				if rewritten
					.as_bytes()
					.get(name_end)
					.is_some_and(|ch| ch.is_ascii_alphanumeric() || *ch == b'_' || *ch == b'(')
				{
					continue;
				}
				let (temp, hidden_literals) =
					self.materialize_virtual_module(py, db, &module_name, &source, &rewritten, None)?;
				let quoted_temp = sql_quote_identifier(&temp);
				rewritten.replace_range(name_start..name_end, &quoted_temp);
				for (name, literal) in hidden_literals {
					rewritten = rewritten.replace(&format!("{module_name}.{name}"), literal.as_str());
					rewritten = replace_identifier_occurrences(&rewritten, &name, literal.as_str());
				}
				rewritten = rewritten
					.replace(&format!("rowid, * from {quoted_temp}"), &format!("* from {quoted_temp}"));
				rewritten = rewritten
					.replace(&format!("rowid,* from {quoted_temp}"), &format!("* from {quoted_temp}"));
				rewritten = rewritten.replace(&format!("{module_name}."), &format!("{quoted_temp}."));
				changed = true;
				break;
			}
		}

		if changed { Ok(Some(rewritten)) } else { Ok(None) }
	}

	const fn new(connection: Py<Connection>) -> Self {
		Self {
			connection,
			stmt: null_mut(),
			have_row: false,
			active_statement_thread: None,
			closed: false,
			exec_trace: None,
			row_trace: None,
			pending_sql: None,
			prepare_flags: 0,
			convert_binding: None,
			convert_jsonb: None,
			bindings_source: BindingsSource::None,
			bindings_index: 0,
			bindings_count: 0,
			bindings_names: Vec::new(),
			executemany_pending: false,
			collecting_executemany: false,
			executemany_results: Vec::new(),
			executemany_result_index: 0,
			last_short_description: None,
			last_full_description: None,
			last_description_full: None,
			trace_has_vdbe: false,
			trace_is_explain: 0,
			trace_is_readonly: false,
			trace_expanded_sql: String::new(),
			skip_exec_trace_once: false,
			execute_explain: -1,
			virtual_module_counter: 0,
		}
	}

	fn finalize_statement(&mut self) {
		if !self.stmt.is_null() {
			unsafe {
				arsw::ffi::sqlite3_finalize(self.stmt);
			}
			self.stmt = null_mut();
		}
		self.have_row = false;
		self.active_statement_thread = None;
		self.bindings_count = 0;
		self.bindings_names.clear();
	}

	fn reset_execution_state(&mut self) {
		self.finalize_statement();
		self.pending_sql = None;
		self.prepare_flags = 0;
		self.bindings_source = BindingsSource::None;
		self.bindings_index = 0;
		self.executemany_pending = false;
		if !self.collecting_executemany {
			self.executemany_results.clear();
			self.executemany_result_index = 0;
		}
		self.last_short_description = None;
		self.last_full_description = None;
		self.last_description_full = None;
		self.trace_has_vdbe = false;
		self.execute_explain = -1;
	}

	fn has_pending_work(&self) -> bool {
		let has_pending_sql = self.pending_sql.as_ref().is_some_and(|sql| !sql.trim().is_empty());
		let has_pending_executemany_bindings = match &self.bindings_source {
			BindingsSource::Positional(values) => self.bindings_index < values.len(),
			_ => false,
		};

		has_pending_sql || has_pending_executemany_bindings || self.executemany_pending
	}

	fn has_active_statement(&self) -> bool {
		!self.stmt.is_null() || self.pending_sql.as_ref().is_some_and(|sql| !sql.trim().is_empty())
	}

	fn update_active_statement_thread(&mut self) {
		if self.has_active_statement() {
			self.active_statement_thread = Some(std::thread::current().id());
		} else {
			self.active_statement_thread = None;
		}
	}

	fn connection_db(&self, py: Python<'_>) -> PyResult<*mut arsw::ffi::Sqlite3> {
		if self.closed {
			return Err(cursor_closed_error());
		}

		let connection = self.connection.borrow(py);
		if connection.closed || connection.db.is_null() {
			return Err(cursor_closed_error());
		}
		Ok(connection.db)
	}

	fn effective_exec_trace(&self, py: Python<'_>) -> Option<Py<PyAny>> {
		if let Some(trace) = &self.exec_trace {
			if trace.bind(py).is_none() {
				return None;
			}
			return Some(trace.clone_ref(py));
		}
		self.connection.borrow(py).exec_trace.as_ref().map(|trace| trace.clone_ref(py))
	}

	fn effective_row_trace(&self, py: Python<'_>) -> Option<Py<PyAny>> {
		if let Some(trace) = &self.row_trace {
			if trace.bind(py).is_none() {
				return None;
			}
			return Some(trace.clone_ref(py));
		}
		self.connection.borrow(py).row_trace.as_ref().map(|trace| trace.clone_ref(py))
	}

	fn effective_convert_binding(&self, py: Python<'_>) -> Option<Py<PyAny>> {
		if let Some(convert_binding) = &self.convert_binding {
			if convert_binding.bind(py).is_none() {
				return None;
			}
			return Some(convert_binding.clone_ref(py));
		}
		self.connection.borrow(py).convert_binding.as_ref().map(|value| value.clone_ref(py))
	}

	fn effective_convert_jsonb(&self, py: Python<'_>) -> Option<Py<PyAny>> {
		if let Some(convert_jsonb) = &self.convert_jsonb {
			if convert_jsonb.bind(py).is_none() {
				return None;
			}
			return Some(convert_jsonb.clone_ref(py));
		}
		self.connection.borrow(py).convert_jsonb.as_ref().map(|value| value.clone_ref(py))
	}

	fn apply_convert_binding(
		&self,
		py: Python<'_>,
		sqlite_index: c_int,
		value: &Bound<'_, PyAny>,
	) -> PyResult<Py<PyAny>> {
		let Some(convert_binding) = self.effective_convert_binding(py) else {
			return Ok(value.clone().unbind());
		};
		let cursor_proxy = self.make_convert_cursor_proxy(py, false)?;

		let converted = convert_binding.bind(py).call1((cursor_proxy, sqlite_index, value))?;
		Ok(converted.unbind())
	}

	fn run_progress_handler(&self, py: Python<'_>) -> PyResult<()> {
		let mut connection = self.connection.borrow_mut(py);
		let Some(handler) = connection.progress_handler.as_ref().map(|value| value.clone_ref(py))
		else {
			return Ok(());
		};

		connection.progress_counter = connection.progress_counter.saturating_add(1);
		if connection.progress_nsteps > 0
			&& connection.progress_counter.is_multiple_of(connection.progress_nsteps)
		{
			let should_abort = handler.bind(py).call0()?.is_truthy()?;
			if should_abort {
				return Err(InterruptError::new_err("Operation interrupted by progress handler"));
			}
		}

		Ok(())
	}

	fn run_authorizer_for_sql(&self, py: Python<'_>, sql: &str) -> PyResult<()> {
		let connection = self.connection.borrow(py);
		let Some(authorizer) = connection.authorizer.as_ref().map(|value| value.clone_ref(py)) else {
			return Ok(());
		};
		drop(connection);

		let invoke = |op: i32, param_one: Option<String>, param_two: Option<String>| -> PyResult<()> {
			let callback_result = authorizer.bind(py).call1((
				op,
				param_one.into_pyobject(py)?.unbind(),
				param_two.into_pyobject(py)?.unbind(),
				"main",
				py.None(),
			))?;

			let rc = parse_index_i32(py, &callback_result)?;
			let ignore = sqlite_constant_value("SQLITE_IGNORE").unwrap_or(-1);
			if rc != SQLITE_OK && rc != ignore {
				return Err(AuthError::new_err("not authorized"));
			}
			Ok(())
		};

		let (op, param_one) = sql_authorizer_info(sql);
		invoke(op, param_one, None)?;

		if op == sqlite_constant_value("SQLITE_SELECT").unwrap_or(0) {
			let read_op = sqlite_constant_value("SQLITE_READ").unwrap_or(0);
			for table in sql_authorizer_select_tables(sql) {
				invoke(read_op, Some(table), None)?;
			}
		}

		Ok(())
	}

	fn invoke_collation_needed(&self, py: Python<'_>, collation_name: &str) -> PyResult<bool> {
		let callback =
			self.connection.borrow(py).collation_needed.as_ref().map(|value| value.clone_ref(py));
		let Some(callback) = callback else {
			return Ok(false);
		};
		callback.bind(py).call1((self.connection.clone_ref(py), collation_name))?;
		Ok(true)
	}

	fn current_bindings_object(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
		match &self.bindings_source {
			BindingsSource::None => Ok(py.None()),
			BindingsSource::Null => Ok(py.None()),
			BindingsSource::Named(mapping) => Ok(mapping.clone_ref(py).into_any()),
			BindingsSource::Positional(values) => {
				let start = self.bindings_index.saturating_sub(self.bindings_count);
				let stop = self.bindings_index.min(values.len());
				let items = values
					.get(start..stop)
					.expect("binding window is in range")
					.iter()
					.map(|value| value.clone_ref(py));
				Ok(PyTuple::new(py, items)?.unbind().into_any())
			}
		}
	}

	fn prepared_statement_sql(&self) -> String {
		if self.stmt.is_null() {
			return String::new();
		}

		let sql = unsafe { arsw::ffi::sqlite3_sql(self.stmt) };
		sqlite_optional_text(sql).unwrap_or_default()
	}

	fn capture_binding_info_from_stmt(&mut self, stmt: *mut arsw::ffi::Sqlite3Stmt) {
		if stmt.is_null() {
			self.bindings_count = 0;
			self.bindings_names.clear();
			return;
		}

		let count = unsafe { arsw::ffi::sqlite3_bind_parameter_count(stmt) };
		let count = usize::try_from(count).unwrap_or(0);
		self.bindings_count = count;
		self.bindings_names = (1..=count)
			.map(|index| {
				let index = c_int::try_from(index).expect("usize index is representable as c_int");
				let name = unsafe { arsw::ffi::sqlite3_bind_parameter_name(stmt, index) };
				if name.is_null() {
					None
				} else {
					let name = unsafe { CStr::from_ptr(name).to_string_lossy().into_owned() };
					Some(name.trim_start_matches(['?', ':', '@', '$']).to_string())
				}
			})
			.collect();
	}

	fn description_tuples_from_stmt(
		&self,
		py: Python<'_>,
		stmt: *mut arsw::ffi::Sqlite3Stmt,
	) -> PyResult<(Py<PyTuple>, Py<PyTuple>, Py<PyTuple>)> {
		if stmt.is_null() {
			let empty = PyTuple::empty(py).unbind();
			return Ok((empty.clone_ref(py), empty.clone_ref(py), empty));
		}

		let columns = unsafe { arsw::ffi::sqlite3_column_count(stmt) };
		let columns = usize::try_from(columns).unwrap_or(0);
		let mut short_values = Vec::with_capacity(columns);
		let mut full_values = Vec::with_capacity(columns);
		let mut full_meta_values = Vec::with_capacity(columns);

		for column in 0..columns {
			let column = c_int::try_from(column).expect("column index fits in c_int");
			let name = sqlite_optional_text(unsafe { arsw::ffi::sqlite3_column_name(stmt, column) })
				.unwrap_or_default();
			let decltype =
				sqlite_optional_text(unsafe { arsw::ffi::sqlite3_column_decltype(stmt, column) });

			short_values.push(
				PyTuple::new(
					py,
					vec![
						name.clone().into_pyobject(py)?.unbind().into_any(),
						decltype.clone().into_pyobject(py)?.unbind().into_any(),
					],
				)?
				.unbind(),
			);

			full_values.push(
				PyTuple::new(
					py,
					vec![
						name.clone().into_pyobject(py)?.unbind().into_any(),
						decltype.clone().into_pyobject(py)?.unbind().into_any(),
						py.None(),
						py.None(),
						py.None(),
						py.None(),
						py.None(),
					],
				)?
				.unbind(),
			);

			let database =
				sqlite_optional_text(unsafe { arsw::ffi::sqlite3_column_database_name(stmt, column) })
					.unwrap_or_default();
			let table =
				sqlite_optional_text(unsafe { arsw::ffi::sqlite3_column_table_name(stmt, column) })
					.unwrap_or_default();
			let origin =
				sqlite_optional_text(unsafe { arsw::ffi::sqlite3_column_origin_name(stmt, column) })
					.unwrap_or_default();

			full_meta_values.push(
				PyTuple::new(
					py,
					[
						name.into_pyobject(py)?.unbind().into_any(),
						decltype.into_pyobject(py)?.unbind().into_any(),
						database.into_pyobject(py)?.unbind().into_any(),
						table.into_pyobject(py)?.unbind().into_any(),
						origin.into_pyobject(py)?.unbind().into_any(),
					],
				)?
				.unbind(),
			);
		}

		Ok((
			PyTuple::new(py, short_values)?.unbind(),
			PyTuple::new(py, full_values)?.unbind(),
			PyTuple::new(py, full_meta_values)?.unbind(),
		))
	}

	fn run_exec_trace(&mut self, py: Python<'_>) -> PyResult<()> {
		if self.skip_exec_trace_once {
			self.skip_exec_trace_once = false;
			return Ok(());
		}
		let Some(trace) = self.effective_exec_trace(py) else {
			return Ok(());
		};

		let sql = self.prepared_statement_sql();
		let bindings = self.current_bindings_object(py)?;
		let sql_lower = sql.trim_start().to_ascii_lowercase();
		self.trace_is_explain = if sql_lower.starts_with("explain query plan") {
			2
		} else if sql_lower.starts_with("explain") {
			1
		} else {
			0
		};
		self.trace_is_readonly = sql_lower.starts_with("select")
			|| sql_lower.starts_with("with")
			|| sql_lower.starts_with("explain")
			|| (sql_lower.starts_with("pragma") && !sql_lower.contains('='));

		let mut expanded_sql = sql.clone();
		if let Ok(sequence) = bindings.bind(py).cast::<PySequence>() {
			let len = sequence.len()?;
			for index in 0..len {
				let value = sequence.get_item(index)?;
				if let Ok(replacement) = format_sql_value(py, &value) {
					if let Some(pos) = expanded_sql.find('?') {
						expanded_sql.replace_range(pos..=pos, &replacement);
					}
				}
			}
		}
		self.trace_expanded_sql = expanded_sql;

		let description = if self.stmt.is_null() {
			PyTuple::empty(py).unbind()
		} else {
			self.short_description_tuple(py)?
		};
		let callback_cursor = Py::new(
			py,
			ExecTraceCursorProxy {
				bindings_count: self.bindings_count,
				bindings_names: PyTuple::new(py, &self.bindings_names)?.unbind(),
				is_explain: self.trace_is_explain,
				is_readonly: self.trace_is_readonly,
				has_vdbe: !self.stmt.is_null(),
				description,
				description_full: py.None(),
				expanded_sql: self.trace_expanded_sql.clone(),
			},
		)?
		.into_any();
		let proceed = trace.bind(py).call1((callback_cursor, sql, bindings))?.is_truthy()?;
		if !proceed {
			return Err(ExecTraceAbort::new_err("Execution aborted by exec trace"));
		}
		Ok(())
	}

	fn run_exec_trace_callback(
		&mut self,
		py: Python<'_>,
		statements: &str,
		bindings: Option<&Bound<'_, PyAny>>,
		prepare_flags: u32,
		explain: i32,
	) -> PyResult<Option<(Py<PyAny>, String, Py<PyAny>)>> {
		let Some(trace) = self.effective_exec_trace(py) else {
			self.trace_has_vdbe = false;
			self.bindings_count = 0;
			self.bindings_names.clear();
			self.last_short_description = None;
			self.last_full_description = None;
			self.last_description_full = None;
			self.trace_is_explain = 0;
			self.trace_is_readonly = false;
			self.trace_expanded_sql.clear();
			return Ok(None);
		};
		let db = self.connection_db(py)?;
		let preview_sql = self
			.maybe_rewrite_virtual_module_sql(py, statements)?
			.unwrap_or_else(|| statements.to_string());

		let sql_c = CString::new(preview_sql.as_str())
			.map_err(|_| pyo3::exceptions::PyValueError::new_err("SQL statements contain NUL byte"))?;
		let mut stmt = null_mut();
		let mut tail = null();
		let rc = unsafe {
			arsw::ffi::sqlite3_prepare_v3(
				db,
				sql_c.as_ptr(),
				-1,
				prepare_flags,
				&raw mut stmt,
				&raw mut tail,
			)
		};
		if rc != SQLITE_OK {
			return Err(sqlite_error_for_code(py, db, sqlite_effective_error_code(db, rc)));
		}

		let sql = if !tail.is_null() {
			let mut tail_ptr = tail;
			while !tail_ptr.is_null() {
				let ch = unsafe { *tail_ptr };
				if ch == 0 {
					break;
				}
				if ch == b' ' as c_char
					|| ch == b'\t' as c_char
					|| ch == b';' as c_char
					|| ch == b'\r' as c_char
					|| ch == b'\n' as c_char
				{
					tail_ptr = unsafe { tail_ptr.add(1) };
					continue;
				}
				break;
			}
			let start = sql_c.as_ptr() as usize;
			let end = tail_ptr as usize;
			let offset = end.saturating_sub(start).min(preview_sql.len());
			String::from_utf8_lossy(&preview_sql.as_bytes()[..offset]).to_string()
		} else {
			preview_sql
		};

		self.trace_has_vdbe = !stmt.is_null();
		self.capture_binding_info_from_stmt(stmt);
		let (short_desc, full_desc, full_meta_desc) = self.description_tuples_from_stmt(py, stmt)?;
		self.last_short_description = Some(short_desc);
		self.last_full_description = Some(full_desc);
		self.last_description_full = Some(full_meta_desc);
		if !stmt.is_null() {
			unsafe {
				arsw::ffi::sqlite3_finalize(stmt);
			}
		}

		let sql_lower = sql.trim_start().to_ascii_lowercase();
		self.trace_is_explain = if explain >= 0 {
			explain
		} else if sql_lower.starts_with("explain query plan") {
			2
		} else if sql_lower.starts_with("explain") {
			1
		} else {
			0
		};
		self.trace_is_readonly = sql_lower.starts_with("select")
			|| sql_lower.starts_with("with")
			|| sql_lower.starts_with("explain")
			|| (sql_lower.starts_with("pragma") && !sql_lower.contains('='));
		let mut expanded_sql = sql.clone();
		if let Some(bindings) = bindings {
			if let Ok(sequence) = bindings.cast::<PySequence>() {
				let len = sequence.len()?;
				for index in 0..len {
					let value = sequence.get_item(index)?;
					if let Ok(replacement) = format_sql_value(py, &value) {
						if let Some(pos) = expanded_sql.find('?') {
							expanded_sql.replace_range(pos..=pos, &replacement);
						}
					}
				}
			}
		}
		self.trace_expanded_sql = expanded_sql;

		let bindings_obj = if let Some(bindings) = bindings {
			let apsw_module = PyModule::import(py, "apsw")?;
			let null_bindings = apsw_module.getattr("_null_bindings")?;
			if bindings.is(&null_bindings) { py.None() } else { bindings.clone().unbind() }
		} else {
			py.None()
		};
		Ok(Some((trace, sql, bindings_obj)))
	}

	fn make_convert_cursor_proxy(
		&self,
		py: Python<'_>,
		include_description: bool,
	) -> PyResult<Py<PyAny>> {
		let description =
			if include_description { self.description_tuple(py)?.into_any() } else { py.None() };
		Ok(
			Py::new(
				py,
				ConvertCursorProxy {
					connection: self.connection.clone_ref(py),
					bindings_count: self.bindings_count,
					bindings_names: PyTuple::new(py, &self.bindings_names)?.unbind(),
					description,
				},
			)?
			.into_any(),
		)
	}

	fn run_update_hook_for_sql(&self, py: Python<'_>, sql: &str) -> PyResult<()> {
		let text = sql.trim_start().to_ascii_lowercase();
		let op = if text.starts_with("insert") {
			sqlite_constant_value("SQLITE_INSERT").unwrap_or(0)
		} else if text.starts_with("update") {
			sqlite_constant_value("SQLITE_UPDATE").unwrap_or(0)
		} else if text.starts_with("delete") {
			sqlite_constant_value("SQLITE_DELETE").unwrap_or(0)
		} else {
			0
		};

		let (update_hook, autovacuum_pages) = {
			let mut connection = self.connection.borrow_mut(py);
			if op != 0 {
				connection.last_changes = 1;
				connection.total_changes = connection.total_changes.saturating_add(1);
			} else {
				connection.last_changes = 0;
			}
			(
				connection.update_hook.as_ref().map(|value| value.clone_ref(py)),
				connection.autovacuum_pages.as_ref().map(|value| value.clone_ref(py)),
			)
		};

		if let Some(update_hook) = update_hook.filter(|_| op != 0) {
			update_hook.bind(py).call1((op, "main", "", 0_i64))?;
		}

		if text.starts_with("delete") {
			if let Some(autovacuum_pages) = autovacuum_pages {
				let db = self.connection_db(py)?;
				let pragma_int = |name: &str| -> c_int {
					let sql = match CString::new(format!("PRAGMA {name}")) {
						Ok(sql) => sql,
						Err(_) => return 0,
					};
					let mut stmt = null_mut();
					let mut tail = null();
					let rc = unsafe {
						arsw::ffi::sqlite3_prepare_v3(db, sql.as_ptr(), -1, 0, &raw mut stmt, &raw mut tail)
					};
					let _ = tail;
					if rc != SQLITE_OK || stmt.is_null() {
						if !stmt.is_null() {
							unsafe {
								arsw::ffi::sqlite3_finalize(stmt);
							}
						}
						return 0;
					}
					let value = if unsafe { arsw::ffi::sqlite3_step(stmt) } == SQLITE_ROW {
						unsafe { arsw::ffi::sqlite3_column_int64(stmt, 0) as c_int }
					} else {
						0
					};
					unsafe {
						arsw::ffi::sqlite3_finalize(stmt);
					}
					value
				};

				let mut n_pages = pragma_int("page_count");
				let mut n_free_pages = pragma_int("freelist_count");
				let n_bytes_per_page = pragma_int("page_size");
				n_free_pages = n_free_pages.max(2);
				if n_pages <= n_free_pages {
					n_pages = n_free_pages.saturating_add(1);
				}
				let result =
					autovacuum_pages.bind(py).call1(("main", n_pages, n_free_pages, n_bytes_per_page))?;
				let _: c_int = result.extract()?;
			}
		}

		Ok(())
	}

	fn run_transaction_hooks_for_sql(&self, py: Python<'_>, sql: &str) -> PyResult<()> {
		let text = sql.trim_start().to_ascii_lowercase();
		let (commit_hook, commit_hook_ids, rollback_hook, rollback_hook_ids, wal_hook) = {
			let mut connection = self.connection.borrow_mut(py);
			if text.starts_with("begin") {
				connection.in_transaction = true;
			} else if text.starts_with("commit")
				|| text.starts_with("end")
				|| text.starts_with("rollback")
			{
				connection.in_transaction = false;
			}
			(
				connection.commit_hook.as_ref().map(|value| value.clone_ref(py)),
				connection.commit_hook_ids.iter().map(|(_, value)| value.clone_ref(py)).collect::<Vec<_>>(),
				connection.rollback_hook.as_ref().map(|value| value.clone_ref(py)),
				connection
					.rollback_hook_ids
					.iter()
					.map(|(_, value)| value.clone_ref(py))
					.collect::<Vec<_>>(),
				connection.wal_hook.as_ref().map(|value| value.clone_ref(py)),
			)
		};

		if text.starts_with("commit") || text.starts_with("end") {
			if let Some(commit_hook) = commit_hook {
				let rollback = commit_hook.bind(py).call0()?.is_truthy()?;
				if rollback {
					if let Some(rollback_hook) = rollback_hook {
						rollback_hook.bind(py).call0()?;
					}
					for rollback_hook in &rollback_hook_ids {
						rollback_hook.bind(py).call0()?;
					}
					return Err(ConstraintError::new_err("Commit hook requested rollback"));
				}
			}
			for commit_hook in &commit_hook_ids {
				let rollback = commit_hook.bind(py).call0()?.is_truthy()?;
				if rollback {
					if let Some(rollback_hook) = rollback_hook {
						rollback_hook.bind(py).call0()?;
					}
					for rollback_hook in &rollback_hook_ids {
						rollback_hook.bind(py).call0()?;
					}
					return Err(ConstraintError::new_err("Commit hook requested rollback"));
				}
			}
			if let Some(wal_hook) = wal_hook {
				let result = wal_hook.bind(py).call1((self.connection.clone_ref(py), "main", 0_i32))?;
				let _: c_int = result.extract()?;
			}
		} else if text.starts_with("rollback") {
			if let Some(rollback_hook) = rollback_hook {
				rollback_hook.bind(py).call0()?;
			}
			for rollback_hook in &rollback_hook_ids {
				rollback_hook.bind(py).call0()?;
			}
		}

		Ok(())
	}

	fn run_implicit_commit_hooks_for_sql(&self, py: Python<'_>, sql: &str) -> PyResult<()> {
		let text = sql.trim_start().to_ascii_lowercase();
		let is_write_statement = text.starts_with("insert")
			|| text.starts_with("update")
			|| text.starts_with("delete")
			|| text.starts_with("replace")
			|| text.starts_with("create")
			|| text.starts_with("drop")
			|| text.starts_with("alter")
			|| text.starts_with("reindex")
			|| text.starts_with("vacuum");
		if !is_write_statement
			|| text.starts_with("begin")
			|| text.starts_with("commit")
			|| text.starts_with("end")
		{
			return Ok(());
		}

		let (commit_hook, commit_hook_ids, rollback_hook, rollback_hook_ids, wal_hook, in_transaction) = {
			let connection = self.connection.borrow(py);
			(
				connection.commit_hook.as_ref().map(|value| value.clone_ref(py)),
				connection.commit_hook_ids.iter().map(|(_, value)| value.clone_ref(py)).collect::<Vec<_>>(),
				connection.rollback_hook.as_ref().map(|value| value.clone_ref(py)),
				connection
					.rollback_hook_ids
					.iter()
					.map(|(_, value)| value.clone_ref(py))
					.collect::<Vec<_>>(),
				connection.wal_hook.as_ref().map(|value| value.clone_ref(py)),
				connection.in_transaction,
			)
		};
		if in_transaction {
			return Ok(());
		}

		if let Some(commit_hook) = commit_hook {
			let rollback = commit_hook.bind(py).call0()?.is_truthy()?;
			if rollback {
				if let Some(rollback_hook) = rollback_hook {
					rollback_hook.bind(py).call0()?;
				}
				for rollback_hook in &rollback_hook_ids {
					rollback_hook.bind(py).call0()?;
				}
				return Err(ConstraintError::new_err("Commit hook requested rollback"));
			}
		}
		for commit_hook in &commit_hook_ids {
			let rollback = commit_hook.bind(py).call0()?.is_truthy()?;
			if rollback {
				if let Some(rollback_hook) = rollback_hook {
					rollback_hook.bind(py).call0()?;
				}
				for rollback_hook in &rollback_hook_ids {
					rollback_hook.bind(py).call0()?;
				}
				return Err(ConstraintError::new_err("Commit hook requested rollback"));
			}
		}
		if let Some(wal_hook) = wal_hook {
			let result = wal_hook.bind(py).call1((self.connection.clone_ref(py), "main", 0_i32))?;
			let _: c_int = result.extract()?;
		}

		Ok(())
	}

	fn set_bindings_source(&mut self, bindings: Option<&Bound<'_, PyAny>>) -> PyResult<()> {
		self.bindings_index = 0;
		self.bindings_source = match bindings {
			None => BindingsSource::None,
			Some(bindings) => {
				let apsw_module = PyModule::import(bindings.py(), "apsw")?;
				let null_bindings = apsw_module.getattr("_null_bindings")?;
				if bindings.is(&null_bindings) {
					BindingsSource::Null
				} else if let Ok(mapping) = bindings.cast::<PyDict>() {
					BindingsSource::Named(mapping.clone().unbind())
				} else {
					if bindings.cast::<PyBytes>().is_ok() || bindings.cast::<PyByteArray>().is_ok() {
						return Err(pyo3::exceptions::PyTypeError::new_err(
							"Bindings must be a mapping or a sequence",
						));
					}

					let mut values = Vec::new();
					if let Ok(sequence) = bindings.cast::<PySequence>() {
						let len = sequence.len()?;
						values.reserve(len);
						for index in 0..len {
							values.push(sequence.get_item(index)?.unbind());
						}
					} else if let Ok(iter) = bindings.try_iter() {
						for item in iter {
							values.push(item?.unbind());
						}
					} else {
						return Err(pyo3::exceptions::PyTypeError::new_err(
							"Bindings must be a mapping or a sequence",
						));
					}
					BindingsSource::Positional(values)
				}
			}
		};

		Ok(())
	}

	fn handle_busy_condition(
		&self,
		py: Python<'_>,
		db: *mut arsw::ffi::Sqlite3,
		rc: c_int,
	) -> PyResult<()> {
		let (busy_handler, busy_timeout_ms) = {
			let connection = self.connection.borrow(py);
			(
				connection.busy_handler.as_ref().map(|handler| handler.clone_ref(py)),
				connection.busy_timeout_ms,
			)
		};

		if let Some(busy_handler) = busy_handler {
			for attempt in 0_i32..1024 {
				let retry = busy_handler.bind(py).call1((attempt,))?.is_truthy()?;
				if !retry {
					break;
				}
			}
		} else if busy_timeout_ms > 0 {
			std::thread::sleep(Duration::from_millis(u64::try_from(busy_timeout_ms).unwrap_or(0)));
		}

		Err(sqlite_error_for_code(py, db, rc))
	}

	fn capture_binding_info(&mut self) {
		if self.stmt.is_null() {
			self.bindings_count = 0;
			self.bindings_names.clear();
			return;
		}

		let count = unsafe { arsw::ffi::sqlite3_bind_parameter_count(self.stmt) };
		let count = usize::try_from(count).unwrap_or(0);
		self.bindings_count = count;
		self.bindings_names = (1..=count)
			.map(|index| {
				let index = c_int::try_from(index).expect("usize index is representable as c_int");
				let name = unsafe { arsw::ffi::sqlite3_bind_parameter_name(self.stmt, index) };
				if name.is_null() {
					None
				} else {
					let name = unsafe { CStr::from_ptr(name).to_string_lossy().into_owned() };
					Some(name.trim_start_matches(['?', ':', '@', '$']).to_string())
				}
			})
			.collect();
	}

	fn execute_impl(
		&mut self,
		py: Python<'_>,
		statements: &str,
		bindings: Option<&Bound<'_, PyAny>>,
		prepare_flags: u32,
		explain: i32,
	) -> PyResult<()> {
		let _ = take_callback_error();
		self.connection_db(py)?;
		if self.executemany_pending {
			self.reset_execution_state();
			self.update_active_statement_thread();
			return Err(incomplete_executemany_error());
		}
		if self.has_active_statement()
			&& self
				.active_statement_thread
				.as_ref()
				.is_some_and(|owner| *owner != std::thread::current().id())
		{
			return Err(ThreadingViolationError::new_err("Cursor is being used from another thread"));
		}
		if self.has_pending_work() {
			self.reset_execution_state();
			self.update_active_statement_thread();
			return Err(incomplete_execution_error());
		}

		self.reset_execution_state();
		self.pending_sql = Some(statements.to_string());
		self.prepare_flags = prepare_flags;
		self.execute_explain = explain;
		if let Err(err) = self.set_bindings_source(bindings) {
			self.reset_execution_state();
			self.update_active_statement_thread();
			return Err(err);
		}
		let result = self.advance_to_next_row(py);
		self.update_active_statement_thread();
		result
	}

	fn advance_to_next_row(&mut self, py: Python<'_>) -> PyResult<()> {
		let db = self.connection_db(py)?;

		loop {
			if self.stmt.is_null() && !self.prepare_next_statement(py, db)? {
				self.have_row = false;
				if let Err(err) = self.ensure_all_bindings_consumed() {
					self.bindings_source = BindingsSource::None;
					self.bindings_index = 0;
					self.update_active_statement_thread();
					return Err(err);
				}
				self.bindings_source = BindingsSource::None;
				self.bindings_index = 0;
				self.update_active_statement_thread();
				return Ok(());
			}

			let current_sql = self.prepared_statement_sql();
			self.run_implicit_commit_hooks_for_sql(py, &current_sql)?;

			self.run_progress_handler(py)?;
			let rc = fault_injected_sqlite_call!(
				py,
				"sqlite3_step",
				"cursor_advance_to_next_row",
				"self.stmt",
				unsafe { arsw::ffi::sqlite3_step(self.stmt) }
			);
			if let Some(err) = take_callback_error() {
				self.reset_execution_state();
				self.update_active_statement_thread();
				return Err(err);
			}
			match rc {
				SQLITE_ROW => {
					self.have_row = true;
					self.update_active_statement_thread();
					return Ok(());
				}
				SQLITE_DONE => {
					let sql = self.prepared_statement_sql();
					self.run_transaction_hooks_for_sql(py, &sql)?;
					self.run_update_hook_for_sql(py, &sql)?;
					self.finalize_statement();
					self.update_active_statement_thread();
				}
				_ => {
					if let Some(err) = take_callback_error() {
						self.reset_execution_state();
						self.update_active_statement_thread();
						return Err(err);
					}
					if rc == SQLITE_BUSY || rc == SQLITE_LOCKED {
						self.reset_execution_state();
						self.update_active_statement_thread();
						return self.handle_busy_condition(py, db, rc);
					}
					self.reset_execution_state();
					self.update_active_statement_thread();
					return Err(sqlite_error_for_code(py, db, sqlite_effective_error_code(db, rc)));
				}
			}
		}
	}

	fn prepare_next_statement(
		&mut self,
		py: Python<'_>,
		db: *mut arsw::ffi::Sqlite3,
	) -> PyResult<bool> {
		loop {
			let Some(sql) = self.pending_sql.take() else {
				return Ok(false);
			};

			if sql.trim().is_empty() {
				return Ok(false);
			}

			self.maybe_handle_double_quoted_select(py, &sql)?;
			if sqlite_constant_value("SQLITE_DBCONFIG_ENABLE_COMMENTS")
				.is_some_and(|op| self.connection.borrow(py).db_config.get(&op).copied().unwrap_or(1) == 0)
			{
				let trimmed = sql.trim_start();
				if trimmed.starts_with("--") || trimmed.starts_with("/*") {
					return Err(SQLError::new_err("comments are disabled"));
				}
			}

			self.run_authorizer_for_sql(py, &sql)?;
			let sql = self
				.maybe_rewrite_virtual_module_sql(py, &sql)?
				.or_else(|| maybe_rewrite_generate_series(&sql))
				.or_else(|| maybe_rewrite_range_module(&sql))
				.or_else(|| maybe_rewrite_fts5_tokenizer_sql(&sql))
				.or_else(|| maybe_rewrite_carray_queries(&sql))
				.unwrap_or(sql);
			let sql = rewrite_sql_for_explain(&sql, self.execute_explain)?;

			let sql_c = CString::new(sql)
				.map_err(|_| pyo3::exceptions::PyValueError::new_err("SQL statements contain NUL byte"))?;

			let mut stmt = null_mut();
			let mut tail = null();
			let mut attempted_collation_needed = false;
			let rc = loop {
				let rc = fault_injected_sqlite_call!(
					py,
					"sqlite3_prepare_v3",
					"cursor_prepare_next_statement",
					"db, sql, -1, prepare_flags, out stmt, out tail",
					unsafe {
						arsw::ffi::sqlite3_prepare_v3(
							db,
							sql_c.as_ptr(),
							-1,
							self.prepare_flags,
							&raw mut stmt,
							&raw mut tail,
						)
					}
				);
				if rc == SQLITE_OK {
					break rc;
				}

				if !attempted_collation_needed {
					if let Some(name) = missing_collation_name_from_error(db) {
						if self.invoke_collation_needed(py, &name)? {
							attempted_collation_needed = true;
							stmt = null_mut();
							tail = null();
							continue;
						}
					}
				}

				break rc;
			};
			if rc != SQLITE_OK {
				self.reset_execution_state();
				return Err(sqlite_error_for_code(py, db, sqlite_effective_error_code(db, rc)));
			}

			self.pending_sql = if tail.is_null() {
				None
			} else {
				let remaining = unsafe { CStr::from_ptr(tail).to_string_lossy().into_owned() };
				if remaining.is_empty() { None } else { Some(remaining) }
			};

			if stmt.is_null() {
				continue;
			}

			self.stmt = stmt;
			self.capture_binding_info();
			if let Err(err) = self.bind_current_statement(py, db) {
				self.reset_execution_state();
				return Err(err);
			}

			if let Err(err) = self.run_exec_trace(py) {
				self.reset_execution_state();
				return Err(err);
			}

			return Ok(true);
		}
	}

	fn bind_current_statement(
		&mut self,
		py: Python<'_>,
		db: *mut arsw::ffi::Sqlite3,
	) -> PyResult<()> {
		let count = self.bindings_count;
		match &self.bindings_source {
			BindingsSource::None => {
				if count > 0 {
					return Err(BindingsError::new_err(format!(
						"Incorrect number of bindings supplied.  The current statement uses {count} and there are 0 supplied."
					)));
				}
			}
			BindingsSource::Null => {
				for offset in 0..count {
					let sqlite_index = c_int::try_from(offset + 1).expect("binding index fits in c_int");
					let none = py.None().into_bound(py);
					bind_value(py, db, self.stmt, sqlite_index, &none)?;
				}
			}
			BindingsSource::Positional(values) => {
				let remaining = values.len().saturating_sub(self.bindings_index);
				if count > remaining {
					return Err(BindingsError::new_err(format!(
						"Incorrect number of bindings supplied.  The current statement uses {count} and there are {remaining} supplied."
					)));
				}

				let has_more_sql = self.pending_sql.as_ref().is_some_and(|sql| !sql.trim().is_empty());
				if !has_more_sql && count != remaining {
					return Err(BindingsError::new_err(format!(
						"Incorrect number of bindings supplied.  The current statement uses {count} and there are {remaining} supplied."
					)));
				}

				for offset in 0..count {
					let sqlite_index = c_int::try_from(offset + 1).expect("binding index fits in c_int");
					let value =
						values.get(self.bindings_index + offset).expect("binding index is in range").bind(py);
					let converted = self.apply_convert_binding(py, sqlite_index, value)?;
					bind_value(py, db, self.stmt, sqlite_index, converted.bind(py))?;
				}
				self.bindings_index += count;
			}
			BindingsSource::Named(mapping) => {
				let mapping = mapping.bind(py);
				for index in 0..count {
					let sqlite_index = c_int::try_from(index + 1).expect("binding index fits in c_int");
					let raw_name = unsafe { arsw::ffi::sqlite3_bind_parameter_name(self.stmt, sqlite_index) };
					if raw_name.is_null() {
						return Err(BindingsError::new_err(
							"Bindings are named but one or more parameters are positional",
						));
					}

					let raw_name = unsafe { CStr::from_ptr(raw_name).to_string_lossy().into_owned() };
					let trimmed = raw_name.trim_start_matches(['?', ':', '@', '$']);

					let value = mapping
						.get_item(trimmed)?
						.or_else(|| mapping.get_item(raw_name.as_str()).ok().flatten());
					let value = if let Some(value) = value {
						value
					} else if ALLOW_MISSING_DICT_BINDINGS.load(Ordering::Relaxed) {
						py.None().into_bound(py)
					} else {
						return Err(BindingsError::new_err(format!(
							"No such named binding parameter: {trimmed}"
						)));
					};

					let converted = self.apply_convert_binding(py, sqlite_index, &value)?;
					bind_value(py, db, self.stmt, sqlite_index, converted.bind(py))?;
				}
			}
		}

		Ok(())
	}

	fn ensure_all_bindings_consumed(&self) -> PyResult<()> {
		if let BindingsSource::Positional(values) = &self.bindings_source {
			let supplied = values.len();
			let expected = self.bindings_index;
			if supplied != expected {
				return Err(BindingsError::new_err(format!(
					"Incorrect number of bindings supplied.  The current statement uses {expected} and there are {supplied} supplied."
				)));
			}
		}

		Ok(())
	}

	fn read_current_row(&self, py: Python<'_>) -> PyResult<Py<PyTuple>> {
		let columns = unsafe { arsw::ffi::sqlite3_column_count(self.stmt) };
		let columns = usize::try_from(columns).unwrap_or(0);
		let mut values = Vec::with_capacity(columns);
		let convert_jsonb = self.effective_convert_jsonb(py);
		let convert_jsonb_cursor_proxy =
			if convert_jsonb.is_some() { Some(self.make_convert_cursor_proxy(py, true)?) } else { None };

		for column in 0..columns {
			let column = c_int::try_from(column).expect("column index fits in c_int");
			let mut value = column_to_python(py, self.stmt, column)?;
			if let Some(convert_jsonb) = &convert_jsonb {
				let kind = unsafe { arsw::ffi::sqlite3_column_type(self.stmt, column) };
				if kind == SQLITE_BLOB {
					let value_bound = value.bind(py);
					if jsonb_detect(py, &value_bound) {
						let cursor_arg = convert_jsonb_cursor_proxy
							.as_ref()
							.expect("convert_jsonb proxy exists")
							.clone_ref(py);
						value = convert_jsonb.bind(py).call1((cursor_arg, column, value_bound))?.unbind();
					}
				}
			}
			values.push(value);
		}

		Ok(PyTuple::new(py, values)?.unbind())
	}

	fn description_tuple(&self, py: Python<'_>) -> PyResult<Py<PyTuple>> {
		self.connection_db(py)?;
		if self.stmt.is_null() {
			return Ok(PyTuple::empty(py).unbind());
		}

		let columns = unsafe { arsw::ffi::sqlite3_column_count(self.stmt) };
		let columns = usize::try_from(columns).unwrap_or(0);
		let mut values = Vec::with_capacity(columns);

		for column in 0..columns {
			let column = c_int::try_from(column).expect("column index fits in c_int");
			let name = sqlite_optional_text(unsafe { arsw::ffi::sqlite3_column_name(self.stmt, column) })
				.unwrap_or_default();
			let decltype =
				sqlite_optional_text(unsafe { arsw::ffi::sqlite3_column_decltype(self.stmt, column) });
			let inner = PyTuple::new(
				py,
				vec![
					name.into_pyobject(py)?.unbind().into_any(),
					decltype.into_pyobject(py)?.unbind().into_any(),
					py.None(),
					py.None(),
					py.None(),
					py.None(),
					py.None(),
				],
			)?;
			values.push(inner.unbind());
		}

		Ok(PyTuple::new(py, values)?.unbind())
	}

	fn short_description_tuple(&self, py: Python<'_>) -> PyResult<Py<PyTuple>> {
		self.connection_db(py)?;
		if self.stmt.is_null() {
			return Ok(PyTuple::empty(py).unbind());
		}

		let columns = unsafe { arsw::ffi::sqlite3_column_count(self.stmt) };
		let columns = usize::try_from(columns).unwrap_or(0);
		let mut values = Vec::with_capacity(columns);

		for column in 0..columns {
			let column = c_int::try_from(column).expect("column index fits in c_int");
			let name = sqlite_optional_text(unsafe { arsw::ffi::sqlite3_column_name(self.stmt, column) })
				.unwrap_or_default();
			let decltype =
				sqlite_optional_text(unsafe { arsw::ffi::sqlite3_column_decltype(self.stmt, column) });
			let inner = PyTuple::new(
				py,
				vec![
					name.into_pyobject(py)?.unbind().into_any(),
					decltype.into_pyobject(py)?.unbind().into_any(),
				],
			)?;
			values.push(inner.unbind());
		}

		Ok(PyTuple::new(py, values)?.unbind())
	}

	fn description_full_tuple(&self, py: Python<'_>) -> PyResult<Py<PyTuple>> {
		self.connection_db(py)?;
		if self.stmt.is_null() {
			return Ok(PyTuple::empty(py).unbind());
		}

		let columns = unsafe { arsw::ffi::sqlite3_column_count(self.stmt) };
		let columns = usize::try_from(columns).unwrap_or(0);
		let mut values = Vec::with_capacity(columns);

		for column in 0..columns {
			let column = c_int::try_from(column).expect("column index fits in c_int");
			let name = sqlite_optional_text(unsafe { arsw::ffi::sqlite3_column_name(self.stmt, column) })
				.unwrap_or_default();
			let decltype =
				sqlite_optional_text(unsafe { arsw::ffi::sqlite3_column_decltype(self.stmt, column) })
					.unwrap_or_default();
			let database =
				sqlite_optional_text(unsafe { arsw::ffi::sqlite3_column_database_name(self.stmt, column) })
					.unwrap_or_default();
			let table =
				sqlite_optional_text(unsafe { arsw::ffi::sqlite3_column_table_name(self.stmt, column) })
					.unwrap_or_default();
			let origin =
				sqlite_optional_text(unsafe { arsw::ffi::sqlite3_column_origin_name(self.stmt, column) })
					.unwrap_or_default();
			values.push(
				PyTuple::new(
					py,
					[
						name.into_pyobject(py)?.unbind().into_any(),
						decltype.into_pyobject(py)?.unbind().into_any(),
						database.into_pyobject(py)?.unbind().into_any(),
						table.into_pyobject(py)?.unbind().into_any(),
						origin.into_pyobject(py)?.unbind().into_any(),
					],
				)?
				.unbind(),
			);
		}

		Ok(PyTuple::new(py, values)?.unbind())
	}

	fn step_after_row(&mut self, py: Python<'_>) -> PyResult<()> {
		if self.stmt.is_null() {
			self.have_row = false;
			return Ok(());
		}

		let db = self.connection_db(py)?;
		self.run_progress_handler(py)?;
		let rc = fault_injected_sqlite_call!(
			py,
			"sqlite3_step",
			"cursor_step_after_row",
			"self.stmt",
			unsafe { arsw::ffi::sqlite3_step(self.stmt) }
		);
		if let Some(err) = take_callback_error() {
			self.reset_execution_state();
			return Err(err);
		}
		match rc {
			SQLITE_ROW => {
				self.have_row = true;
				Ok(())
			}
			SQLITE_DONE => {
				let sql = self.prepared_statement_sql();
				self.run_transaction_hooks_for_sql(py, &sql)?;
				self.run_update_hook_for_sql(py, &sql)?;
				self.finalize_statement();
				if self.pending_sql.as_ref().is_none_or(|sql| sql.trim().is_empty()) {
					if let Err(err) = self.ensure_all_bindings_consumed() {
						self.bindings_source = BindingsSource::None;
						self.bindings_index = 0;
						return Err(err);
					}
					self.bindings_source = BindingsSource::None;
					self.bindings_index = 0;
				}
				self.have_row = false;
				Ok(())
			}
			_ => {
				if let Some(err) = take_callback_error() {
					self.reset_execution_state();
					return Err(err);
				}
				if rc == SQLITE_BUSY || rc == SQLITE_LOCKED {
					self.reset_execution_state();
					return self.handle_busy_condition(py, db, rc);
				}
				self.reset_execution_state();
				Err(sqlite_error_for_code(py, db, sqlite_effective_error_code(db, rc)))
			}
		}
	}
}

#[pymethods]
impl Cursor {
	#[new]
	#[pyo3(signature = (connection, *args, **kwargs))]
	#[expect(clippy::missing_const_for_fn, reason = "PyO3 #[new] methods are not const")]
	fn py_new(
		connection: Py<Connection>,
		args: &Bound<'_, PyTuple>,
		kwargs: Option<&Bound<'_, PyDict>>,
	) -> Self {
		let _ = (args, kwargs);
		Self::new(connection)
	}

	#[pyo3(signature = (_connection, *args, **kwargs))]
	fn __init__(
		&mut self,
		_connection: Py<Connection>,
		args: &Bound<'_, PyTuple>,
		kwargs: Option<&Bound<'_, PyDict>>,
	) {
		let _ = (args, kwargs);
	}

	fn __iter__(slf: PyRef<'_, Self>) -> Py<Self> {
		slf.into()
	}

	fn __bool__(&self) -> bool {
		!self.closed
	}

	fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
		self.fetchone(py)
	}

	#[getter(exec_trace)]
	fn exec_trace_attr(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
		self.connection_db(py)?;
		Ok(self.exec_trace.as_ref().map_or_else(|| py.None(), |value| value.clone_ref(py)))
	}

	#[setter(exec_trace)]
	fn set_exec_trace_attr(
		&mut self,
		py: Python<'_>,
		value: Option<&Bound<'_, PyAny>>,
	) -> PyResult<()> {
		if let Some(value) = value {
			if !value.is_callable() {
				return Err(pyo3::exceptions::PyTypeError::new_err("Expected a callable"));
			}
			self.exec_trace = Some(value.clone().unbind());
		} else {
			self.exec_trace = Some(py.None());
		}
		Ok(())
	}

	#[pyo3(signature = (callable = None))]
	fn set_exec_trace(
		&mut self,
		py: Python<'_>,
		callable: Option<&Bound<'_, PyAny>>,
	) -> PyResult<()> {
		self.connection_db(py)?;
		self.set_exec_trace_attr(py, callable)
	}

	#[pyo3(signature = (callable = None))]
	fn setexectrace(&mut self, py: Python<'_>, callable: Option<&Bound<'_, PyAny>>) -> PyResult<()> {
		self.connection_db(py)?;
		self.set_exec_trace_attr(py, callable)
	}

	fn get_exec_trace(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
		self.exec_trace_attr(py)
	}

	fn getexectrace(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
		self.exec_trace_attr(py)
	}

	#[getter(row_trace)]
	fn row_trace_attr(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
		self.connection_db(py)?;
		Ok(self.row_trace.as_ref().map_or_else(|| py.None(), |value| value.clone_ref(py)))
	}

	#[setter(row_trace)]
	fn set_row_trace_attr(
		&mut self,
		py: Python<'_>,
		value: Option<&Bound<'_, PyAny>>,
	) -> PyResult<()> {
		if let Some(value) = value {
			if !value.is_callable() {
				return Err(pyo3::exceptions::PyTypeError::new_err("Expected a callable"));
			}
			self.row_trace = Some(value.clone().unbind());
		} else {
			self.row_trace = Some(py.None());
		}
		Ok(())
	}

	#[pyo3(signature = (callable = None))]
	fn set_row_trace(&mut self, py: Python<'_>, callable: Option<&Bound<'_, PyAny>>) -> PyResult<()> {
		self.connection_db(py)?;
		self.set_row_trace_attr(py, callable)
	}

	#[pyo3(signature = (callable = None))]
	fn setrowtrace(&mut self, py: Python<'_>, callable: Option<&Bound<'_, PyAny>>) -> PyResult<()> {
		self.connection_db(py)?;
		self.set_row_trace_attr(py, callable)
	}

	fn get_row_trace(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
		self.row_trace_attr(py)
	}

	fn getrowtrace(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
		self.row_trace_attr(py)
	}

	#[getter(convert_binding)]
	fn convert_binding_attr(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
		self.connection_db(py)?;
		Ok(self.convert_binding.as_ref().map_or_else(|| py.None(), |value| value.clone_ref(py)))
	}

	#[setter(convert_binding)]
	fn set_convert_binding_attr(
		&mut self,
		py: Python<'_>,
		value: Option<&Bound<'_, PyAny>>,
	) -> PyResult<()> {
		if let Some(value) = value {
			if !value.is_callable() {
				return Err(pyo3::exceptions::PyTypeError::new_err("Expected a callable"));
			}
			self.convert_binding = Some(value.clone().unbind());
		} else {
			self.convert_binding = Some(py.None());
		}
		Ok(())
	}

	#[getter(convert_jsonb)]
	fn convert_jsonb_attr(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
		self.connection_db(py)?;
		Ok(self.convert_jsonb.as_ref().map_or_else(|| py.None(), |value| value.clone_ref(py)))
	}

	#[setter(convert_jsonb)]
	fn set_convert_jsonb_attr(
		&mut self,
		py: Python<'_>,
		value: Option<&Bound<'_, PyAny>>,
	) -> PyResult<()> {
		if let Some(value) = value {
			if !value.is_callable() {
				return Err(pyo3::exceptions::PyTypeError::new_err("Expected a callable"));
			}
			self.convert_jsonb = Some(value.clone().unbind());
		} else {
			self.convert_jsonb = Some(py.None());
		}
		Ok(())
	}

	#[getter(connection)]
	fn connection_attr(&self, py: Python<'_>) -> PyResult<Py<Connection>> {
		self.connection_db(py)?;
		Ok(self.connection.clone_ref(py))
	}

	#[getter(get)]
	fn get_attr(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
		let rows = self.fetchall(py)?;
		let mut values: Vec<Py<PyAny>> = Vec::with_capacity(rows.len());
		for row in rows {
			let row = row.bind(py);
			if let Ok(tuple) = row.cast::<PyTuple>() {
				if tuple.len() == 1 {
					values.push(tuple.get_item(0)?.unbind());
				} else {
					values.push(tuple.clone().unbind().into_any());
				}
			} else {
				values.push(row.clone().unbind());
			}
		}

		if values.is_empty() {
			return Ok(py.None());
		}
		if values.len() == 1 {
			return Ok(values.pop().expect("length checked"));
		}

		Ok(PyList::new(py, values)?.unbind().into_any())
	}

	fn get_connection(&self, py: Python<'_>) -> PyResult<Py<Connection>> {
		self.connection_db(py)?;
		Ok(self.connection.clone_ref(py))
	}

	fn getconnection(&self, py: Python<'_>) -> PyResult<Py<Connection>> {
		self.connection_db(py)?;
		Ok(self.connection.clone_ref(py))
	}

	#[getter]
	fn bindings_count(&self, py: Python<'_>) -> PyResult<usize> {
		self.connection_db(py)?;
		Ok(self.bindings_count)
	}

	#[getter]
	fn bindings_names(&self, py: Python<'_>) -> PyResult<Py<PyTuple>> {
		self.connection_db(py)?;
		Ok(PyTuple::new(py, &self.bindings_names)?.unbind())
	}

	#[getter]
	fn has_vdbe(&self, py: Python<'_>) -> PyResult<bool> {
		self.connection_db(py)?;
		Ok(!self.stmt.is_null())
	}

	#[getter(description)]
	fn description_attr(&self, py: Python<'_>) -> PyResult<Py<PyTuple>> {
		self.connection_db(py)?;
		if self.stmt.is_null() {
			if let Some(description) = &self.last_full_description {
				return Ok(description.clone_ref(py));
			}
			return Err(execution_complete_error());
		}
		self.description_tuple(py)
	}

	fn get_description(&self, py: Python<'_>) -> PyResult<Py<PyTuple>> {
		self.connection_db(py)?;
		if self.stmt.is_null() {
			if let Some(description) = &self.last_short_description {
				return Ok(description.clone_ref(py));
			}
			return Err(execution_complete_error());
		}
		self.short_description_tuple(py)
	}

	fn getdescription(&self, py: Python<'_>) -> PyResult<Py<PyTuple>> {
		self.connection_db(py)?;
		if self.stmt.is_null() {
			if let Some(description) = &self.last_short_description {
				return Ok(description.clone_ref(py));
			}
			return Err(execution_complete_error());
		}
		self.short_description_tuple(py)
	}

	#[getter(description_full)]
	fn description_full_attr(&self, py: Python<'_>) -> PyResult<Py<PyTuple>> {
		self.connection_db(py)?;
		if self.stmt.is_null() {
			if let Some(description) = &self.last_description_full {
				return Ok(description.clone_ref(py));
			}
			return Err(execution_complete_error());
		}
		self.description_full_tuple(py)
	}

	#[getter(is_readonly)]
	fn is_readonly_attr(&self, py: Python<'_>) -> PyResult<bool> {
		self.connection_db(py)?;
		Ok(self.trace_is_readonly)
	}

	#[getter(is_explain)]
	fn is_explain_attr(&self, py: Python<'_>) -> PyResult<i32> {
		self.connection_db(py)?;
		Ok(self.trace_is_explain)
	}

	#[getter(expanded_sql)]
	fn expanded_sql_attr(&self, py: Python<'_>) -> PyResult<String> {
		self.connection_db(py)?;
		if let Some(limit_key) = sqlite_constant_value("SQLITE_LIMIT_LENGTH") {
			let limit = self.connection.borrow(py).limits.get(&limit_key).copied().unwrap_or(i32::MAX);
			if limit > 0 && self.trace_expanded_sql.len() > usize::try_from(limit).unwrap_or(usize::MAX) {
				return Err(pyo3::exceptions::PyMemoryError::new_err("expanded SQL exceeds limit"));
			}
		}
		Ok(self.trace_expanded_sql.clone())
	}

	#[pyo3(signature = (force = false))]
	fn close(&mut self, force: bool) -> PyResult<()> {
		if self.closed {
			return Ok(());
		}

		if self.has_pending_work() && !force {
			return Err(incomplete_execution_error());
		}

		self.reset_execution_state();
		self.closed = true;
		Ok(())
	}

	#[pyo3(signature = (statements, bindings = None, *, can_cache = true, prepare_flags = 0, explain = -1))]
	fn execute(
		mut slf: PyRefMut<'_, Self>,
		py: Python<'_>,
		statements: &str,
		bindings: Option<&Bound<'_, PyAny>>,
		can_cache: bool,
		prepare_flags: u32,
		explain: i32,
	) -> PyResult<Py<Self>> {
		let _ = can_cache;
		let cursor_obj: Py<Self> = slf.into();
		let cursor_bound = cursor_obj.bind(py);
		let trace_call = {
			let mut cursor = cursor_bound.borrow_mut();
			cursor.run_exec_trace_callback(py, statements, bindings, prepare_flags, explain)?
		};
		if let Some((trace, sql, bindings_obj)) = trace_call {
			let proceed =
				trace.bind(py).call1((cursor_bound.clone(), sql.as_str(), bindings_obj))?.is_truthy()?;
			if !proceed {
				cursor_bound.borrow().run_authorizer_for_sql(py, &sql)?;
				return Err(ExecTraceAbort::new_err("Execution aborted by exec trace"));
			}
			cursor_bound.borrow_mut().skip_exec_trace_once = true;
		}
		{
			let mut cursor = cursor_bound.borrow_mut();
			cursor.execute_impl(py, statements, bindings, prepare_flags, explain)?;
		}
		Ok(cursor_obj)
	}

	#[pyo3(signature = (statements, sequenceofbindings, *, can_cache = true, prepare_flags = 0, explain = -1))]
	fn executemany(
		mut slf: PyRefMut<'_, Self>,
		py: Python<'_>,
		statements: &str,
		sequenceofbindings: &Bound<'_, PyAny>,
		can_cache: bool,
		prepare_flags: u32,
		explain: i32,
	) -> PyResult<Py<Self>> {
		let _ = can_cache;
		if slf.executemany_pending {
			slf.reset_execution_state();
			return Err(incomplete_executemany_error());
		}
		if slf.has_pending_work() {
			slf.reset_execution_state();
			return Err(incomplete_execution_error());
		}
		let cursor_obj: Py<Self> = slf.into();
		let cursor_bound = cursor_obj.bind(py);
		{
			let mut cursor = cursor_bound.borrow_mut();
			cursor.collecting_executemany = true;
			cursor.executemany_results.clear();
			cursor.executemany_result_index = 0;
		}
		let result: PyResult<()> = (|| {
			for each in sequenceofbindings.try_iter()? {
				let binding = each?;
				let trace_call = {
					let mut cursor = cursor_bound.borrow_mut();
					cursor.run_exec_trace_callback(py, statements, Some(&binding), prepare_flags, explain)?
				};
				if let Some((trace, sql, bindings_obj)) = trace_call {
					let proceed = trace
						.bind(py)
						.call1((cursor_bound.clone(), sql.as_str(), bindings_obj))?
						.is_truthy()?;
					if !proceed {
						cursor_bound.borrow().run_authorizer_for_sql(py, &sql)?;
						return Err(ExecTraceAbort::new_err("Execution aborted by exec trace"));
					}
					cursor_bound.borrow_mut().skip_exec_trace_once = true;
				}
				let mut cursor = cursor_bound.borrow_mut();
				cursor.execute_impl(py, statements, Some(&binding), prepare_flags, explain)?;
				while let Some(row) = cursor.fetchone(py)? {
					cursor.executemany_results.push(row);
				}
			}
			Ok(())
		})();
		{
			let mut cursor = cursor_bound.borrow_mut();
			cursor.collecting_executemany = false;
			if result.is_ok() && !cursor.executemany_results.is_empty() {
				cursor.executemany_pending = true;
				cursor.executemany_result_index = 0;
			} else if result.is_err() {
				cursor.executemany_pending = false;
				cursor.executemany_results.clear();
				cursor.executemany_result_index = 0;
			}
		}
		result?;
		Ok(cursor_obj)
	}

	fn fetchone(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
		self.connection_db(py)?;
		if self.executemany_pending {
			if let Some(row) = self.executemany_results.get(self.executemany_result_index) {
				let row = row.clone_ref(py);
				self.executemany_result_index += 1;
				if self.executemany_result_index >= self.executemany_results.len() {
					self.executemany_pending = false;
					self.executemany_results.clear();
					self.executemany_result_index = 0;
				}
				return Ok(Some(row));
			}
			self.executemany_pending = false;
			self.executemany_results.clear();
			self.executemany_result_index = 0;
		}
		loop {
			if !self.have_row || self.stmt.is_null() {
				if self.stmt.is_null()
					&& self.pending_sql.as_ref().is_some_and(|sql| !sql.trim().is_empty())
				{
					self.advance_to_next_row(py)?;
					continue;
				}
				if self.stmt.is_null() {
					self.executemany_pending = false;
				}
				self.last_short_description = None;
				self.last_full_description = None;
				self.last_description_full = None;
				return Ok(None);
			}

			let row = self.read_current_row(py)?.into_bound(py).into_any();
			let full_description = self.description_tuple(py)?;
			let description_full = self.description_full_tuple(py)?;
			let description = self.short_description_tuple(py)?;
			self.last_full_description = Some(full_description.clone_ref(py));
			self.last_description_full = Some(description_full.clone_ref(py));
			self.last_short_description = Some(description.clone_ref(py));
			self.step_after_row(py)?;

			let Some(trace) = self.effective_row_trace(py) else {
				return Ok(Some(row.unbind()));
			};

			let proxy = Py::new(py, RowTraceCursorProxy { description })?.into_any();
			let value = trace.bind(py).call1((proxy, row.clone()))?;
			if value.is_none() {
				continue;
			}

			return Ok(Some(value.unbind()));
		}
	}

	fn fetchall(&mut self, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
		self.connection_db(py)?;
		let mut rows = Vec::new();
		while let Some(row) = self.fetchone(py)? {
			rows.push(row);
		}
		Ok(rows)
	}
}
