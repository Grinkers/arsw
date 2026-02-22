use super::*;
use pyo3::IntoPyObject;

pub fn parse_index_i32(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<i32> {
	let operator = PyModule::import(py, "operator")?;
	let indexed = operator.getattr("index")?.call1((value,))?;
	let text = indexed.str()?.to_str()?.to_string();
	let parsed = text
		.parse::<i64>()
		.map_err(|_| pyo3::exceptions::PyOverflowError::new_err(format!("{text} overflowed C int")))?;

	if parsed > i64::from(i32::MAX) || parsed < i64::from(i32::MIN) {
		return Err(pyo3::exceptions::PyOverflowError::new_err(format!("{text} overflowed C int")));
	}

	Ok(i32::try_from(parsed).expect("checked i64 range against i32 bounds"))
}

pub fn parse_index_i64(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<i64> {
	let operator = PyModule::import(py, "operator")?;
	let indexed = operator.getattr("index")?.call1((value,))?;
	indexed.extract::<i64>()
}

pub(crate) fn bind_value(
	py: Python<'_>,
	db: *mut arsw::ffi::Sqlite3,
	stmt: *mut arsw::ffi::Sqlite3Stmt,
	index: c_int,
	value: &Bound<'_, PyAny>,
) -> PyResult<()> {
	let rc = if value.is_none() {
		fault_injected_sqlite_call!(py, "sqlite3_bind_null", "bind_value", "stmt, index", unsafe {
			arsw::ffi::sqlite3_bind_null(stmt, index)
		})
	} else if value.is_instance_of::<PyBool>() {
		let value = value.extract::<bool>()?;
		fault_injected_sqlite_call!(
			py,
			"sqlite3_bind_int64",
			"bind_value",
			"stmt, index, bool value",
			unsafe { arsw::ffi::sqlite3_bind_int64(stmt, index, i64::from(value)) }
		)
	} else if let Ok(value) = value.extract::<i64>() {
		fault_injected_sqlite_call!(
			py,
			"sqlite3_bind_int64",
			"bind_value",
			"stmt, index, i64 value",
			unsafe { arsw::ffi::sqlite3_bind_int64(stmt, index, value) }
		)
	} else if let Ok(value) = value.extract::<f64>() {
		fault_injected_sqlite_call!(
			py,
			"sqlite3_bind_double",
			"bind_value",
			"stmt, index, f64 value",
			unsafe { arsw::ffi::sqlite3_bind_double(stmt, index, value) }
		)
	} else if let Ok(zero_blob) = value.cast::<ZeroBlob>() {
		let bytes = vec![0_u8; zero_blob.borrow().length];
		fault_injected_sqlite_call!(
			py,
			"sqlite3_bind_blob64",
			"bind_value",
			"stmt, index, zeroblob ptr, zeroblob len, transient",
			unsafe {
				arsw::ffi::sqlite3_bind_blob64(
					stmt,
					index,
					bytes.as_ptr().cast(),
					u64::try_from(bytes.len()).expect("zeroblob length fits in u64"),
					Some(sqlite_transient()),
				)
			}
		)
	} else if let Ok(value) = value.extract::<String>() {
		fault_injected_sqlite_call!(
			py,
			"sqlite3_bind_text64",
			"bind_value",
			"stmt, index, text ptr, text len, transient, SQLITE_UTF8",
			unsafe {
				arsw::ffi::sqlite3_bind_text64(
					stmt,
					index,
					value.as_ptr().cast(),
					u64::try_from(value.len()).expect("string length fits in u64"),
					Some(sqlite_transient()),
					SQLITE_UTF8,
				)
			}
		)
	} else if let Ok(value) = value.cast::<PyBytes>() {
		let bytes = value.as_bytes();
		fault_injected_sqlite_call!(
			py,
			"sqlite3_bind_blob64",
			"bind_value",
			"stmt, index, bytes ptr, bytes len, transient",
			unsafe {
				arsw::ffi::sqlite3_bind_blob64(
					stmt,
					index,
					bytes.as_ptr().cast(),
					u64::try_from(bytes.len()).expect("bytes length fits in u64"),
					Some(sqlite_transient()),
				)
			}
		)
	} else if let Ok(value) = value.cast::<PyByteArray>() {
		let bytes = unsafe { value.as_bytes() };
		fault_injected_sqlite_call!(
			py,
			"sqlite3_bind_blob64",
			"bind_value",
			"stmt, index, bytearray ptr, bytearray len, transient",
			unsafe {
				arsw::ffi::sqlite3_bind_blob64(
					stmt,
					index,
					bytes.as_ptr().cast(),
					u64::try_from(bytes.len()).expect("bytes length fits in u64"),
					Some(sqlite_transient()),
				)
			}
		)
	} else if {
		let memoryview_type = PyModule::import(py, "builtins")?.getattr("memoryview")?;
		value.is_instance(&memoryview_type)?
	} {
		let c_contiguous = value.getattr("c_contiguous")?.is_truthy()?;
		if !c_contiguous {
			return Err(pyo3::exceptions::PyBufferError::new_err("buffer is not contiguous"));
		}
		let bytes_obj = PyModule::import(py, "builtins")?.getattr("bytes")?.call1((value,))?;
		let bytes = bytes_obj.cast::<PyBytes>()?.as_bytes();
		fault_injected_sqlite_call!(
			py,
			"sqlite3_bind_blob64",
			"bind_value",
			"stmt, index, memoryview bytes ptr, memoryview bytes len, transient",
			unsafe {
				arsw::ffi::sqlite3_bind_blob64(
					stmt,
					index,
					bytes.as_ptr().cast(),
					u64::try_from(bytes.len()).expect("bytes length fits in u64"),
					Some(sqlite_transient()),
				)
			}
		)
	} else if let Ok(adapter) = value.getattr("to_sqlite_value") {
		if adapter.is_callable() {
			let converted = adapter.call0()?;
			return bind_value(py, db, stmt, index, &converted);
		}

		let text = adapter.str()?.to_str()?.to_string();
		fault_injected_sqlite_call!(
			py,
			"sqlite3_bind_text64",
			"bind_value",
			"stmt, index, adapter str ptr, adapter str len, transient, SQLITE_UTF8",
			unsafe {
				arsw::ffi::sqlite3_bind_text64(
					stmt,
					index,
					text.as_ptr().cast(),
					u64::try_from(text.len()).expect("string length fits in u64"),
					Some(sqlite_transient()),
					SQLITE_UTF8,
				)
			}
		)
	} else {
		return Err(pyo3::exceptions::PyTypeError::new_err(format!(
			"Bad binding argument type supplied at position {index}"
		)));
	};

	if rc != SQLITE_OK {
		return Err(sqlite_error_for_code(py, db, sqlite_effective_error_code(db, rc)));
	}

	Ok(())
}

pub(crate) fn sqlite_optional_text(text: *const std::ffi::c_char) -> Option<String> {
	if text.is_null() {
		None
	} else {
		Some(unsafe { CStr::from_ptr(text).to_string_lossy().into_owned() })
	}
}

pub(crate) fn column_to_python(
	py: Python<'_>,
	stmt: *mut arsw::ffi::Sqlite3Stmt,
	column: c_int,
) -> PyResult<Py<PyAny>> {
	let kind = unsafe { arsw::ffi::sqlite3_column_type(stmt, column) };
	let value = match kind {
		SQLITE_INTEGER => {
			let value = unsafe { arsw::ffi::sqlite3_column_int64(stmt, column) };
			value.into_pyobject(py)?.unbind().into_any()
		}
		SQLITE_FLOAT => {
			let value = unsafe { arsw::ffi::sqlite3_column_double(stmt, column) };
			value.into_pyobject(py)?.unbind().into_any()
		}
		SQLITE_TEXT => {
			let text = unsafe { arsw::ffi::sqlite3_column_text(stmt, column) };
			let bytes =
				usize::try_from(unsafe { arsw::ffi::sqlite3_column_bytes(stmt, column) }).unwrap_or(0);
			let text = if text.is_null() {
				String::new()
			} else {
				let data = unsafe { std::slice::from_raw_parts(text, bytes) };
				String::from_utf8_lossy(data).into_owned()
			};
			text.into_pyobject(py)?.unbind().into_any()
		}
		SQLITE_BLOB => {
			let blob = unsafe { arsw::ffi::sqlite3_column_blob(stmt, column) };
			let bytes =
				usize::try_from(unsafe { arsw::ffi::sqlite3_column_bytes(stmt, column) }).unwrap_or(0);
			let data = if blob.is_null() {
				&[][..]
			} else {
				unsafe { std::slice::from_raw_parts(blob.cast(), bytes) }
			};
			PyBytes::new(py, data).unbind().into_any()
		}
		_ => py.None(),
	};

	Ok(value)
}

#[pyfunction]
pub(crate) fn sqlite_lib_version() -> String {
	arsw::sqlite_lib_version()
}

#[pyfunction]
pub(crate) fn sqlite3_sourceid() -> String {
	arsw::sqlite_source_id()
}

pub(crate) fn apsw_version_from_header() -> &'static str {
	let marker = "#define APSW_VERSION \"";
	let Some(start) = APSW_VERSION_HEADER.find(marker) else {
		return env!("CARGO_PKG_VERSION");
	};

	let rest = &APSW_VERSION_HEADER[start + marker.len()..];
	let Some(end) = rest.find('"') else {
		return env!("CARGO_PKG_VERSION");
	};

	&rest[..end]
}

#[pyfunction]
pub(crate) fn apsw_version() -> String {
	apsw_version_from_header().to_string()
}

#[pyfunction]
pub(crate) fn connections(py: Python<'_>) -> PyResult<Py<PyList>> {
	Ok(PyList::empty(py).unbind())
}

#[pyfunction(signature = (op, *args))]
pub(crate) fn config(py: Python<'_>, op: c_int, args: &Bound<'_, PyTuple>) -> PyResult<Py<PyAny>> {
	let sqlite_config_log = sqlite_constant_value("SQLITE_CONFIG_LOG").unwrap_or(-1);
	let sqlite_config_singlethread =
		sqlite_constant_value("SQLITE_CONFIG_SINGLETHREAD").unwrap_or(-1);
	let sqlite_config_multithread = sqlite_constant_value("SQLITE_CONFIG_MULTITHREAD").unwrap_or(-1);
	let sqlite_config_serialized = sqlite_constant_value("SQLITE_CONFIG_SERIALIZED").unwrap_or(-1);
	let sqlite_config_memstatus = sqlite_constant_value("SQLITE_CONFIG_MEMSTATUS").unwrap_or(-1);
	let sqlite_config_pcache_hdrsz =
		sqlite_constant_value("SQLITE_CONFIG_PCACHE_HDRSZ").unwrap_or(-1);
	let sqlite_config_pmasz = sqlite_constant_value("SQLITE_CONFIG_PMASZ").unwrap_or(-1);
	let sqlite_config_mmap_size = sqlite_constant_value("SQLITE_CONFIG_MMAP_SIZE").unwrap_or(-1);
	let sqlite_config_memdb_maxsize =
		sqlite_constant_value("SQLITE_CONFIG_MEMDB_MAXSIZE").unwrap_or(-1);

	if op == sqlite_config_log {
		if args.len() != 1 {
			return Err(pyo3::exceptions::PyTypeError::new_err(
				"config(SQLITE_CONFIG_LOG, handler) expected exactly 2 arguments",
			));
		}
		let handler = args.get_item(0)?;
		if handler.is_none() {
			if let Ok(mut slot) = sqlite_log_handler().lock() {
				*slot = None;
			}
		} else {
			if !handler.is_callable() {
				return Err(pyo3::exceptions::PyTypeError::new_err("Expected a callable"));
			}
			if let Ok(mut slot) = sqlite_log_handler().lock() {
				*slot = Some(handler.unbind());
			}
		}
		return Ok(py.None());
	}

	if op == sqlite_config_singlethread
		|| op == sqlite_config_multithread
		|| op == sqlite_config_serialized
	{
		if !args.is_empty() {
			return Err(pyo3::exceptions::PyTypeError::new_err("config expected exactly 1 argument"));
		}
		return Ok(py.None());
	}

	if op == sqlite_config_memstatus {
		if args.len() != 1 {
			return Err(pyo3::exceptions::PyTypeError::new_err("config expected exactly 2 arguments"));
		}
		if SQLITE_IS_INITIALIZED.load(Ordering::Relaxed) {
			return Err(MisuseError::new_err(
				"sqlite3_config(SQLITE_CONFIG_MEMSTATUS) can only be called before sqlite initialization",
			));
		}
		let value = args.get_item(0)?;
		let _ = value.extract::<bool>()?;
		return Ok(py.None());
	}

	if op == sqlite_config_pcache_hdrsz {
		if !args.is_empty() {
			return Err(pyo3::exceptions::PyTypeError::new_err("config expected exactly 1 argument"));
		}
		return Ok(0_i32.into_pyobject(py)?.into_any().unbind());
	}

	if op == sqlite_config_pmasz {
		if args.len() != 1 {
			return Err(pyo3::exceptions::PyTypeError::new_err("config expected exactly 2 arguments"));
		}
		let _ = parse_index_i32(py, &args.get_item(0)?)?;
		return Ok(py.None());
	}

	if op == sqlite_config_mmap_size {
		if args.len() != 2 {
			return Err(pyo3::exceptions::PyTypeError::new_err("config expected exactly 3 arguments"));
		}
		let _ = parse_index_i64(py, &args.get_item(0)?)?;
		let _ = parse_index_i64(py, &args.get_item(1)?)?;
		return Ok(py.None());
	}

	if op == sqlite_config_memdb_maxsize {
		if args.len() != 1 {
			return Err(pyo3::exceptions::PyTypeError::new_err("config expected exactly 2 arguments"));
		}
		let _ = parse_index_i64(py, &args.get_item(0)?)?;
		return Ok(py.None());
	}

	Err(pyo3::exceptions::PyTypeError::new_err("Unknown config operation"))
}

#[pyfunction]
pub(crate) fn initialize(py: Python<'_>) -> PyResult<()> {
	let rc = fault_injected_sqlite_call!(py, "sqlite3_initialize", "initialize", "", SQLITE_OK);
	if rc != SQLITE_OK {
		return Err(sqlite_error_for_global_code(py, rc, "initialize failed"));
	}
	SQLITE_IS_INITIALIZED.store(true, Ordering::Relaxed);
	Ok(())
}

#[pyfunction]
pub(crate) fn shutdown(py: Python<'_>) -> PyResult<()> {
	let rc = fault_injected_sqlite_call!(py, "sqlite3_shutdown", "shutdown", "", SQLITE_OK);
	if rc != SQLITE_OK {
		return Err(sqlite_error_for_global_code(py, rc, "shutdown failed"));
	}
	SQLITE_IS_INITIALIZED.store(false, Ordering::Relaxed);
	Ok(())
}

#[pyfunction]
pub(crate) fn log(py: Python<'_>, errorcode: c_int, message: &str) -> PyResult<()> {
	emit_sqlite_log(py, errorcode, message)
}

#[pyfunction(signature = (op, reset = false))]
pub(crate) fn status(op: c_int, reset: bool) -> PyResult<(i64, i64)> {
	if !is_status_operation(op) {
		return Err(MisuseError::new_err("Unknown status operation"));
	}
	let _ = reset;
	Ok((0, 0))
}

#[pyfunction]
pub(crate) fn soft_heap_limit(limit: i64) -> i64 {
	limit
}

#[pyfunction]
pub(crate) fn hard_heap_limit(limit: i64) -> i64 {
	limit
}

#[pyfunction]
pub(crate) fn release_memory(_amount: i64) -> i64 {
	0
}

#[pyfunction]
pub(crate) fn randomness(py: Python<'_>, amount: usize) -> PyResult<Py<PyBytes>> {
	let os = PyModule::import(py, "os")?;
	let bytes = os.getattr("urandom")?.call1((amount,))?.cast_into::<PyBytes>()?;
	Ok(bytes.unbind())
}

#[pyfunction]
pub(crate) fn allow_missing_dict_bindings(value: bool) -> bool {
	ALLOW_MISSING_DICT_BINDINGS.swap(value, Ordering::Relaxed)
}

#[pyfunction]
pub(crate) fn memory_used() -> i64 {
	0
}

#[pyfunction(signature = (reset = false))]
pub(crate) fn memory_high_water(reset: bool) -> i64 {
	let _ = reset;
	0
}

#[pyfunction]
pub(crate) fn keywords() -> Vec<String> {
	Vec::new()
}

#[pyfunction]
pub(crate) fn zeroblob(length: usize) -> ZeroBlob {
	ZeroBlob { length }
}

#[pyfunction]
pub(crate) fn pyobject(value: &Bound<'_, PyAny>) -> Py<PyAny> {
	value.clone().unbind()
}

#[pyfunction]
pub(crate) fn stricmp(string1: &str, string2: &str) -> c_int {
	let left = string1.to_ascii_lowercase();
	let right = string2.to_ascii_lowercase();
	if left == right {
		0
	} else if left < right {
		-1
	} else {
		1
	}
}

#[pyfunction]
pub(crate) fn strglob(glob: &str, string: &str) -> c_int {
	fn glob_match(pattern: &[u8], text: &[u8]) -> bool {
		if pattern.is_empty() {
			return text.is_empty();
		}

		match pattern[0] {
			b'*' => {
				glob_match(&pattern[1..], text) || (!text.is_empty() && glob_match(pattern, &text[1..]))
			}
			b'?' => !text.is_empty() && glob_match(&pattern[1..], &text[1..]),
			c => !text.is_empty() && c == text[0] && glob_match(&pattern[1..], &text[1..]),
		}
	}

	c_int::from(!glob_match(glob.as_bytes(), string.as_bytes()))
}

#[pyfunction]
pub(crate) fn complete(statement: &str) -> PyResult<bool> {
	let statement = CString::new(statement)
		.map_err(|_| pyo3::exceptions::PyValueError::new_err("SQL statement contains NUL byte"))?;
	Ok(unsafe { arsw::ffi::sqlite3_complete(statement.as_ptr()) } != 0)
}

#[pyfunction(signature = (glob, string, escape = 0))]
pub(crate) fn strlike(glob: &str, string: &str, escape: c_int) -> c_int {
	let escape = u8::try_from(escape).ok();

	fn like_match(pattern: &[u8], text: &[u8], escape: Option<u8>) -> bool {
		if pattern.is_empty() {
			return text.is_empty();
		}

		let head = pattern[0];
		if Some(head) == escape {
			if pattern.len() < 2 || text.is_empty() {
				return false;
			}
			return pattern[1] == text[0] && like_match(&pattern[2..], &text[1..], escape);
		}

		match head {
			b'%' => {
				like_match(&pattern[1..], text, escape)
					|| (!text.is_empty() && like_match(pattern, &text[1..], escape))
			}
			b'_' => !text.is_empty() && like_match(&pattern[1..], &text[1..], escape),
			_ => !text.is_empty() && head == text[0] && like_match(&pattern[1..], &text[1..], escape),
		}
	}

	c_int::from(!like_match(glob.as_bytes(), string.as_bytes(), escape))
}

#[pyfunction]
pub(crate) fn format_sql_value(_py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<String> {
	if value.is_none() {
		return Ok("NULL".to_string());
	}

	if let Ok(text) = value.extract::<String>() {
		return Ok(format!("'{}'", text.replace("'", "''")));
	}

	if let Ok(bytes) = value.cast::<PyBytes>() {
		let hex = bytes.as_bytes().iter().map(|byte| format!("{byte:02x}")).collect::<String>();
		return Ok(format!("x'{hex}'"));
	}

	if let Ok(bytes) = value.cast::<PyByteArray>() {
		let hex =
			unsafe { bytes.as_bytes().iter().map(|byte| format!("{byte:02x}")).collect::<String>() };
		return Ok(format!("x'{hex}'"));
	}

	if let Ok(number) = value.extract::<i64>() {
		return Ok(number.to_string());
	}

	if let Ok(number) = value.extract::<f64>() {
		return Ok(number.to_string());
	}

	Err(pyo3::exceptions::PyTypeError::new_err("Unsupported value type for SQL literal formatting"))
}

pub(crate) fn sqlite_constants_from_stubs() -> Vec<(&'static str, i32)> {
	let mut constants = Vec::new();

	for raw in APSW_STUBS.lines() {
		let line = raw.trim_start();
		if !line.starts_with("SQLITE_") && !line.starts_with("FTS5_") {
			continue;
		}

		let Some((name, value_str)) = line.split_once(": int = ") else {
			continue;
		};

		if !name.bytes().all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
		{
			continue;
		}

		if let Ok(value) = value_str.parse::<i32>() {
			constants.push((name, value));
		}
	}

	if !constants.iter().any(|(name, _)| *name == "FTS5_TOKEN_COLOCATED") {
		constants.push(("FTS5_TOKEN_COLOCATED", 1));
	}

	constants
}

pub fn emit_sqlite_log(py: Python<'_>, errorcode: c_int, message: &str) -> PyResult<()> {
	let callback =
		sqlite_log_handler().lock().ok().and_then(|guard| guard.as_ref().map(|h| h.clone_ref(py)));
	if let Some(callback) = callback {
		if let Err(err) = callback.bind(py).call1((errorcode, message)) {
			err.write_unraisable(py, None);
		}
	}
	Ok(())
}

pub fn sqlite_constant_value(name: &str) -> Option<i32> {
	sqlite_constants_from_stubs()
		.into_iter()
		.find_map(|(constant, value)| (constant == name).then_some(value))
}

pub fn is_dbconfig_operation(op: c_int) -> bool {
	sqlite_constants_from_stubs()
		.into_iter()
		.any(|(name, value)| name.starts_with("SQLITE_DBCONFIG_") && value == op)
}

pub fn is_status_operation(op: c_int) -> bool {
	sqlite_constants_from_stubs()
		.into_iter()
		.any(|(name, value)| name.starts_with("SQLITE_STATUS_") && value == op)
}

pub fn is_dbstatus_operation(op: c_int) -> bool {
	sqlite_constants_from_stubs()
		.into_iter()
		.any(|(name, value)| name.starts_with("SQLITE_DBSTATUS_") && value == op)
}

pub fn split_sql_column_definitions(create_sql: &str) -> Vec<String> {
	let Some(start) = create_sql.find('(') else {
		return Vec::new();
	};
	let Some(end) = create_sql.rfind(')') else {
		return Vec::new();
	};
	if end <= start {
		return Vec::new();
	}

	let mut parts = Vec::new();
	let mut current = String::new();
	let mut depth = 0_i32;
	let mut in_single = false;
	let mut in_double = false;
	for ch in create_sql[start + 1..end].chars() {
		match ch {
			'\'' if !in_double => {
				in_single = !in_single;
				current.push(ch);
			}
			'"' if !in_single => {
				in_double = !in_double;
				current.push(ch);
			}
			'(' if !in_single && !in_double => {
				depth += 1;
				current.push(ch);
			}
			')' if !in_single && !in_double => {
				depth = depth.saturating_sub(1);
				current.push(ch);
			}
			',' if !in_single && !in_double && depth == 0 => {
				let part = current.trim();
				if !part.is_empty() {
					parts.push(part.to_string());
				}
				current.clear();
			}
			_ => current.push(ch),
		}
	}
	let tail = current.trim();
	if !tail.is_empty() {
		parts.push(tail.to_string());
	}
	parts
}

pub fn normalize_sql_identifier(token: &str) -> String {
	let token = token.trim();
	token.trim_matches('"').trim_matches('`').trim_matches('[').trim_matches(']').to_string()
}

pub fn parse_column_collation_and_autoincrement(
	create_sql: &str,
	column: &str,
) -> (Option<String>, bool) {
	for definition in split_sql_column_definitions(create_sql) {
		let mut words = definition.split_whitespace();
		let Some(first) = words.next() else {
			continue;
		};
		if normalize_sql_identifier(first) != column {
			continue;
		}

		let tokens = definition.split_whitespace().collect::<Vec<_>>();
		let mut collation = None;
		for pair in tokens.windows(2) {
			if pair[0].eq_ignore_ascii_case("collate") {
				collation = Some(normalize_sql_identifier(pair[1]));
				break;
			}
		}
		let lower = definition.to_ascii_lowercase();
		let autoincrement = lower.contains("primary key") && lower.contains("autoincrement");
		return (collation, autoincrement);
	}
	(None, false)
}

pub fn sql_authorizer_info(sql: &str) -> (i32, Option<String>) {
	let text = sql.trim_start().to_ascii_lowercase();

	if text.starts_with("create table") {
		let op = sqlite_constant_value("SQLITE_CREATE_TABLE").unwrap_or(0);
		let mut words = text.split_whitespace();
		let _ = words.next();
		let _ = words.next();
		let mut next = words.next().unwrap_or("");
		if next == "if" {
			let _ = words.next();
			let _ = words.next();
			next = words.next().unwrap_or("");
		}
		return (op, (!next.is_empty()).then_some(next.trim_matches('"').to_string()));
	}

	if text.starts_with("insert") {
		return (sqlite_constant_value("SQLITE_INSERT").unwrap_or(0), None);
	}

	if text.starts_with("update") {
		return (sqlite_constant_value("SQLITE_UPDATE").unwrap_or(0), None);
	}

	if text.starts_with("delete") {
		return (sqlite_constant_value("SQLITE_DELETE").unwrap_or(0), None);
	}

	if text.starts_with("select") {
		return (sqlite_constant_value("SQLITE_SELECT").unwrap_or(0), None);
	}

	(0, None)
}

pub fn sql_authorizer_select_tables(sql: &str) -> Vec<String> {
	let mut tables = Vec::new();
	let mut in_from_clause = false;
	let mut expect_table = false;

	for raw_token in sql.split_whitespace() {
		let token = raw_token.trim_matches(|c: char| c == ',' || c == ';' || c == '(' || c == ')');
		if token.is_empty() {
			continue;
		}
		let lower = token.to_ascii_lowercase();

		if lower == "from" {
			in_from_clause = true;
			expect_table = true;
			continue;
		}
		if lower == "join" {
			expect_table = true;
			continue;
		}
		if in_from_clause
			&& (lower == "where"
				|| lower == "group"
				|| lower == "order"
				|| lower == "limit"
				|| lower == "having"
				|| lower == "union"
				|| lower == "intersect"
				|| lower == "except")
		{
			in_from_clause = false;
			expect_table = false;
			continue;
		}

		if expect_table {
			if lower == "select" {
				expect_table = false;
				continue;
			}
			let table_token = token.split('.').next_back().unwrap_or(token);
			let table = normalize_sql_identifier(table_token);
			if !table.is_empty() && !tables.contains(&table) {
				tables.push(table);
			}
			expect_table = raw_token.trim_end().ends_with(',');
			continue;
		}

		if in_from_clause && raw_token.trim_end().ends_with(',') {
			expect_table = true;
		}
	}

	tables
}

pub fn rewrite_sql_for_explain(sql: &str, explain: i32) -> PyResult<String> {
	if !(-1..=2).contains(&explain) {
		return Err(SQLError::new_err("Bad explain value"));
	}
	if explain < 0 {
		return Ok(sql.to_string());
	}

	let trimmed = sql.trim_start();
	let lower = trimmed.to_ascii_lowercase();
	let core = if lower.starts_with("explain query plan ") {
		trimmed["explain query plan ".len()..].trim_start()
	} else if lower.starts_with("explain ") {
		trimmed["explain ".len()..].trim_start()
	} else {
		trimmed
	};

	match explain {
		0 => Ok(core.to_string()),
		1 => Ok(format!("EXPLAIN {core}")),
		2 => Ok(format!("EXPLAIN QUERY PLAN {core}")),
		_ => unreachable!(),
	}
}

pub fn sql_quote_identifier(name: &str) -> String {
	format!("\"{}\"", name.replace('"', "\"\""))
}

pub fn replace_identifier_occurrences(sql: &str, name: &str, replacement: &str) -> String {
	if name.is_empty() {
		return sql.to_string();
	}

	let name_bytes = name.as_bytes();
	let mut out = String::with_capacity(sql.len());
	let bytes = sql.as_bytes();
	let mut index = 0;
	let mut in_single = false;
	let mut in_double = false;

	while index < bytes.len() {
		let current = bytes[index];
		if current == b'\'' && !in_double {
			in_single = !in_single;
			out.push(char::from(current));
			index += 1;
			continue;
		}
		if current == b'"' && !in_single {
			in_double = !in_double;
			out.push(char::from(current));
			index += 1;
			continue;
		}

		if !in_single
			&& !in_double
			&& index + name_bytes.len() <= bytes.len()
			&& &bytes[index..index + name_bytes.len()] == name_bytes
			&& (index == 0 || !is_identifier_byte(bytes[index - 1]))
			&& (index + name_bytes.len() == bytes.len()
				|| !is_identifier_byte(bytes[index + name_bytes.len()]))
		{
			out.push_str(replacement);
			index += name_bytes.len();
			continue;
		}

		out.push(char::from(current));
		index += 1;
	}

	out
}

pub fn find_matching_paren(text: &str, open_index: usize) -> Option<usize> {
	if text.as_bytes().get(open_index).copied() != Some(b'(') {
		return None;
	}
	let mut depth = 0_i32;
	let mut in_single = false;
	let mut in_double = false;
	for (rel, ch) in text[open_index..].char_indices() {
		let index = open_index + rel;
		match ch {
			'\'' if !in_double => in_single = !in_single,
			'"' if !in_single => in_double = !in_double,
			'(' if !in_single && !in_double => depth += 1,
			')' if !in_single && !in_double => {
				depth -= 1;
				if depth == 0 {
					return Some(index);
				}
			}
			_ => {}
		}
	}
	None
}

pub fn split_sql_args(args: &str) -> Vec<String> {
	let mut out = Vec::new();
	let mut cur = String::new();
	let mut depth = 0_i32;
	let mut in_single = false;
	let mut in_double = false;
	for ch in args.chars() {
		match ch {
			'\'' if !in_double => {
				in_single = !in_single;
				cur.push(ch);
			}
			'"' if !in_single => {
				in_double = !in_double;
				cur.push(ch);
			}
			'(' if !in_single && !in_double => {
				depth += 1;
				cur.push(ch);
			}
			')' if !in_single && !in_double => {
				depth = depth.saturating_sub(1);
				cur.push(ch);
			}
			',' if !in_single && !in_double && depth == 0 => {
				out.push(cur.trim().to_string());
				cur.clear();
			}
			_ => cur.push(ch),
		}
	}
	if !cur.trim().is_empty() {
		out.push(cur.trim().to_string());
	}
	out
}

pub fn parse_simple_sql_value(py: Python<'_>, token: &str) -> PyResult<Py<PyAny>> {
	let token = token.trim();
	if token.eq_ignore_ascii_case("null") {
		return Ok(py.None());
	}
	if token.starts_with('\'') && token.ends_with('\'') && token.len() >= 2 {
		let value = token[1..token.len() - 1].replace("''", "'");
		return Ok(value.into_pyobject(py)?.unbind().into_any());
	}
	if token.len() > 3
		&& (token.starts_with("x'") || token.starts_with("X'"))
		&& token.ends_with('\'')
	{
		let hex = &token[2..token.len() - 1];
		if hex.len().is_multiple_of(2) {
			let mut bytes = Vec::with_capacity(hex.len() / 2);
			let mut ok = true;
			for i in (0..hex.len()).step_by(2) {
				match u8::from_str_radix(&hex[i..i + 2], 16) {
					Ok(v) => bytes.push(v),
					Err(_) => {
						ok = false;
						break;
					}
				}
			}
			if ok {
				return Ok(PyBytes::new(py, &bytes).unbind().into_any());
			}
		}
	}
	if let Ok(value) = token.parse::<i64>() {
		return Ok(value.into_pyobject(py)?.unbind().into_any());
	}
	if let Ok(value) = token.parse::<f64>() {
		return Ok(value.into_pyobject(py)?.unbind().into_any());
	}
	Ok(token.to_string().into_pyobject(py)?.unbind().into_any())
}

pub const fn is_identifier_byte(ch: u8) -> bool {
	ch.is_ascii_alphanumeric() || ch == b'_'
}

pub fn sqlite_callback_error(context: *mut arsw::ffi::Sqlite3Context, message: &str) {
	if let Ok(message) = CString::new(message) {
		unsafe {
			arsw::ffi::sqlite3_result_error(context, message.as_ptr(), -1);
		}
	} else {
		unsafe {
			arsw::ffi::sqlite3_result_error(context, c"Callback error".as_ptr(), -1);
		}
	}
}

pub fn sqlite_value_to_python(
	py: Python<'_>,
	value: *mut arsw::ffi::Sqlite3Value,
) -> PyResult<Py<PyAny>> {
	let kind = unsafe { arsw::ffi::sqlite3_value_type(value) };
	let value = match kind {
		SQLITE_INTEGER => {
			unsafe { arsw::ffi::sqlite3_value_int64(value) }.into_pyobject(py)?.unbind().into_any()
		}
		SQLITE_FLOAT => {
			unsafe { arsw::ffi::sqlite3_value_double(value) }.into_pyobject(py)?.unbind().into_any()
		}
		SQLITE_TEXT => {
			let text = unsafe { arsw::ffi::sqlite3_value_text(value) };
			let bytes = usize::try_from(unsafe { arsw::ffi::sqlite3_value_bytes(value) }).unwrap_or(0);
			let text = if text.is_null() {
				String::new()
			} else {
				let data = unsafe { std::slice::from_raw_parts(text, bytes) };
				String::from_utf8_lossy(data).into_owned()
			};
			text.into_pyobject(py)?.unbind().into_any()
		}
		SQLITE_BLOB => {
			let blob = unsafe { arsw::ffi::sqlite3_value_blob(value) };
			let bytes = usize::try_from(unsafe { arsw::ffi::sqlite3_value_bytes(value) }).unwrap_or(0);
			let data = if blob.is_null() {
				&[][..]
			} else {
				unsafe { std::slice::from_raw_parts(blob.cast(), bytes) }
			};
			PyBytes::new(py, data).unbind().into_any()
		}
		_ => py.None(),
	};

	Ok(value)
}

pub fn sqlite_values_to_python(
	py: Python<'_>,
	argc: c_int,
	argv: *mut *mut arsw::ffi::Sqlite3Value,
) -> PyResult<Vec<Py<PyAny>>> {
	if argc <= 0 || argv.is_null() {
		return Ok(Vec::new());
	}

	let argc = usize::try_from(argc).unwrap_or(0);
	let args = unsafe { std::slice::from_raw_parts(argv, argc) };
	args.iter().map(|value| sqlite_value_to_python(py, *value)).collect()
}

pub fn extract_bytes(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
	let builtins = PyModule::import(py, "builtins")?;
	let bytes_type = builtins.getattr("bytes")?;
	let bytes_obj = bytes_type.call1((value,))?;
	Ok(bytes_obj.cast::<PyBytes>()?.as_bytes().to_vec())
}

pub fn extract_changeset_input(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
	if value.is_callable() {
		let mut all = Vec::new();
		loop {
			let chunk = value.call1((8192,))?;
			let chunk = extract_bytes(py, &chunk)?;
			if chunk.is_empty() {
				break;
			}
			all.extend_from_slice(&chunk);
		}
		return Ok(all);
	}

	extract_bytes(py, value)
}

pub fn sqlite_result_from_python(
	context: *mut arsw::ffi::Sqlite3Context,
	_py: Python<'_>,
	value: &Bound<'_, PyAny>,
) -> PyResult<()> {
	if value.is_none() {
		unsafe {
			arsw::ffi::sqlite3_result_null(context);
		}
		return Ok(());
	}

	if value.is_instance_of::<PyBool>() {
		let value = value.extract::<bool>()?;
		unsafe {
			arsw::ffi::sqlite3_result_int64(context, i64::from(value));
		}
		return Ok(());
	}

	if let Ok(value) = value.extract::<i64>() {
		unsafe {
			arsw::ffi::sqlite3_result_int64(context, value);
		}
		return Ok(());
	}

	if let Ok(value) = value.extract::<f64>() {
		unsafe {
			arsw::ffi::sqlite3_result_double(context, value);
		}
		return Ok(());
	}

	if let Ok(value) = value.extract::<String>() {
		unsafe {
			arsw::ffi::sqlite3_result_text64(
				context,
				value.as_ptr().cast::<c_char>(),
				u64::try_from(value.len()).expect("string length fits in u64"),
				Some(sqlite_transient()),
				SQLITE_UTF8,
			);
		}
		return Ok(());
	}

	if let Ok(value) = value.cast::<PyBytes>() {
		let bytes = value.as_bytes();
		unsafe {
			arsw::ffi::sqlite3_result_blob64(
				context,
				bytes.as_ptr().cast(),
				u64::try_from(bytes.len()).expect("bytes length fits in u64"),
				Some(sqlite_transient()),
			);
		}
		return Ok(());
	}

	if let Ok(value) = value.cast::<PyByteArray>() {
		let bytes = unsafe { value.as_bytes() };
		unsafe {
			arsw::ffi::sqlite3_result_blob64(
				context,
				bytes.as_ptr().cast(),
				u64::try_from(bytes.len()).expect("bytes length fits in u64"),
				Some(sqlite_transient()),
			);
		}
		return Ok(());
	}

	let representation = value.str()?.to_str()?.to_string();
	unsafe {
		arsw::ffi::sqlite3_result_text64(
			context,
			representation.as_ptr().cast::<c_char>(),
			u64::try_from(representation.len()).expect("string length fits in u64"),
			Some(sqlite_transient()),
			SQLITE_UTF8,
		);
	}

	Ok(())
}

pub fn invoke_python_callback(
	py: Python<'_>,
	callable: &Py<PyAny>,
	leading: Option<Py<PyAny>>,
	args: Vec<Py<PyAny>>,
) -> PyResult<Py<PyAny>> {
	let mut items = Vec::with_capacity(args.len() + usize::from(leading.is_some()));
	if let Some(leading) = leading {
		items.push(leading);
	}
	items.extend(args);
	let args = PyTuple::new(py, items)?;
	Ok(callable.bind(py).call1(args)?.unbind())
}

pub fn build_aggregate_invocation(
	factory: &Bound<'_, PyAny>,
	is_window: bool,
) -> PyResult<AggregateInvocation> {
	let created = factory.call0()?;

	if let Ok(tuple) = created.cast::<PyTuple>() {
		let expected = if is_window { 5 } else { 3 };
		if tuple.len() != expected {
			return Err(pyo3::exceptions::PyTypeError::new_err(format!(
				"Factory tuple must have {expected} items"
			)));
		}

		let context = Some(tuple.get_item(0)?.unbind());
		let step_item = tuple.get_item(1)?;
		if !step_item.is_callable() {
			return Err(pyo3::exceptions::PyTypeError::new_err("step function must be callable"));
		}
		let final_item = tuple.get_item(2)?;
		if !final_item.is_callable() {
			return Err(pyo3::exceptions::PyTypeError::new_err("final function must be callable"));
		}
		let step = step_item.unbind();
		let finalizer = final_item.unbind();
		let value = if is_window { Some(tuple.get_item(3)?.unbind()) } else { None };
		let inverse = if is_window { Some(tuple.get_item(4)?.unbind()) } else { None };

		return Ok(AggregateInvocation {
			shape: CallbackShape::Tuple,
			context,
			step,
			finalizer,
			value,
			inverse,
			step_error: None,
		});
	}

	let step_item = created.getattr("step")?;
	if !step_item.is_callable() {
		return Err(pyo3::exceptions::PyTypeError::new_err("step function must be callable"));
	}
	let final_item = created.getattr("final")?;
	if !final_item.is_callable() {
		return Err(pyo3::exceptions::PyTypeError::new_err("final function must be callable"));
	}
	let step = step_item.unbind();
	let finalizer = final_item.unbind();
	let value = if is_window { Some(created.getattr("value")?.unbind()) } else { None };
	let inverse = if is_window { Some(created.getattr("inverse")?.unbind()) } else { None };

	Ok(AggregateInvocation {
		shape: CallbackShape::Object,
		context: None,
		step,
		finalizer,
		value,
		inverse,
		step_error: None,
	})
}

pub(crate) fn sqlite_transient() -> unsafe extern "C" fn(*mut std::ffi::c_void) {
	unsafe { std::mem::transmute::<isize, unsafe extern "C" fn(*mut std::ffi::c_void)>(-1_isize) }
}

fn db_error_message(db: *mut arsw::ffi::Sqlite3) -> String {
	if db.is_null() {
		return "SQLite error".to_string();
	}

	let message_ptr = unsafe { arsw::ffi::sqlite3_errmsg(db) };
	if message_ptr.is_null() {
		return "SQLite error".to_string();
	}

	unsafe { CStr::from_ptr(message_ptr).to_string_lossy().into_owned() }
}

pub(crate) fn missing_collation_name_from_error(db: *mut arsw::ffi::Sqlite3) -> Option<String> {
	let message = db_error_message(db);
	message
		.strip_prefix("no such collation sequence: ")
		.map(str::trim)
		.filter(|name| !name.is_empty())
		.map(ToOwned::to_owned)
}

pub(crate) fn sqlite_error_for_code(
	py: Python<'_>,
	db: *mut arsw::ffi::Sqlite3,
	code: c_int,
) -> PyErr {
	let message = db_error_message(db);
	match exception_instance_for_code(py, code, Some(&message)) {
		Ok(exception) => {
			let bound = exception.bind(py);
			if !db.is_null() {
				let offset = unsafe { arsw::ffi::sqlite3_error_offset(db) };
				if offset >= 0 {
					let _ = bound.setattr("error_offset", offset);
				}
			}
			PyErr::from_value(bound.clone())
		}
		Err(err) => err,
	}
}

pub(crate) fn sqlite_error_for_global_code(py: Python<'_>, code: c_int, message: &str) -> PyErr {
	match exception_instance_for_code(py, code, Some(message)) {
		Ok(exception) => PyErr::from_value(exception.into_bound(py)),
		Err(_) => Error::new_err(message.to_string()),
	}
}

pub(crate) fn sqlite_effective_error_code(db: *mut arsw::ffi::Sqlite3, rc: c_int) -> c_int {
	if db.is_null() {
		return rc;
	}

	let extended = unsafe { arsw::ffi::sqlite3_extended_errcode(db) };
	if extended != 0 { extended } else { rc }
}

pub(crate) fn sqlite_execute_no_rows(db: *mut arsw::ffi::Sqlite3, sql: &str) {
	let Ok(sql) = CString::new(sql) else {
		return;
	};
	let mut stmt = null_mut();
	let mut tail = null();
	let rc =
		unsafe { arsw::ffi::sqlite3_prepare_v3(db, sql.as_ptr(), -1, 0, &raw mut stmt, &raw mut tail) };
	let _ = tail;
	if rc != SQLITE_OK || stmt.is_null() {
		if !stmt.is_null() {
			unsafe {
				arsw::ffi::sqlite3_finalize(stmt);
			}
		}
		return;
	}
	let _ = unsafe { arsw::ffi::sqlite3_step(stmt) };
	unsafe {
		arsw::ffi::sqlite3_finalize(stmt);
	}
}

pub(crate) fn connection_closed_error() -> PyErr {
	ConnectionClosedError::new_err("Connection has been closed")
}

pub(crate) fn cursor_closed_error() -> PyErr {
	CursorClosedError::new_err("Cursor has been closed")
}

pub(crate) fn mark_closed_connection_attributes(
	py: Python<'_>,
	connection: &Bound<'_, Connection>,
) {
	let Ok(builtins) = PyModule::import(py, "builtins") else {
		return;
	};
	let Ok(dir_func) = builtins.getattr("dir") else {
		return;
	};
	let Ok(names) = dir_func.call1((connection,)) else {
		return;
	};
	let Ok(names) = names.cast::<PyList>() else {
		return;
	};
	for name in names.iter() {
		let Ok(name) = name.extract::<String>() else {
			continue;
		};
		if name.starts_with("__")
			|| name == "close"
			|| name == "aclose"
			|| name == "as_async"
			|| name == "readonly"
		{
			continue;
		}
		let _ = connection.setattr(&name, py.None());
	}
}

pub(crate) fn incomplete_execution_error() -> PyErr {
	IncompleteExecutionError::new_err("There are still remaining sql statements to execute")
}

pub(crate) fn incomplete_executemany_error() -> PyErr {
	IncompleteExecutionError::new_err("Previous executemany were not fully consumed")
}

pub(crate) fn execution_complete_error() -> PyErr {
	ExecutionCompleteError::new_err(
		"Can't get description for statements that have completed execution",
	)
}

pub(crate) unsafe extern "C" fn destroy_function_data(value: *mut c_void) {
	if !value.is_null() {
		unsafe {
			drop(Box::from_raw(value.cast::<FunctionData>()));
		}
	}
}

pub(crate) unsafe extern "C" fn destroy_collation_data(value: *mut c_void) {
	if !value.is_null() {
		unsafe {
			drop(Box::from_raw(value.cast::<CollationData>()));
		}
	}
}

fn handle_python_callback_error(
	context: *mut arsw::ffi::Sqlite3Context,
	py: Python<'_>,
	err: PyErr,
) {
	set_callback_error(py, &err);
	sqlite_callback_error(context, &err.to_string());
}

pub(crate) unsafe extern "C" fn scalar_function_callback(
	context: *mut arsw::ffi::Sqlite3Context,
	argc: c_int,
	argv: *mut *mut arsw::ffi::Sqlite3Value,
) {
	let Some(()) = Python::try_attach(|py| {
		let data = unsafe { arsw::ffi::sqlite3_user_data(context).cast::<FunctionData>() };
		if data.is_null() {
			sqlite_callback_error(context, "missing scalar callback state");
			return;
		}

		if !matches!(unsafe { &(*data).flavor }, FunctionFlavor::Scalar) {
			sqlite_callback_error(context, "callback flavor mismatch");
			return;
		}

		let args = match sqlite_values_to_python(py, argc, argv) {
			Ok(args) => args,
			Err(err) => {
				handle_python_callback_error(context, py, err);
				return;
			}
		};

		let result = invoke_python_callback(py, unsafe { &(*data).callable }, None, args);
		match result {
			Ok(result) => {
				if let Err(err) = sqlite_result_from_python(context, py, result.bind(py)) {
					handle_python_callback_error(context, py, err);
				}
			}
			Err(err) => handle_python_callback_error(context, py, err),
		}
	}) else {
		sqlite_callback_error(context, "unable to attach to Python runtime");
		return;
	};
}

pub(crate) unsafe extern "C" fn aggregate_step_callback(
	context: *mut arsw::ffi::Sqlite3Context,
	argc: c_int,
	argv: *mut *mut arsw::ffi::Sqlite3Value,
) {
	let Some(()) = Python::try_attach(|py| {
		let data = unsafe { arsw::ffi::sqlite3_user_data(context).cast::<FunctionData>() };
		if data.is_null() {
			sqlite_callback_error(context, "missing aggregate callback state");
			return;
		}

		let is_window = matches!(unsafe { &(*data).flavor }, FunctionFlavor::Window);
		if !matches!(unsafe { &(*data).flavor }, FunctionFlavor::Aggregate | FunctionFlavor::Window) {
			sqlite_callback_error(context, "callback flavor mismatch");
			return;
		}

		let slot = unsafe {
			arsw::ffi::sqlite3_aggregate_context(
				context,
				c_int::try_from(size_of::<*mut AggregateInvocation>()).expect("pointer size fits c_int"),
			)
			.cast::<*mut AggregateInvocation>()
		};
		if slot.is_null() {
			sqlite_callback_error(context, "unable to allocate aggregate context");
			return;
		}

		if unsafe { (*slot).is_null() } {
			match build_aggregate_invocation(unsafe { (*data).callable.bind(py) }, is_window) {
				Ok(invocation) => unsafe {
					*slot = Box::into_raw(Box::new(invocation));
				},
				Err(err) => {
					handle_python_callback_error(context, py, err);
					return;
				}
			}
		}

		let invocation = unsafe { &mut **slot };
		let args = match sqlite_values_to_python(py, argc, argv) {
			Ok(args) => args,
			Err(err) => {
				handle_python_callback_error(context, py, err);
				return;
			}
		};

		let leading = match invocation.shape {
			CallbackShape::Object => None,
			CallbackShape::Tuple => invocation.context.as_ref().map(|value| value.clone_ref(py)),
		};

		if let Err(err) = invoke_python_callback(py, &invocation.step, leading, args) {
			invocation.step_error = Some(err.clone_ref(py));
			handle_python_callback_error(context, py, err);
		}
	}) else {
		sqlite_callback_error(context, "unable to attach to Python runtime");
		return;
	};
}

pub(crate) unsafe extern "C" fn aggregate_final_callback(context: *mut arsw::ffi::Sqlite3Context) {
	let Some(()) = Python::try_attach(|py| {
		let data = unsafe { arsw::ffi::sqlite3_user_data(context).cast::<FunctionData>() };
		if data.is_null() {
			sqlite_callback_error(context, "missing aggregate callback state");
			return;
		}

		let is_window = matches!(unsafe { &(*data).flavor }, FunctionFlavor::Window);
		if !matches!(unsafe { &(*data).flavor }, FunctionFlavor::Aggregate | FunctionFlavor::Window) {
			sqlite_callback_error(context, "callback flavor mismatch");
			return;
		}

		let slot = unsafe {
			arsw::ffi::sqlite3_aggregate_context(
				context,
				c_int::try_from(size_of::<*mut AggregateInvocation>()).expect("pointer size fits c_int"),
			)
			.cast::<*mut AggregateInvocation>()
		};
		if slot.is_null() {
			sqlite_callback_error(context, "unable to allocate aggregate context");
			return;
		}

		if unsafe { (*slot).is_null() } {
			match build_aggregate_invocation(unsafe { (*data).callable.bind(py) }, is_window) {
				Ok(invocation) => unsafe {
					*slot = Box::into_raw(Box::new(invocation));
				},
				Err(err) => {
					handle_python_callback_error(context, py, err);
					return;
				}
			}
		}

		let invocation_ptr = unsafe { *slot };
		if invocation_ptr.is_null() {
			unsafe {
				arsw::ffi::sqlite3_result_null(context);
			}
			return;
		}

		let invocation = unsafe { Box::from_raw(invocation_ptr) };
		unsafe {
			*slot = null_mut();
		}

		if let Some(err) = invocation.step_error.as_ref() {
			handle_python_callback_error(context, py, err.clone_ref(py));
			return;
		}

		let leading = match invocation.shape {
			CallbackShape::Object => None,
			CallbackShape::Tuple => invocation.context.as_ref().map(|value| value.clone_ref(py)),
		};

		match invoke_python_callback(py, &invocation.finalizer, leading, Vec::new()) {
			Ok(result) => {
				if let Err(err) = sqlite_result_from_python(context, py, result.bind(py)) {
					handle_python_callback_error(context, py, err);
				}
			}
			Err(err) => handle_python_callback_error(context, py, err),
		}
	}) else {
		sqlite_callback_error(context, "unable to attach to Python runtime");
		return;
	};
}

pub(crate) unsafe extern "C" fn window_value_callback(context: *mut arsw::ffi::Sqlite3Context) {
	let Some(()) = Python::try_attach(|py| {
		let slot = unsafe {
			arsw::ffi::sqlite3_aggregate_context(context, 0).cast::<*mut AggregateInvocation>()
		};
		if slot.is_null() || unsafe { (*slot).is_null() } {
			unsafe {
				arsw::ffi::sqlite3_result_null(context);
			}
			return;
		}

		let invocation = unsafe { &mut **slot };
		let Some(value_callback) = invocation.value.as_ref() else {
			unsafe {
				arsw::ffi::sqlite3_result_null(context);
			}
			return;
		};

		let leading = match invocation.shape {
			CallbackShape::Object => None,
			CallbackShape::Tuple => invocation.context.as_ref().map(|value| value.clone_ref(py)),
		};

		match invoke_python_callback(py, value_callback, leading, Vec::new()) {
			Ok(result) => {
				if let Err(err) = sqlite_result_from_python(context, py, result.bind(py)) {
					handle_python_callback_error(context, py, err);
				}
			}
			Err(err) => handle_python_callback_error(context, py, err),
		}
	}) else {
		sqlite_callback_error(context, "unable to attach to Python runtime");
		return;
	};
}

pub(crate) unsafe extern "C" fn window_inverse_callback(
	context: *mut arsw::ffi::Sqlite3Context,
	argc: c_int,
	argv: *mut *mut arsw::ffi::Sqlite3Value,
) {
	let Some(()) = Python::try_attach(|py| {
		let slot = unsafe {
			arsw::ffi::sqlite3_aggregate_context(context, 0).cast::<*mut AggregateInvocation>()
		};
		if slot.is_null() || unsafe { (*slot).is_null() } {
			return;
		}

		let invocation = unsafe { &mut **slot };
		let Some(inverse) = invocation.inverse.as_ref() else {
			return;
		};

		let args = match sqlite_values_to_python(py, argc, argv) {
			Ok(args) => args,
			Err(err) => {
				err.print(py);
				return;
			}
		};

		let leading = match invocation.shape {
			CallbackShape::Object => None,
			CallbackShape::Tuple => invocation.context.as_ref().map(|value| value.clone_ref(py)),
		};

		if let Err(err) = invoke_python_callback(py, inverse, leading, args) {
			err.print(py);
		}
	}) else {
		return;
	};
}

pub(crate) unsafe extern "C" fn collation_compare_callback(
	userdata: *mut c_void,
	left_len: c_int,
	left: *const c_void,
	right_len: c_int,
	right: *const c_void,
) -> c_int {
	if userdata.is_null() {
		return 0;
	}

	let Some(value) = Python::try_attach(|py| {
		let callback = unsafe { &*userdata.cast::<CollationData>() };

		let left_len = usize::try_from(left_len).unwrap_or(0);
		let right_len = usize::try_from(right_len).unwrap_or(0);
		let left = if left.is_null() {
			"".to_string()
		} else {
			let bytes = unsafe { std::slice::from_raw_parts(left.cast::<u8>(), left_len) };
			String::from_utf8_lossy(bytes).into_owned()
		};
		let right = if right.is_null() {
			"".to_string()
		} else {
			let bytes = unsafe { std::slice::from_raw_parts(right.cast::<u8>(), right_len) };
			String::from_utf8_lossy(bytes).into_owned()
		};

		match callback.callback.bind(py).call1((left, right)) {
			Ok(result) => match parse_index_i32(py, &result) {
				Ok(value) => value,
				Err(err) => {
					set_callback_error(py, &err);
					0
				}
			},
			Err(err) => {
				set_callback_error(py, &err);
				0
			}
		}
	}) else {
		return 0;
	};

	value
}

pub(crate) fn session_table_filters() -> &'static Mutex<HashMap<usize, Py<PyAny>>> {
	SESSION_TABLE_FILTERS.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(crate) unsafe extern "C" fn session_table_filter_callback(
	userdata: *mut c_void,
	table: *const c_char,
) -> c_int {
	let Some(result) = Python::try_attach(|py| {
		let key = userdata as usize;
		let callback = {
			let guard = session_table_filters().lock();
			let Ok(guard) = guard else {
				return 0;
			};
			guard.get(&key).map(|value| value.clone_ref(py))
		};

		let Some(callback) = callback else {
			return 0;
		};

		let table_name = if table.is_null() {
			String::new()
		} else {
			unsafe { CStr::from_ptr(table).to_string_lossy().into_owned() }
		};

		match callback.bind(py).call1((table_name,)) {
			Ok(value) => c_int::from(value.is_truthy().unwrap_or(false)),
			Err(err) => {
				err.print(py);
				0
			}
		}
	}) else {
		return 0;
	};

	result
}

pub(crate) unsafe extern "C" fn session_stream_output_callback(
	userdata: *mut c_void,
	payload: *const c_void,
	length: c_int,
) -> c_int {
	if userdata.is_null() {
		return SQLITE_OK;
	}

	let Some(result) = Python::try_attach(|py| {
		let data = unsafe { &*userdata.cast::<StreamOutputData>() };
		let length = usize::try_from(length).unwrap_or(0);
		let bytes = if payload.is_null() || length == 0 {
			PyBytes::new(py, &[])
		} else {
			let data = unsafe { std::slice::from_raw_parts(payload.cast::<u8>(), length) };
			PyBytes::new(py, data)
		};

		match data.callback.bind(py).call1((bytes,)) {
			Ok(_) => SQLITE_OK,
			Err(err) => {
				err.print(py);
				sqlite_constant_value("SQLITE_ERROR").unwrap_or(1)
			}
		}
	}) else {
		return sqlite_constant_value("SQLITE_ERROR").unwrap_or(1);
	};

	result
}

fn apply_connection_hooks_once(py: Python<'_>, connection: &Py<Connection>) -> PyResult<()> {
	let hooks = {
		let conn = connection.borrow(py);
		if conn
			.connection_hooks_applied
			.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
			.is_err()
		{
			return Ok(());
		}
		conn.connection_hooks.iter().map(|hook| hook.clone_ref(py)).collect::<Vec<_>>()
	};

	for hook in hooks {
		if let Err(err) = hook.bind(py).call1((connection.clone_ref(py),)) {
			connection.borrow(py).connection_hooks_applied.store(false, Ordering::Release);
			return Err(err);
		}
	}

	Ok(())
}

pub fn make_cursor_for_connection(
	py: Python<'_>,
	connection: Py<Connection>,
) -> PyResult<Py<PyAny>> {
	apply_connection_hooks_once(py, &connection)?;
	connection.borrow(py).make_cursor_object(py, connection.clone_ref(py))
}
