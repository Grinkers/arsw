use core::ffi::{c_char, c_int, c_uchar, c_void};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::ffi::{CStr, CString};
use std::io::{Read as _, Seek as _, SeekFrom};
use std::mem::size_of;
use std::path::Path;
use std::ptr::{null, null_mut};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};
use std::thread::ThreadId;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use pyo3::prelude::*;
use pyo3::types::{
	PyAny, PyBool, PyByteArray, PyBytes, PyDict, PyFloat, PyInt, PyList, PySequence, PySet, PyString,
	PyTuple, PyType,
};

#[macro_use]
mod faultinject;
mod backup;
mod blob;
mod connection;
mod cursor;
mod errors;
mod exceptions;
mod fts;
mod jsonb;
mod session;
mod traceback;
mod util;
mod vfs;
mod vtable;

pub(crate) use backup::Backup;
pub(crate) use blob::{Blob, ZeroBlob};
pub(crate) use connection::Connection;
pub(crate) use cursor::{Cursor, ExecTraceCursorProxy, RowTraceCursorProxy};
pub(crate) use errors::*;
pub(crate) use exceptions::*;
pub(crate) use faultinject::*;
pub(crate) use fts::FTS5Tokenizer;
pub(crate) use jsonb::*;
pub(crate) use session::{
	Changeset, ChangesetBuilder, Rebaser, Session, TableChange, session_config,
};
pub(crate) use util::*;
pub(crate) use vfs::{VFS, VFSFile, set_default_vfs, vfs_details, vfs_names};
pub(crate) use vtable::*;

const APSW_VERSION_HEADER: &str = include_str!("../../../src/apswversion.h");
const APSW_STUBS: &str = include_str!("../../../apsw/__init__.pyi");

#[pyclass(name = "no_change", module = "apsw")]
struct NoChange;

#[pymethods]
impl NoChange {
	#[expect(clippy::unused_self, reason = "PyO3 dunder methods take &self")]
	#[expect(clippy::missing_const_for_fn, reason = "PyO3 dunder methods are not const")]
	fn __repr__(&self) -> &'static str {
		"<apsw.no_change>"
	}

	#[expect(clippy::unused_self, reason = "PyO3 dunder methods take &self")]
	#[expect(clippy::missing_const_for_fn, reason = "PyO3 dunder methods are not const")]
	fn __str__(&self) -> &'static str {
		"<apsw.no_change>"
	}
}

#[pyclass(module = "apsw")]
struct ConvertCursorProxy {
	connection: Py<Connection>,
	bindings_count: usize,
	bindings_names: Py<PyTuple>,
	description: Py<PyAny>,
}

#[pymethods]
impl ConvertCursorProxy {
	#[getter(connection)]
	fn connection_attr(&self, py: Python<'_>) -> Py<Connection> {
		self.connection.clone_ref(py)
	}

	fn get_connection(&self, py: Python<'_>) -> Py<Connection> {
		self.connection.clone_ref(py)
	}

	fn getconnection(&self, py: Python<'_>) -> Py<Connection> {
		self.connection.clone_ref(py)
	}

	#[getter(bindings_count)]
	fn bindings_count_attr(&self) -> usize {
		self.bindings_count
	}

	#[getter(bindings_names)]
	fn bindings_names_attr(&self, py: Python<'_>) -> Py<PyTuple> {
		self.bindings_names.clone_ref(py)
	}

	#[getter(description)]
	fn description_attr(&self, py: Python<'_>) -> Py<PyAny> {
		self.description.clone_ref(py)
	}

	fn get_description(&self, py: Python<'_>) -> Py<PyAny> {
		self.description.clone_ref(py)
	}

	fn getdescription(&self, py: Python<'_>) -> Py<PyAny> {
		self.description.clone_ref(py)
	}
}

const SQLITE_OK: c_int = 0;
const SQLITE_ROW: c_int = 100;
const SQLITE_DONE: c_int = 101;
const SQLITE_INTEGER: c_int = 1;
const SQLITE_FLOAT: c_int = 2;
const SQLITE_TEXT: c_int = 3;
const SQLITE_BLOB: c_int = 4;
const SQLITE_UTF8: c_uchar = 1;
const SQLITE_BUSY: c_int = 5;
const SQLITE_LOCKED: c_int = 6;
const SQLITE_UTF8_ENCODING: c_int = 1;

const DEFAULT_OPEN_FLAGS: c_int = 0x0000_0002 | 0x0000_0004;
const FAULT_INJECT_PROCEED: i64 = 0x01FA_CADE;
const FAULT_INJECT_PROCEED_RETURN18: i64 = 0x02FA_CADE;

enum CallbackShape {
	Object,
	Tuple,
}

enum FunctionFlavor {
	Scalar,
	Aggregate,
	Window,
}

struct FunctionData {
	callable: Py<PyAny>,
	flavor: FunctionFlavor,
}

struct AggregateInvocation {
	shape: CallbackShape,
	context: Option<Py<PyAny>>,
	step: Py<PyAny>,
	finalizer: Py<PyAny>,
	value: Option<Py<PyAny>>,
	inverse: Option<Py<PyAny>>,
	step_error: Option<PyErr>,
}

struct CollationData {
	callback: Py<PyAny>,
}

struct StreamOutputData {
	callback: Py<PyAny>,
}

static SESSION_TABLE_FILTERS: OnceLock<Mutex<HashMap<usize, Py<PyAny>>>> = OnceLock::new();
static ALLOW_MISSING_DICT_BINDINGS: AtomicBool = AtomicBool::new(false);
static CUSTOM_VFS_NAMES: OnceLock<Mutex<HashMap<String, i32>>> = OnceLock::new();
static SQLITE_LOG_HANDLER: OnceLock<Mutex<Option<Py<PyAny>>>> = OnceLock::new();
static SQLITE_IS_INITIALIZED: AtomicBool = AtomicBool::new(false);

thread_local! {
	static CALLBACK_ERROR: RefCell<Option<PyErr>> = const { RefCell::new(None) };
}

fn custom_vfs_names() -> &'static Mutex<HashMap<String, i32>> {
	CUSTOM_VFS_NAMES.get_or_init(|| Mutex::new(HashMap::new()))
}

fn sqlite_log_handler() -> &'static Mutex<Option<Py<PyAny>>> {
	SQLITE_LOG_HANDLER.get_or_init(|| Mutex::new(None))
}

fn set_callback_error(py: Python<'_>, err: &PyErr) {
	CALLBACK_ERROR.with(|slot| {
		*slot.borrow_mut() = Some(err.clone_ref(py));
	});
}

fn take_callback_error() -> Option<PyErr> {
	CALLBACK_ERROR.with(|slot| slot.borrow_mut().take())
}

enum BindingsSource {
	None,
	Null,
	Positional(Vec<Py<PyAny>>),
	Named(Py<PyDict>),
}

const RESULT_CODES: [(&str, i32); 31] = [
	("SQLITE_OK", 0),
	("SQLITE_ERROR", 1),
	("SQLITE_INTERNAL", 2),
	("SQLITE_PERM", 3),
	("SQLITE_ABORT", 4),
	("SQLITE_BUSY", 5),
	("SQLITE_LOCKED", 6),
	("SQLITE_NOMEM", 7),
	("SQLITE_READONLY", 8),
	("SQLITE_INTERRUPT", 9),
	("SQLITE_IOERR", 10),
	("SQLITE_CORRUPT", 11),
	("SQLITE_NOTFOUND", 12),
	("SQLITE_FULL", 13),
	("SQLITE_CANTOPEN", 14),
	("SQLITE_PROTOCOL", 15),
	("SQLITE_EMPTY", 16),
	("SQLITE_SCHEMA", 17),
	("SQLITE_TOOBIG", 18),
	("SQLITE_CONSTRAINT", 19),
	("SQLITE_MISMATCH", 20),
	("SQLITE_MISUSE", 21),
	("SQLITE_NOLFS", 22),
	("SQLITE_AUTH", 23),
	("SQLITE_FORMAT", 24),
	("SQLITE_RANGE", 25),
	("SQLITE_NOTADB", 26),
	("SQLITE_NOTICE", 27),
	("SQLITE_WARNING", 28),
	("SQLITE_ROW", 100),
	("SQLITE_DONE", 101),
];

fn add_mapping_for_prefix(
	py: Python<'_>,
	m: &Bound<'_, PyModule>,
	mapping_name: &str,
	constants: &[(&'static str, i32)],
	prefix: &str,
) -> PyResult<()> {
	let mapping = PyDict::new(py);

	for (name, value) in constants {
		if name.starts_with(prefix) {
			mapping.set_item(*name, *value)?;
			mapping.set_item(*value, *name)?;
		}
	}

	m.add(mapping_name, mapping)?;
	Ok(())
}

fn add_mapping_for_names(
	py: Python<'_>,
	m: &Bound<'_, PyModule>,
	mapping_name: &str,
	constants: &[(&'static str, i32)],
	names: &[&str],
) -> PyResult<()> {
	let mapping = PyDict::new(py);

	for wanted in names {
		if let Some((_, value)) = constants.iter().find(|(name, _)| name == wanted) {
			mapping.set_item(*wanted, *value)?;
			mapping.set_item(*value, *wanted)?;
		}
	}

	m.add(mapping_name, mapping)?;
	Ok(())
}

fn add_mapping_result_codes(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
	let mapping_result_codes = PyDict::new(py);

	for (name, value) in RESULT_CODES {
		m.add(name, value)?;
		mapping_result_codes.set_item(name, value)?;
		mapping_result_codes.set_item(value, name)?;
	}

	m.add("mapping_result_codes", mapping_result_codes)?;
	Ok(())
}

fn add_mapping_extended_result_codes(
	py: Python<'_>,
	m: &Bound<'_, PyModule>,
	constants: &[(&'static str, i32)],
) -> PyResult<()> {
	let mapping_extended_result_codes = PyDict::new(py);

	for (name, value) in constants {
		if is_extended_result_code(name, *value) {
			mapping_extended_result_codes.set_item(*name, *value)?;
			mapping_extended_result_codes.set_item(*value, *name)?;
		}
	}

	m.add("mapping_extended_result_codes", mapping_extended_result_codes)?;
	Ok(())
}

fn add_prefix_mappings(
	py: Python<'_>,
	m: &Bound<'_, PyModule>,
	constants: &[(&'static str, i32)],
) -> PyResult<()> {
	add_mapping_for_prefix(py, m, "mapping_access", constants, "SQLITE_ACCESS_")?;
	add_mapping_for_prefix(
		py,
		m,
		"mapping_bestindex_constraints",
		constants,
		"SQLITE_INDEX_CONSTRAINT_",
	)?;
	add_mapping_for_prefix(py, m, "mapping_config", constants, "SQLITE_CONFIG_")?;
	add_mapping_for_prefix(py, m, "mapping_db_config", constants, "SQLITE_DBCONFIG_")?;
	add_mapping_for_prefix(py, m, "mapping_db_status", constants, "SQLITE_DBSTATUS_")?;
	add_mapping_for_prefix(py, m, "mapping_device_characteristics", constants, "SQLITE_IOCAP_")?;
	add_mapping_for_prefix(py, m, "mapping_file_control", constants, "SQLITE_FCNTL_")?;
	add_mapping_for_prefix(py, m, "mapping_fts5_token_flags", constants, "FTS5_TOKEN_")?;
	add_mapping_for_prefix(py, m, "mapping_fts5_tokenize_reason", constants, "FTS5_TOKENIZE_")?;
	add_mapping_for_prefix(py, m, "mapping_limits", constants, "SQLITE_LIMIT_")?;
	add_mapping_for_prefix(py, m, "mapping_open_flags", constants, "SQLITE_OPEN_")?;
	add_mapping_for_prefix(py, m, "mapping_prepare_flags", constants, "SQLITE_PREPARE_")?;
	add_mapping_for_prefix(py, m, "mapping_setlk_timeout_flags", constants, "SQLITE_SETLK_")?;
	add_mapping_for_prefix(py, m, "mapping_statement_status", constants, "SQLITE_STMTSTATUS_")?;
	add_mapping_for_prefix(py, m, "mapping_status", constants, "SQLITE_STATUS_")?;
	add_mapping_for_prefix(py, m, "mapping_trace_codes", constants, "SQLITE_TRACE_")?;
	add_mapping_for_prefix(py, m, "mapping_txn_state", constants, "SQLITE_TXN_")?;
	add_mapping_for_prefix(
		py,
		m,
		"mapping_virtual_table_configuration_options",
		constants,
		"SQLITE_VTAB_",
	)?;
	add_mapping_for_prefix(
		py,
		m,
		"mapping_virtual_table_scan_flags",
		constants,
		"SQLITE_INDEX_SCAN_",
	)?;
	add_mapping_for_prefix(py, m, "mapping_wal_checkpoint", constants, "SQLITE_CHECKPOINT_")?;
	add_mapping_for_prefix(py, m, "mapping_xshmlock_flags", constants, "SQLITE_SHM_")?;
	Ok(())
}

fn add_named_mappings(
	py: Python<'_>,
	m: &Bound<'_, PyModule>,
	constants: &[(&'static str, i32)],
) -> PyResult<()> {
	add_mapping_for_names(
		py,
		m,
		"mapping_authorizer_return_codes",
		constants,
		&["SQLITE_OK", "SQLITE_DENY", "SQLITE_IGNORE"],
	)?;
	add_mapping_for_names(
		py,
		m,
		"mapping_conflict_resolution_modes",
		constants,
		&["SQLITE_ABORT", "SQLITE_FAIL", "SQLITE_IGNORE", "SQLITE_REPLACE", "SQLITE_ROLLBACK"],
	)?;
	add_mapping_for_names(
		py,
		m,
		"mapping_function_flags",
		constants,
		&[
			"SQLITE_DETERMINISTIC",
			"SQLITE_DIRECTONLY",
			"SQLITE_INNOCUOUS",
			"SQLITE_RESULT_SUBTYPE",
			"SQLITE_SELFORDER1",
			"SQLITE_SUBTYPE",
		],
	)?;
	add_mapping_for_names(
		py,
		m,
		"mapping_locking_level",
		constants,
		&[
			"SQLITE_LOCK_EXCLUSIVE",
			"SQLITE_LOCK_NONE",
			"SQLITE_LOCK_PENDING",
			"SQLITE_LOCK_RESERVED",
			"SQLITE_LOCK_SHARED",
		],
	)?;
	add_mapping_for_names(
		py,
		m,
		"mapping_sync",
		constants,
		&["SQLITE_SYNC_DATAONLY", "SQLITE_SYNC_FULL", "SQLITE_SYNC_NORMAL"],
	)?;
	add_mapping_for_names(
		py,
		m,
		"mapping_authorizer_function",
		constants,
		&[
			"SQLITE_ALTER_TABLE",
			"SQLITE_ANALYZE",
			"SQLITE_ATTACH",
			"SQLITE_COPY",
			"SQLITE_CREATE_INDEX",
			"SQLITE_CREATE_TABLE",
			"SQLITE_CREATE_TEMP_INDEX",
			"SQLITE_CREATE_TEMP_TABLE",
			"SQLITE_CREATE_TEMP_TRIGGER",
			"SQLITE_CREATE_TEMP_VIEW",
			"SQLITE_CREATE_TRIGGER",
			"SQLITE_CREATE_VIEW",
			"SQLITE_CREATE_VTABLE",
			"SQLITE_DELETE",
			"SQLITE_DETACH",
			"SQLITE_DROP_INDEX",
			"SQLITE_DROP_TABLE",
			"SQLITE_DROP_TEMP_INDEX",
			"SQLITE_DROP_TEMP_TABLE",
			"SQLITE_DROP_TEMP_TRIGGER",
			"SQLITE_DROP_TEMP_VIEW",
			"SQLITE_DROP_TRIGGER",
			"SQLITE_DROP_VIEW",
			"SQLITE_DROP_VTABLE",
			"SQLITE_FUNCTION",
			"SQLITE_INSERT",
			"SQLITE_PRAGMA",
			"SQLITE_READ",
			"SQLITE_RECURSIVE",
			"SQLITE_REINDEX",
			"SQLITE_SAVEPOINT",
			"SQLITE_SELECT",
			"SQLITE_TRANSACTION",
			"SQLITE_UPDATE",
		],
	)?;
	Ok(())
}

fn result_code_primary(name: &str) -> Option<i32> {
	RESULT_CODES
		.iter()
		.find_map(|(result_name, value)| if *result_name == name { Some(*value) } else { None })
}

fn is_extended_result_code(name: &str, value: i32) -> bool {
	if value <= 0 {
		return false;
	}

	let Some(rest) = name.strip_prefix("SQLITE_") else {
		return false;
	};

	let Some((base, _)) = rest.split_once('_') else {
		return false;
	};

	let primary_name = format!("SQLITE_{base}");
	let Some(primary_value) = result_code_primary(&primary_name) else {
		return false;
	};

	value != primary_value
}

fn add_module_constants(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
	let sqlite_constants = sqlite_constants_from_stubs();

	for (name, value) in &sqlite_constants {
		m.add(*name, *value)?;
	}
	if !m.hasattr("SQLITE_SESSION_OBJCONFIG_SIZE")? {
		m.add("SQLITE_SESSION_OBJCONFIG_SIZE", 1)?;
	}
	if !m.hasattr("SQLITE_SESSION_CONFIG_STRMSIZE")? {
		m.add("SQLITE_SESSION_CONFIG_STRMSIZE", 1)?;
	}

	add_mapping_result_codes(py, m)?;
	add_mapping_extended_result_codes(py, m, &sqlite_constants)?;
	add_prefix_mappings(py, m, &sqlite_constants)?;
	add_named_mappings(py, m, &sqlite_constants)?;

	m.add("SQLITE_VERSION_NUMBER", arsw::sqlite_lib_version_number())?;
	m.add("SQLITE_SCM_BRANCH", "")?;
	m.add("SQLITE_SCM_TAGS", "")?;
	m.add("SQLITE_SCM_DATETIME", "")?;
	m.add("compile_options", PyTuple::new(py, arsw::sqlite_compile_options())?)?;
	m.add("using_amalgamation", cfg!(feature = "bundled-sqlite"))?;
	Ok(())
}

fn add_module_runtime_state(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
	let contextvars = PyModule::import(py, "contextvars")?;
	let context_var = contextvars.getattr("ContextVar")?;

	let kwargs = PyDict::new(py);
	kwargs.set_item("default", py.None())?;

	m.add("async_controller", context_var.call(("apsw.async_controller",), Some(&kwargs))?)?;
	m.add(
		"async_cursor_prefetch",
		context_var.call(("apsw.async_cursor_prefetch",), Some(&kwargs))?,
	)?;
	m.add("async_run_coro", py.None())?;
	m.add("c", py.None())?;
	m.add("keywords", PySet::new(py, ["SELECT", "FROM", "WHERE", "VALUES", "CAST"])?)?;
	m.add("_null_bindings", py.eval(pyo3::ffi::c_str!("object()"), None, None)?)?;
	m.add("connection_hooks", PyList::empty(py))?;
	m.add("no_change", Py::new(py, NoChange {})?)?;
	Ok(())
}

fn add_module_compat_aliases(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
	m.add("apswversion", m.getattr("apsw_version")?)?;
	m.add("sqlitelibversion", m.getattr("sqlite_lib_version")?)?;
	m.add("exceptionfor", m.getattr("exception_for")?)?;
	m.add("vfsnames", m.getattr("vfs_names")?)?;
	let shell = PyModule::import(py, "apsw.shell")?;
	m.add("main", shell.getattr("main")?)?;
	m.add("Shell", shell.getattr("Shell")?)?;
	Ok(())
}

#[pymodule]
fn apsw(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
	m.add_function(wrap_pyfunction!(apsw_version, m)?)?;
	m.add_function(wrap_pyfunction!(allow_missing_dict_bindings, m)?)?;
	m.add_function(wrap_pyfunction!(config, m)?)?;
	m.add_function(wrap_pyfunction!(connections, m)?)?;
	m.add_function(wrap_pyfunction!(exception_for, m)?)?;
	m.add_function(wrap_pyfunction!(format_sql_value, m)?)?;
	m.add_function(wrap_pyfunction!(hard_heap_limit, m)?)?;
	m.add_function(wrap_pyfunction!(initialize, m)?)?;
	m.add_function(wrap_pyfunction!(jsonb_decode, m)?)?;
	m.add_function(wrap_pyfunction!(jsonb_detect, m)?)?;
	m.add_function(wrap_pyfunction!(jsonb_encode, m)?)?;
	m.add_function(wrap_pyfunction!(carray, m)?)?;
	m.add_function(wrap_pyfunction!(keywords, m)?)?;
	m.add_function(wrap_pyfunction!(log, m)?)?;
	m.add_function(wrap_pyfunction!(memory_high_water, m)?)?;
	m.add_function(wrap_pyfunction!(memory_used, m)?)?;
	m.add_function(wrap_pyfunction!(pyobject, m)?)?;
	m.add_function(wrap_pyfunction!(randomness, m)?)?;
	m.add_function(wrap_pyfunction!(release_memory, m)?)?;
	m.add_function(wrap_pyfunction!(set_default_vfs, m)?)?;
	m.add_function(wrap_pyfunction!(shutdown, m)?)?;
	m.add_function(wrap_pyfunction!(soft_heap_limit, m)?)?;
	m.add_function(wrap_pyfunction!(sqlite_lib_version, m)?)?;
	m.add_function(wrap_pyfunction!(sqlite3_sourceid, m)?)?;
	m.add_function(wrap_pyfunction!(status, m)?)?;
	m.add_function(wrap_pyfunction!(complete, m)?)?;
	m.add_function(wrap_pyfunction!(strglob, m)?)?;
	m.add_function(wrap_pyfunction!(stricmp, m)?)?;
	m.add_function(wrap_pyfunction!(strlike, m)?)?;
	m.add_function(wrap_pyfunction!(session_config, m)?)?;
	m.add_function(wrap_pyfunction!(vfs_names, m)?)?;
	m.add_function(wrap_pyfunction!(vfs_details, m)?)?;
	m.add_function(wrap_pyfunction!(zeroblob, m)?)?;
	m.add_class::<Connection>()?;
	m.add_class::<Cursor>()?;
	m.add_class::<Backup>()?;
	m.add_class::<Blob>()?;
	m.add_class::<Session>()?;
	m.add_class::<Rebaser>()?;
	m.add_class::<ChangesetBuilder>()?;
	m.add_class::<Changeset>()?;
	m.add_class::<TableChange>()?;
	m.add_class::<FTS5Tokenizer>()?;
	m.add_class::<VFS>()?;
	m.add_class::<VFSFile>()?;
	m.add_class::<ExecTraceCursorProxy>()?;
	m.add_class::<RowTraceCursorProxy>()?;
	m.add_class::<ZeroBlob>()?;
	add_module_exceptions(py, m)?;
	add_module_constants(py, m)?;
	add_module_runtime_state(py, m)?;
	add_module_compat_aliases(py, m)?;
	Ok(())
}
