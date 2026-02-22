use core::ffi::{c_char, c_int, c_void};
use std::collections::HashMap;
use std::ptr;
use std::sync::Mutex;

use pyo3::prelude::*;
use pyo3::types::PyDict;

use super::*;

const SQLITE_TRACE_STMT: c_int = 1;
const SQLITE_TRACE_PROFILE: c_int = 2;
const SQLITE_TRACE_ROW: c_int = 4;
const SQLITE_TRACE_CLOSE: c_int = 8;

const SQLITE_STMTSTATUS_FULLSCAN_STEP: c_int = 1;
const SQLITE_STMTSTATUS_SORT: c_int = 2;
const SQLITE_STMTSTATUS_AUTOINDEX: c_int = 3;
const SQLITE_STMTSTATUS_VM_STEP: c_int = 4;
const SQLITE_STMTSTATUS_REPREPARE: c_int = 5;
const SQLITE_STMTSTATUS_RUN: c_int = 6;
const SQLITE_STMTSTATUS_FILTER_MISS: c_int = 7;
const SQLITE_STMTSTATUS_FILTER_HIT: c_int = 8;
const SQLITE_STMTSTATUS_MEMUSED: c_int = 9;

pub(crate) struct TraceContext {
	pub callback: Py<PyAny>,
	pub mask: c_int,
	pub id: Option<Py<PyAny>>,
	pub db: *mut arsw::ffi::Sqlite3,
	pub connection: Py<PyAny>,
	stmt_status_initial: Mutex<HashMap<usize, StmtStatus>>,
}

#[derive(Default, Clone)]
struct StmtStatus {
	fullscan_step: i32,
	sort: i32,
	autoindex: i32,
	vm_step: i32,
	reprepare: i32,
	run: i32,
	filter_miss: i32,
	filter_hit: i32,
	memused: i32,
}

impl TraceContext {
	pub fn new(
		callback: Py<PyAny>,
		mask: c_int,
		id: Option<Py<PyAny>>,
		db: *mut arsw::ffi::Sqlite3,
		connection: Py<PyAny>,
	) -> Self {
		Self { callback, mask, id, db, connection, stmt_status_initial: Mutex::new(HashMap::new()) }
	}
}

unsafe extern "C" fn trace_callback(
	code: c_int,
	p_ctx: *mut c_void,
	one: *mut c_void,
	two: *mut c_void,
) -> c_int {
	let ctx = unsafe { &*(p_ctx as *const TraceContext) };
	let db = ctx.db;

	Python::try_attach(|py| -> PyResult<c_int> {
		let total_changes = unsafe { arsw::ffi::sqlite3_total_changes64(db) };

		let event = PyDict::new(py);

		event.set_item("code", code)?;

		match code {
			SQLITE_TRACE_STMT => {
				let stmt = one as *mut arsw::ffi::Sqlite3Stmt;
				let sql = if two.is_null() {
					""
				} else {
					unsafe { std::ffi::CStr::from_ptr(two as *const c_char).to_str().unwrap_or("") }
				};

				let trigger = sql.starts_with("-- ");
				let sql = if trigger { &sql[3..] } else { sql };

				let id = one as usize;
				event.set_item("id", id)?;

				let sql_str = sql.to_string();
				event.set_item("sql", sql_str)?;

				event.set_item("trigger", trigger)?;
				event.set_item("connection", ctx.connection.clone_ref(py))?;
				event.set_item("total_changes", total_changes)?;

				let readonly = unsafe { arsw::ffi::sqlite3_stmt_readonly(stmt) } != 0;
				event.set_item("readonly", readonly)?;

				let explain = unsafe { arsw::ffi::sqlite3_stmt_isexplain(stmt) };
				event.set_item("explain", explain)?;

				if ctx.mask & SQLITE_TRACE_PROFILE != 0 {
					let mut status = StmtStatus::default();
					status.fullscan_step =
						unsafe { arsw::ffi::sqlite3_stmt_status(stmt, SQLITE_STMTSTATUS_FULLSCAN_STEP, 1) };
					status.sort = unsafe { arsw::ffi::sqlite3_stmt_status(stmt, SQLITE_STMTSTATUS_SORT, 1) };
					status.autoindex =
						unsafe { arsw::ffi::sqlite3_stmt_status(stmt, SQLITE_STMTSTATUS_AUTOINDEX, 1) };
					status.vm_step =
						unsafe { arsw::ffi::sqlite3_stmt_status(stmt, SQLITE_STMTSTATUS_VM_STEP, 1) };
					status.reprepare =
						unsafe { arsw::ffi::sqlite3_stmt_status(stmt, SQLITE_STMTSTATUS_REPREPARE, 1) };
					status.run = unsafe { arsw::ffi::sqlite3_stmt_status(stmt, SQLITE_STMTSTATUS_RUN, 1) };
					status.filter_miss =
						unsafe { arsw::ffi::sqlite3_stmt_status(stmt, SQLITE_STMTSTATUS_FILTER_MISS, 1) };
					status.filter_hit =
						unsafe { arsw::ffi::sqlite3_stmt_status(stmt, SQLITE_STMTSTATUS_FILTER_HIT, 1) };

					let stmt_ptr = stmt as usize;
					if let Ok(mut guard) = ctx.stmt_status_initial.lock() {
						guard.insert(stmt_ptr, status);
					}
				}
			}

			SQLITE_TRACE_ROW => {
				let id = one as usize;
				event.set_item("id", id)?;
				event.set_item("connection", ctx.connection.clone_ref(py))?;
			}

			SQLITE_TRACE_PROFILE => {
				let stmt = one as *mut arsw::ffi::Sqlite3Stmt;
				let nanoseconds = two as *mut i64;
				let nanoseconds_val = unsafe { *nanoseconds };

				let sql = unsafe { arsw::ffi::sqlite3_sql(stmt) };
				let sql_str = if sql.is_null() {
					String::new()
				} else {
					unsafe { std::ffi::CStr::from_ptr(sql).to_str().unwrap_or("").to_string() }
				};

				let id = one as usize;
				event.set_item("id", id)?;
				event.set_item("sql", sql_str)?;
				event.set_item("connection", ctx.connection.clone_ref(py))?;
				event.set_item("total_changes", total_changes)?;
				event.set_item("nanoseconds", nanoseconds_val)?;

				let stmt_status = PyDict::new(py);

				let current = StmtStatus {
					fullscan_step: unsafe {
						arsw::ffi::sqlite3_stmt_status(stmt, SQLITE_STMTSTATUS_FULLSCAN_STEP, 0)
					},
					sort: unsafe { arsw::ffi::sqlite3_stmt_status(stmt, SQLITE_STMTSTATUS_SORT, 0) },
					autoindex: unsafe {
						arsw::ffi::sqlite3_stmt_status(stmt, SQLITE_STMTSTATUS_AUTOINDEX, 0)
					},
					vm_step: unsafe { arsw::ffi::sqlite3_stmt_status(stmt, SQLITE_STMTSTATUS_VM_STEP, 0) },
					reprepare: unsafe {
						arsw::ffi::sqlite3_stmt_status(stmt, SQLITE_STMTSTATUS_REPREPARE, 0)
					},
					run: unsafe { arsw::ffi::sqlite3_stmt_status(stmt, SQLITE_STMTSTATUS_RUN, 0) },
					filter_miss: unsafe {
						arsw::ffi::sqlite3_stmt_status(stmt, SQLITE_STMTSTATUS_FILTER_MISS, 0)
					},
					filter_hit: unsafe {
						arsw::ffi::sqlite3_stmt_status(stmt, SQLITE_STMTSTATUS_FILTER_HIT, 0)
					},
					memused: unsafe { arsw::ffi::sqlite3_stmt_status(stmt, SQLITE_STMTSTATUS_MEMUSED, 0) },
				};

				let stmt_ptr = stmt as usize;
				let initial = if let Ok(guard) = ctx.stmt_status_initial.lock() {
					guard.get(&stmt_ptr).cloned()
				} else {
					None
				};

				if let Some(initial) = initial {
					stmt_status.set_item(
						"SQLITE_STMTSTATUS_FULLSCAN_STEP",
						(current.fullscan_step - initial.fullscan_step) as i64,
					)?;
					stmt_status.set_item("SQLITE_STMTSTATUS_SORT", (current.sort - initial.sort) as i64)?;
					stmt_status.set_item(
						"SQLITE_STMTSTATUS_AUTOINDEX",
						(current.autoindex - initial.autoindex) as i64,
					)?;
					stmt_status
						.set_item("SQLITE_STMTSTATUS_VM_STEP", (current.vm_step - initial.vm_step) as i64)?;
					stmt_status.set_item(
						"SQLITE_STMTSTATUS_REPREPARE",
						(current.reprepare - initial.reprepare) as i64,
					)?;
					stmt_status.set_item("SQLITE_STMTSTATUS_RUN", (current.run - initial.run) as i64)?;
					stmt_status.set_item(
						"SQLITE_STMTSTATUS_FILTER_MISS",
						(current.filter_miss - initial.filter_miss) as i64,
					)?;
					stmt_status.set_item(
						"SQLITE_STMTSTATUS_FILTER_HIT",
						(current.filter_hit - initial.filter_hit) as i64,
					)?;
					stmt_status
						.set_item("SQLITE_STMTSTATUS_MEMUSED", (current.memused - initial.memused) as i64)?;
				}

				event.set_item("stmt_status", stmt_status)?;
			}

			SQLITE_TRACE_CLOSE => {
				event.set_item("code", code)?;
				let conn: Py<PyAny> = py.None();
				event.set_item("connection", conn)?;
			}

			_ => {}
		}

		let id = ctx.id.as_ref().map(|id| id.clone_ref(py));
		let callback = ctx.callback.clone_ref(py);

		if let Some(id) = id {
			callback.call1(py, (event, id))?;
		} else {
			callback.call1(py, (event,))?;
		}

		Ok(0)
	})
	.map_or(0, |r| r.unwrap_or(0))
}

pub(crate) fn register_trace(py: Python<'_>, ctx: &TraceContext) -> PyResult<()> {
	let db = ctx.db;

	if db.is_null() {
		return Err(pyo3::exceptions::PyRuntimeError::new_err("Database connection is closed"));
	}

	let mask = ctx.mask;
	if mask == 0 {
		return Ok(());
	}

	unsafe {
		let result = arsw::ffi::sqlite3_trace_v2(
			db,
			mask,
			Some(trace_callback),
			ctx as *const TraceContext as *mut c_void,
		);

		if result != 0 {
			return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
				"Failed to register trace callback: {}",
				result
			)));
		}
	}

	Ok(())
}

pub(crate) fn unregister_trace(
	db: *mut arsw::ffi::Sqlite3,
	ctx_ptr: Option<*mut c_void>,
) -> PyResult<()> {
	if db.is_null() {
		return Ok(());
	}

	unsafe {
		// When mask is 0, SQLite ignores the callback but uses the context
		// to identify which trace to unregister. Pass the original context.
		let result = arsw::ffi::sqlite3_trace_v2(db, 0, None, ctx_ptr.unwrap_or(std::ptr::null_mut()));
		if result != 0 && result != 21 {
			// 21 is SQLITE_MISUSE
			return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
				"Failed to unregister trace callback: {}",
				result
			)));
		}
	}

	Ok(())
}
