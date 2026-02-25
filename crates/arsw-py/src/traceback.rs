use core::ffi::{c_char, c_int, c_void};

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

pub(crate) struct TraceHook {
	pub callback: Py<PyAny>,
	pub mask: c_int,
	pub id: Option<Py<PyAny>>,
}

impl TraceHook {
	pub fn clone_ref(&self, py: Python<'_>) -> Self {
		Self {
			callback: self.callback.clone_ref(py),
			mask: self.mask,
			id: self.id.as_ref().map(|id| id.clone_ref(py)),
		}
	}
}

pub(crate) struct TraceContext {
	pub hooks: Vec<TraceHook>,
	pub db: *mut arsw::ffi::Sqlite3,
	pub connection: Py<PyAny>,
}

impl TraceContext {
	pub fn new(hooks: Vec<TraceHook>, db: *mut arsw::ffi::Sqlite3, connection: Py<PyAny>) -> Self {
		Self { hooks, db, connection }
	}

	fn has_profile_listener(&self) -> bool {
		self.hooks.iter().any(|hook| hook.mask & SQLITE_TRACE_PROFILE != 0)
	}

	fn has_trace_listener_for(&self, code: c_int) -> bool {
		self.hooks.iter().any(|hook| hook.mask & code != 0)
	}

	fn combined_mask(&self) -> c_int {
		let mut mask = 0;
		for hook in &self.hooks {
			mask |= hook.mask;
		}

		if mask & SQLITE_TRACE_PROFILE != 0 {
			mask |= SQLITE_TRACE_STMT;
		}

		mask
	}
}

unsafe extern "C" fn trace_callback(
	code: c_int,
	p_ctx: *mut c_void,
	one: *mut c_void,
	two: *mut c_void,
) -> c_int {
	if p_ctx.is_null() {
		return 0;
	}

	let ctx = unsafe { &*(p_ctx as *const TraceContext) };
	let db = ctx.db;

	let Some(result) = Python::try_attach(|py| {
		let wants_event = ctx.has_trace_listener_for(code);
		let mut event: Option<Py<PyDict>> = None;
		let mut first_error: Option<PyErr> = None;

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

				if !trigger && ctx.has_profile_listener() {
					unsafe {
						arsw::ffi::sqlite3_stmt_status(stmt, SQLITE_STMTSTATUS_FULLSCAN_STEP, 1);
						arsw::ffi::sqlite3_stmt_status(stmt, SQLITE_STMTSTATUS_SORT, 1);
						arsw::ffi::sqlite3_stmt_status(stmt, SQLITE_STMTSTATUS_AUTOINDEX, 1);
						arsw::ffi::sqlite3_stmt_status(stmt, SQLITE_STMTSTATUS_VM_STEP, 1);
						arsw::ffi::sqlite3_stmt_status(stmt, SQLITE_STMTSTATUS_REPREPARE, 1);
						arsw::ffi::sqlite3_stmt_status(stmt, SQLITE_STMTSTATUS_RUN, 1);
						arsw::ffi::sqlite3_stmt_status(stmt, SQLITE_STMTSTATUS_FILTER_MISS, 1);
						arsw::ffi::sqlite3_stmt_status(stmt, SQLITE_STMTSTATUS_FILTER_HIT, 1);
					}
				}

				if wants_event {
					let total_changes = unsafe { arsw::ffi::sqlite3_total_changes64(db) };
					let event_dict = PyDict::new(py);
					if let Err(err) = (|| -> PyResult<()> {
						event_dict.set_item("code", code)?;
						event_dict.set_item("id", one as usize)?;
						event_dict.set_item("sql", sql)?;
						event_dict.set_item("trigger", trigger)?;
						event_dict.set_item("connection", ctx.connection.clone_ref(py))?;
						event_dict.set_item("total_changes", total_changes)?;
						event_dict
							.set_item("readonly", unsafe { arsw::ffi::sqlite3_stmt_readonly(stmt) } != 0)?;
						event_dict.set_item("explain", unsafe { arsw::ffi::sqlite3_stmt_isexplain(stmt) })?;
						Ok(())
					})() {
						first_error = Some(err);
					} else {
						event = Some(event_dict.unbind());
					}
				}
			}

			SQLITE_TRACE_ROW => {
				if wants_event {
					let event_dict = PyDict::new(py);
					if let Err(err) = (|| -> PyResult<()> {
						event_dict.set_item("code", code)?;
						event_dict.set_item("id", one as usize)?;
						event_dict.set_item("connection", ctx.connection.clone_ref(py))?;
						Ok(())
					})() {
						first_error = Some(err);
					} else {
						event = Some(event_dict.unbind());
					}
				}
			}

			SQLITE_TRACE_PROFILE => {
				let stmt = one as *mut arsw::ffi::Sqlite3Stmt;
				let nanoseconds =
					if two.is_null() { 0 } else { unsafe { *(two as *const arsw::ffi::Sqlite3Int64) } };
				let nanoseconds = if nanoseconds > 0 { nanoseconds } else { 1 };
				if wants_event {
					let sql = unsafe { arsw::ffi::sqlite3_sql(stmt) };
					let sql = if sql.is_null() {
						String::new()
					} else {
						unsafe { std::ffi::CStr::from_ptr(sql).to_string_lossy().into_owned() }
					};
					let total_changes = unsafe { arsw::ffi::sqlite3_total_changes64(db) };
					let event_dict = PyDict::new(py);
					if let Err(err) = (|| -> PyResult<()> {
						event_dict.set_item("code", code)?;
						event_dict.set_item("id", one as usize)?;
						event_dict.set_item("sql", sql)?;
						event_dict.set_item("connection", ctx.connection.clone_ref(py))?;
						event_dict.set_item("total_changes", total_changes)?;
						event_dict.set_item("nanoseconds", nanoseconds)?;

						let stmt_status = PyDict::new(py);
						stmt_status.set_item("SQLITE_STMTSTATUS_FULLSCAN_STEP", unsafe {
							arsw::ffi::sqlite3_stmt_status(stmt, SQLITE_STMTSTATUS_FULLSCAN_STEP, 0)
						} as i64)?;
						stmt_status.set_item("SQLITE_STMTSTATUS_SORT", unsafe {
							arsw::ffi::sqlite3_stmt_status(stmt, SQLITE_STMTSTATUS_SORT, 0)
						} as i64)?;
						stmt_status.set_item("SQLITE_STMTSTATUS_AUTOINDEX", unsafe {
							arsw::ffi::sqlite3_stmt_status(stmt, SQLITE_STMTSTATUS_AUTOINDEX, 0)
						} as i64)?;
						stmt_status.set_item("SQLITE_STMTSTATUS_VM_STEP", unsafe {
							arsw::ffi::sqlite3_stmt_status(stmt, SQLITE_STMTSTATUS_VM_STEP, 0)
						} as i64)?;
						stmt_status.set_item("SQLITE_STMTSTATUS_REPREPARE", unsafe {
							arsw::ffi::sqlite3_stmt_status(stmt, SQLITE_STMTSTATUS_REPREPARE, 0)
						} as i64)?;
						stmt_status.set_item("SQLITE_STMTSTATUS_RUN", unsafe {
							arsw::ffi::sqlite3_stmt_status(stmt, SQLITE_STMTSTATUS_RUN, 0)
						} as i64)?;
						stmt_status.set_item("SQLITE_STMTSTATUS_FILTER_MISS", unsafe {
							arsw::ffi::sqlite3_stmt_status(stmt, SQLITE_STMTSTATUS_FILTER_MISS, 0)
						} as i64)?;
						stmt_status.set_item("SQLITE_STMTSTATUS_FILTER_HIT", unsafe {
							arsw::ffi::sqlite3_stmt_status(stmt, SQLITE_STMTSTATUS_FILTER_HIT, 0)
						} as i64)?;
						stmt_status.set_item("SQLITE_STMTSTATUS_MEMUSED", unsafe {
							arsw::ffi::sqlite3_stmt_status(stmt, SQLITE_STMTSTATUS_MEMUSED, 0)
						} as i64)?;
						event_dict.set_item("stmt_status", stmt_status)?;

						Ok(())
					})() {
						first_error = Some(err);
					} else {
						event = Some(event_dict.unbind());
					}
				}
			}

			SQLITE_TRACE_CLOSE => {
				if wants_event {
					let event_dict = PyDict::new(py);
					if let Err(err) = (|| -> PyResult<()> {
						event_dict.set_item("code", code)?;
						event_dict.set_item("connection", ctx.connection.clone_ref(py))?;
						Ok(())
					})() {
						first_error = Some(err);
					} else {
						event = Some(event_dict.unbind());
					}
				}
			}

			_ => {}
		}

		if let Some(event) = event {
			let event = event.bind(py);
			for hook in &ctx.hooks {
				if hook.mask & code == 0 {
					continue;
				}

				let callback = hook.callback.bind(py);
				let _id = hook.id.as_ref();
				let call_result = callback.call1((event.clone(),));

				if let Err(err) = call_result {
					if first_error.is_none() {
						first_error = Some(err.clone_ref(py));
					}
				}
			}
		}

		if let Some(err) = first_error {
			set_callback_error(py, &err);
		}

		0
	}) else {
		return 0;
	};

	result
}

pub(crate) fn register_trace(_py: Python<'_>, ctx: &TraceContext) -> PyResult<()> {
	let db = ctx.db;

	if db.is_null() {
		return Err(pyo3::exceptions::PyRuntimeError::new_err("Database connection is closed"));
	}

	let mask = ctx.combined_mask();
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
		let result = arsw::ffi::sqlite3_trace_v2(db, 0, None, ctx_ptr.unwrap_or(std::ptr::null_mut()));
		if result != 0 && result != 21 {
			return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
				"Failed to unregister trace callback: {}",
				result
			)));
		}
	}

	Ok(())
}
