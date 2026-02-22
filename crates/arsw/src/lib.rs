mod capi;

pub use arsw_sqlite_ffi::ffi;
pub use arsw_sqlite_ffi::raw;

use core::ffi::c_int;
use std::ffi::{CStr, CString};
use std::fmt;
use std::ptr::NonNull;

const SQLITE_OK: c_int = 0;
const SQLITE_ROW: c_int = 100;
const SQLITE_DONE: c_int = 101;

const SQLITE_INTEGER: c_int = 1;
const SQLITE_FLOAT: c_int = 2;
const SQLITE_TEXT: c_int = 3;
const SQLITE_BLOB: c_int = 4;

const SQLITE_OPEN_READWRITE: c_int = 0x0000_0002;
const SQLITE_OPEN_CREATE: c_int = 0x0000_0004;
const SQLITE_OPEN_URI: c_int = 0x0000_0040;

const SQLITE_MISUSE: c_int = 21;
const SQLITE_UTF8: u8 = 1;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Error {
	pub code: c_int,
	pub message: String,
}

impl fmt::Display for Error {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "SQLite error {}: {}", self.code, self.message)
	}
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
	Null,
	Integer(i64),
	Float(f64),
	Text(String),
	Blob(Vec<u8>),
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum TransactionMode {
	Deferred,
	Immediate,
	Exclusive,
}

impl TransactionMode {
	const fn begin_sql(self) -> &'static str {
		match self {
			Self::Deferred => "BEGIN DEFERRED",
			Self::Immediate => "BEGIN IMMEDIATE",
			Self::Exclusive => "BEGIN EXCLUSIVE",
		}
	}
}

pub struct Connection {
	db: NonNull<ffi::Sqlite3>,
}

impl Connection {
	pub fn open(path: &str) -> Result<Self> {
		let path_c = CString::new(path).map_err(|_| Error {
			code: SQLITE_MISUSE,
			message: "database path contains a NUL byte".to_string(),
		})?;

		let mut db: *mut ffi::Sqlite3 = std::ptr::null_mut();
		let flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI;

		let code =
			unsafe { ffi::sqlite3_open_v2(path_c.as_ptr(), &raw mut db, flags, std::ptr::null()) };
		if code != SQLITE_OK {
			let error = sqlite_error(db, code);
			if !db.is_null() {
				let _ = unsafe { ffi::sqlite3_close_v2(db) };
			}
			return Err(error);
		}

		let db = NonNull::new(db).ok_or_else(|| Error {
			code: SQLITE_MISUSE,
			message: "SQLite returned a null connection handle".to_string(),
		})?;

		let _ = unsafe { ffi::sqlite3_extended_result_codes(db.as_ptr(), 1) };

		Ok(Self { db })
	}

	pub fn open_in_memory() -> Result<Self> {
		Self::open(":memory:")
	}

	pub fn prepare(&self, sql: &str) -> Result<Statement<'_>> {
		let sql_c = CString::new(sql)
			.map_err(|_| Error { code: SQLITE_MISUSE, message: "SQL contains a NUL byte".to_string() })?;

		let mut stmt: *mut ffi::Sqlite3Stmt = std::ptr::null_mut();
		let code = unsafe {
			ffi::sqlite3_prepare_v3(
				self.db.as_ptr(),
				sql_c.as_ptr(),
				-1,
				0,
				&raw mut stmt,
				std::ptr::null_mut(),
			)
		};
		if code != SQLITE_OK {
			return Err(sqlite_error(self.db.as_ptr(), code));
		}

		let stmt = NonNull::new(stmt).ok_or_else(|| Error {
			code: SQLITE_MISUSE,
			message: "SQL did not produce a statement".to_string(),
		})?;

		Ok(Statement { conn: self, stmt })
	}

	pub fn create_session(&self, db_name: &str) -> Result<Session<'_>> {
		let db_name_c = CString::new(db_name).map_err(|_| Error {
			code: SQLITE_MISUSE,
			message: "database name contains a NUL byte".to_string(),
		})?;

		let mut session: *mut ffi::Sqlite3Session = std::ptr::null_mut();
		let code =
			unsafe { ffi::sqlite3session_create(self.db.as_ptr(), db_name_c.as_ptr(), &raw mut session) };
		if code != SQLITE_OK {
			return Err(sqlite_error(self.db.as_ptr(), code));
		}

		let session = NonNull::new(session).ok_or_else(|| Error {
			code: SQLITE_MISUSE,
			message: "SQLite returned a null session handle".to_string(),
		})?;

		Ok(Session { conn: self, session })
	}

	pub fn execute(&self, sql: &str) -> Result<()> {
		let mut stmt = self.prepare(sql)?;
		while stmt.step()? {}
		Ok(())
	}

	pub fn query_all(&self, sql: &str) -> Result<Vec<Vec<Value>>> {
		let mut stmt = self.prepare(sql)?;
		let mut rows = Vec::new();
		while stmt.step()? {
			rows.push(stmt.row_values());
		}
		Ok(rows)
	}

	pub fn query_row(&self, sql: &str) -> Result<Option<Vec<Value>>> {
		let mut stmt = self.prepare(sql)?;
		if !stmt.step()? {
			return Ok(None);
		}

		let row = stmt.row_values();
		if stmt.step()? {
			return Err(Error {
				code: SQLITE_MISUSE,
				message: "query returned more than one row".to_string(),
			});
		}

		Ok(Some(row))
	}

	pub fn transaction(&self) -> Result<Transaction<'_>> {
		self.transaction_with_mode(TransactionMode::Immediate)
	}

	pub fn transaction_with_mode(&self, mode: TransactionMode) -> Result<Transaction<'_>> {
		self.execute(mode.begin_sql())?;
		Ok(Transaction { conn: self, active: true })
	}

	#[must_use]
	pub const fn as_ptr(&self) -> *mut ffi::Sqlite3 {
		self.db.as_ptr()
	}
}

impl Drop for Connection {
	fn drop(&mut self) {
		let _ = unsafe { ffi::sqlite3_close_v2(self.db.as_ptr()) };
	}
}

pub struct Statement<'conn> {
	conn: &'conn Connection,
	stmt: NonNull<ffi::Sqlite3Stmt>,
}

pub struct Session<'conn> {
	conn: &'conn Connection,
	session: NonNull<ffi::Sqlite3Session>,
}

pub struct Transaction<'conn> {
	conn: &'conn Connection,
	active: bool,
}

impl Session<'_> {
	pub fn attach(&mut self, table: Option<&str>) -> Result<()> {
		let table_c = table.map(CString::new).transpose().map_err(|_| Error {
			code: SQLITE_MISUSE,
			message: "table name contains a NUL byte".to_string(),
		})?;

		let table_ptr = table_c.as_ref().map_or(std::ptr::null(), |name| name.as_ptr());
		let code = unsafe { ffi::sqlite3session_attach(self.session.as_ptr(), table_ptr) };
		check_ok(self.conn.as_ptr(), code)
	}

	pub fn set_enabled(&mut self, enabled: bool) -> Result<bool> {
		let code = unsafe { ffi::sqlite3session_enable(self.session.as_ptr(), i32::from(enabled)) };
		if code < 0 { Err(sqlite_error(self.conn.as_ptr(), SQLITE_MISUSE)) } else { Ok(code != 0) }
	}

	pub fn set_indirect(&mut self, indirect: bool) -> Result<bool> {
		let code = unsafe { ffi::sqlite3session_indirect(self.session.as_ptr(), i32::from(indirect)) };
		if code < 0 { Err(sqlite_error(self.conn.as_ptr(), SQLITE_MISUSE)) } else { Ok(code != 0) }
	}

	#[must_use]
	pub fn is_empty(&self) -> bool {
		let code = unsafe { ffi::sqlite3session_isempty(self.session.as_ptr()) };
		code != 0
	}

	pub fn changeset(&mut self) -> Result<Vec<u8>> {
		let mut len: c_int = 0;
		let mut out: *mut std::ffi::c_void = std::ptr::null_mut();
		let code =
			unsafe { ffi::sqlite3session_changeset(self.session.as_ptr(), &raw mut len, &raw mut out) };
		if code != SQLITE_OK {
			return Err(sqlite_error(self.conn.as_ptr(), code));
		}

		if out.is_null() || len <= 0 {
			return Ok(Vec::new());
		}

		let len = usize::try_from(len).map_err(|_| Error {
			code: SQLITE_MISUSE,
			message: "changeset size did not fit in usize".to_string(),
		})?;
		let bytes = unsafe { std::slice::from_raw_parts(out.cast::<u8>(), len) }.to_vec();
		unsafe { ffi::sqlite3_free(out) };
		Ok(bytes)
	}

	pub fn patchset(&mut self) -> Result<Vec<u8>> {
		let mut len: c_int = 0;
		let mut out: *mut std::ffi::c_void = std::ptr::null_mut();
		let code =
			unsafe { ffi::sqlite3session_patchset(self.session.as_ptr(), &raw mut len, &raw mut out) };
		if code != SQLITE_OK {
			return Err(sqlite_error(self.conn.as_ptr(), code));
		}

		if out.is_null() || len <= 0 {
			return Ok(Vec::new());
		}

		let len = usize::try_from(len).map_err(|_| Error {
			code: SQLITE_MISUSE,
			message: "patchset size did not fit in usize".to_string(),
		})?;
		let bytes = unsafe { std::slice::from_raw_parts(out.cast::<u8>(), len) }.to_vec();
		unsafe { ffi::sqlite3_free(out) };
		Ok(bytes)
	}
}

impl Drop for Session<'_> {
	fn drop(&mut self) {
		unsafe { ffi::sqlite3session_delete(self.session.as_ptr()) };
	}
}

impl<'conn> Transaction<'conn> {
	pub fn prepare(&self, sql: &str) -> Result<Statement<'conn>> {
		self.conn.prepare(sql)
	}

	pub fn execute(&self, sql: &str) -> Result<()> {
		self.conn.execute(sql)
	}

	pub fn query_all(&self, sql: &str) -> Result<Vec<Vec<Value>>> {
		self.conn.query_all(sql)
	}

	pub fn query_row(&self, sql: &str) -> Result<Option<Vec<Value>>> {
		self.conn.query_row(sql)
	}

	pub fn commit(mut self) -> Result<()> {
		if !self.active {
			return Ok(());
		}

		self.conn.execute("COMMIT")?;
		self.active = false;
		Ok(())
	}

	pub fn rollback(mut self) -> Result<()> {
		if !self.active {
			return Ok(());
		}

		self.conn.execute("ROLLBACK")?;
		self.active = false;
		Ok(())
	}

	#[must_use]
	pub const fn is_active(&self) -> bool {
		self.active
	}
}

impl Drop for Transaction<'_> {
	fn drop(&mut self) {
		if self.active {
			let _ = self.conn.execute("ROLLBACK");
		}
	}
}

impl Statement<'_> {
	#[must_use]
	pub fn parameter_count(&self) -> usize {
		let n = unsafe { ffi::sqlite3_bind_parameter_count(self.stmt.as_ptr()) };
		usize::try_from(n).unwrap_or_default()
	}

	pub fn bind_value(&mut self, index: c_int, value: &Value) -> Result<()> {
		match value {
			Value::Null => self.bind_null(index),
			Value::Integer(v) => self.bind_i64(index, *v),
			Value::Float(v) => self.bind_f64(index, *v),
			Value::Text(v) => self.bind_text(index, v),
			Value::Blob(v) => self.bind_blob(index, v),
		}
	}

	pub fn bind_values(&mut self, values: &[Value]) -> Result<()> {
		let expected = self.parameter_count();
		if values.len() != expected {
			return Err(Error {
				code: SQLITE_MISUSE,
				message: format!("expected {expected} binding values, got {}", values.len()),
			});
		}

		for (offset, value) in values.iter().enumerate() {
			let index = c_int::try_from(offset + 1).map_err(|_| Error {
				code: SQLITE_MISUSE,
				message: "binding index did not fit in c_int".to_string(),
			})?;
			self.bind_value(index, value)?;
		}

		Ok(())
	}

	pub fn bind_named(&mut self, name: &str, value: &Value) -> Result<()> {
		for candidate in binding_name_candidates(name) {
			let candidate = CString::new(candidate).map_err(|_| Error {
				code: SQLITE_MISUSE,
				message: "binding name contains a NUL byte".to_string(),
			})?;

			let index =
				unsafe { ffi::sqlite3_bind_parameter_index(self.stmt.as_ptr(), candidate.as_ptr()) };
			if index > 0 {
				return self.bind_value(index, value);
			}
		}

		Err(Error { code: SQLITE_MISUSE, message: format!("unknown named parameter: {name}") })
	}

	pub fn bind_null(&mut self, index: c_int) -> Result<()> {
		let code = unsafe { ffi::sqlite3_bind_null(self.stmt.as_ptr(), index) };
		check_ok(self.conn.as_ptr(), code)
	}

	pub fn bind_i64(&mut self, index: c_int, value: i64) -> Result<()> {
		let code = unsafe { ffi::sqlite3_bind_int64(self.stmt.as_ptr(), index, value) };
		check_ok(self.conn.as_ptr(), code)
	}

	pub fn bind_f64(&mut self, index: c_int, value: f64) -> Result<()> {
		let code = unsafe { ffi::sqlite3_bind_double(self.stmt.as_ptr(), index, value) };
		check_ok(self.conn.as_ptr(), code)
	}

	pub fn bind_text(&mut self, index: c_int, value: &str) -> Result<()> {
		let text = CString::new(value).map_err(|_| Error {
			code: SQLITE_MISUSE,
			message: "text parameter contains a NUL byte".to_string(),
		})?;
		let len = u64::try_from(text.as_bytes().len()).map_err(|_| Error {
			code: SQLITE_MISUSE,
			message: "text parameter is too large".to_string(),
		})?;

		let code = unsafe {
			ffi::sqlite3_bind_text64(
				self.stmt.as_ptr(),
				index,
				text.as_ptr(),
				len,
				Some(sqlite_transient()),
				SQLITE_UTF8,
			)
		};
		check_ok(self.conn.as_ptr(), code)
	}

	pub fn bind_blob(&mut self, index: c_int, value: &[u8]) -> Result<()> {
		let len = u64::try_from(value.len()).map_err(|_| Error {
			code: SQLITE_MISUSE,
			message: "blob parameter is too large".to_string(),
		})?;

		let code = unsafe {
			ffi::sqlite3_bind_blob64(
				self.stmt.as_ptr(),
				index,
				value.as_ptr().cast(),
				len,
				Some(sqlite_transient()),
			)
		};
		check_ok(self.conn.as_ptr(), code)
	}

	pub fn step(&mut self) -> Result<bool> {
		let code = unsafe { ffi::sqlite3_step(self.stmt.as_ptr()) };
		match code {
			SQLITE_ROW => Ok(true),
			SQLITE_DONE => Ok(false),
			_ => Err(sqlite_error(self.conn.as_ptr(), code)),
		}
	}

	pub fn reset(&mut self) -> Result<()> {
		let code = unsafe { ffi::sqlite3_reset(self.stmt.as_ptr()) };
		check_ok(self.conn.as_ptr(), code)
	}

	#[must_use]
	pub fn column_count(&self) -> usize {
		let n = unsafe { ffi::sqlite3_column_count(self.stmt.as_ptr()) };
		usize::try_from(n).unwrap_or_default()
	}

	#[must_use]
	pub fn column_name(&self, index: c_int) -> Option<String> {
		let ptr = unsafe { ffi::sqlite3_column_name(self.stmt.as_ptr(), index) };
		if ptr.is_null() {
			None
		} else {
			Some(unsafe { CStr::from_ptr(ptr).to_string_lossy().into_owned() })
		}
	}

	#[must_use]
	pub fn row_values(&self) -> Vec<Value> {
		let mut row = Vec::with_capacity(self.column_count());
		for i in 0..self.column_count() {
			let Ok(index) = c_int::try_from(i) else {
				break;
			};
			row.push(self.column_value(index));
		}
		row
	}

	#[must_use]
	pub fn column_value(&self, index: c_int) -> Value {
		match unsafe { ffi::sqlite3_column_type(self.stmt.as_ptr(), index) } {
			SQLITE_INTEGER => {
				Value::Integer(unsafe { ffi::sqlite3_column_int64(self.stmt.as_ptr(), index) })
			}
			SQLITE_FLOAT => {
				Value::Float(unsafe { ffi::sqlite3_column_double(self.stmt.as_ptr(), index) })
			}
			SQLITE_TEXT => {
				let bytes = unsafe { ffi::sqlite3_column_bytes(self.stmt.as_ptr(), index) };
				let ptr = unsafe { ffi::sqlite3_column_text(self.stmt.as_ptr(), index) };
				if ptr.is_null() || bytes <= 0 {
					Value::Text(String::new())
				} else {
					let slice =
						unsafe { std::slice::from_raw_parts(ptr, usize::try_from(bytes).unwrap_or_default()) };
					Value::Text(String::from_utf8_lossy(slice).into_owned())
				}
			}
			SQLITE_BLOB => {
				let bytes = unsafe { ffi::sqlite3_column_bytes(self.stmt.as_ptr(), index) };
				let ptr = unsafe { ffi::sqlite3_column_blob(self.stmt.as_ptr(), index) };
				if ptr.is_null() || bytes <= 0 {
					Value::Blob(Vec::new())
				} else {
					let slice = unsafe {
						std::slice::from_raw_parts(ptr.cast::<u8>(), usize::try_from(bytes).unwrap_or_default())
					};
					Value::Blob(slice.to_vec())
				}
			}
			_ => Value::Null,
		}
	}

	#[must_use]
	pub fn sql(&self) -> Option<String> {
		let ptr = unsafe { ffi::sqlite3_sql(self.stmt.as_ptr()) };
		if ptr.is_null() {
			None
		} else {
			Some(unsafe { CStr::from_ptr(ptr).to_string_lossy().into_owned() })
		}
	}
}

impl Drop for Statement<'_> {
	fn drop(&mut self) {
		let _ = unsafe { ffi::sqlite3_finalize(self.stmt.as_ptr()) };
	}
}

fn binding_name_candidates(name: &str) -> Vec<String> {
	if name.starts_with(':')
		|| name.starts_with('@')
		|| name.starts_with('$')
		|| name.starts_with('?')
	{
		vec![name.to_string()]
	} else {
		vec![format!(":{name}"), format!("@{name}"), format!("${name}")]
	}
}

fn sqlite_transient() -> unsafe extern "C" fn(*mut std::ffi::c_void) {
	unsafe { std::mem::transmute::<isize, unsafe extern "C" fn(*mut std::ffi::c_void)>(-1_isize) }
}

fn check_ok(db: *mut ffi::Sqlite3, code: c_int) -> Result<()> {
	if code == SQLITE_OK { Ok(()) } else { Err(sqlite_error(db, code)) }
}

fn sqlite_error(db: *mut ffi::Sqlite3, code: c_int) -> Error {
	Error { code, message: db_error_message(db) }
}

fn db_error_message(db: *mut ffi::Sqlite3) -> String {
	if db.is_null() {
		return "SQLite error".to_string();
	}

	let message_ptr = unsafe { ffi::sqlite3_errmsg(db) };
	if message_ptr.is_null() {
		return "SQLite error".to_string();
	}

	unsafe { CStr::from_ptr(message_ptr).to_string_lossy().into_owned() }
}

pub fn sqlite_lib_version() -> String {
	arsw_sqlite_ffi::sqlite_lib_version()
}

pub fn sqlite_lib_version_number() -> i32 {
	arsw_sqlite_ffi::sqlite_lib_version_number()
}

pub fn sqlite_source_id() -> String {
	arsw_sqlite_ffi::sqlite_source_id()
}

pub fn sqlite_compile_option_used(name: &str) -> bool {
	arsw_sqlite_ffi::sqlite_compile_option_used(name)
}

pub fn sqlite_compile_options() -> Vec<String> {
	arsw_sqlite_ffi::sqlite_compile_options()
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn sqlite_version_number_is_sane() {
		assert!(sqlite_lib_version_number() >= 3_000_000);
	}

	#[test]
	fn exposes_raw_sqlite_api() {
		let _ = raw::SQLITE_CARRAY_INT32;
	}

	#[test]
	fn can_execute_and_query_rows() {
		let db = Connection::open_in_memory().expect("open memory database");
		db.execute("create table items(id integer primary key, name text not null)")
			.expect("create table");
		db.execute("insert into items(name) values ('apples'), ('oranges')").expect("insert rows");

		let rows = db.query_all("select id, name from items order by id").expect("query rows");

		assert_eq!(
			rows,
			vec![
				vec![Value::Integer(1), Value::Text("apples".to_string())],
				vec![Value::Integer(2), Value::Text("oranges".to_string())],
			]
		);
	}

	#[test]
	fn statement_bindings_work() {
		let db = Connection::open_in_memory().expect("open memory database");
		db.execute("create table valueset(i integer, t text, b blob)").expect("create table");

		let mut stmt =
			db.prepare("insert into valueset(i, t, b) values (?, ?, ?)").expect("prepare insert");
		stmt.bind_i64(1, 41).expect("bind integer");
		stmt.bind_text(2, "hello").expect("bind text");
		stmt.bind_blob(3, b"abc").expect("bind blob");
		let done = stmt.step().expect("step insert");
		assert!(!done);

		let rows = db.query_all("select i, t, b from valueset").expect("query valueset");
		assert_eq!(
			rows,
			vec![vec![
				Value::Integer(41),
				Value::Text("hello".to_string()),
				Value::Blob(b"abc".to_vec())
			]]
		);
	}

	#[test]
	fn bind_value_handles_all_types() {
		let db = Connection::open_in_memory().expect("open memory database");
		db.execute("create table valueset(i integer, f real, t text, b blob, n text)")
			.expect("create table");

		let mut stmt = db
			.prepare("insert into valueset(i, f, t, b, n) values (?, ?, ?, ?, ?)")
			.expect("prepare insert");
		stmt.bind_value(1, &Value::Integer(7)).expect("bind integer");
		stmt.bind_value(2, &Value::Float(3.5)).expect("bind float");
		stmt.bind_value(3, &Value::Text("ok".to_string())).expect("bind text");
		stmt.bind_value(4, &Value::Blob(vec![1, 2, 3])).expect("bind blob");
		stmt.bind_value(5, &Value::Null).expect("bind null");
		let _ = stmt.step().expect("step insert");

		let rows = db.query_all("select i, f, t, b, n from valueset").expect("query valueset");
		assert_eq!(
			rows,
			vec![vec![
				Value::Integer(7),
				Value::Float(3.5),
				Value::Text("ok".to_string()),
				Value::Blob(vec![1, 2, 3]),
				Value::Null,
			]]
		);
	}

	#[test]
	fn bind_named_and_bind_values_work() {
		let db = Connection::open_in_memory().expect("open memory database");
		db.execute("create table kv(name text, qty integer)").expect("create table");

		let mut named =
			db.prepare("insert into kv(name, qty) values (:name, :qty)").expect("prepare named insert");
		named.bind_named("name", &Value::Text("apples".to_string())).expect("bind named name");
		named.bind_named(":qty", &Value::Integer(12)).expect("bind named qty");
		let _ = named.step().expect("execute named insert");

		let mut positional =
			db.prepare("insert into kv(name, qty) values (?, ?)").expect("prepare positional insert");
		positional
			.bind_values(&[Value::Text("pears".to_string()), Value::Integer(7)])
			.expect("bind positional values");
		let _ = positional.step().expect("execute positional insert");

		let rows = db.query_all("select name, qty from kv order by name").expect("query kv rows");
		assert_eq!(
			rows,
			vec![
				vec![Value::Text("apples".to_string()), Value::Integer(12)],
				vec![Value::Text("pears".to_string()), Value::Integer(7)],
			]
		);
	}

	#[test]
	fn query_row_returns_none_single_or_error() {
		let db = Connection::open_in_memory().expect("open memory database");

		let none_row = db.query_row("select 1 where 0").expect("run zero-row query");
		assert_eq!(none_row, None);

		let one_row = db.query_row("select 42").expect("run single-row query");
		assert_eq!(one_row, Some(vec![Value::Integer(42)]));

		let err = db.query_row("select 1 union all select 2").expect_err("query should fail");
		assert_eq!(err.code, SQLITE_MISUSE);
		assert_eq!(err.message, "query returned more than one row");
	}

	#[test]
	fn transaction_rolls_back_on_drop_and_can_commit() {
		let db = Connection::open_in_memory().expect("open memory database");
		db.execute("create table txlog(value text)").expect("create table");

		{
			let tx = db.transaction().expect("begin transaction");
			tx.execute("insert into txlog(value) values ('rolled')").expect("insert rolled row");
		}

		let rolled = db.query_row("select count(*) from txlog").expect("count rows after rollback");
		assert_eq!(rolled, Some(vec![Value::Integer(0)]));

		let tx = db.transaction().expect("begin transaction");
		tx.execute("insert into txlog(value) values ('committed')").expect("insert committed row");
		tx.commit().expect("commit transaction");

		let committed = db.query_row("select count(*) from txlog").expect("count rows after commit");
		assert_eq!(committed, Some(vec![Value::Integer(1)]));
	}

	#[test]
	fn transaction_with_mode_starts_requested_kind() {
		let db = Connection::open_in_memory().expect("open memory database");
		let tx =
			db.transaction_with_mode(TransactionMode::Deferred).expect("begin deferred transaction");
		assert!(tx.is_active());
		tx.rollback().expect("rollback deferred transaction");
	}

	#[test]
	fn session_captures_changeset() {
		if !sqlite_compile_option_used("ENABLE_SESSION") {
			return;
		}

		let db = Connection::open_in_memory().expect("open memory database");
		db.execute("create table logs(id integer primary key, value text)").expect("create table");

		let mut session = db.create_session("main").expect("create session");
		session.attach(Some("logs")).expect("attach table");

		db.execute("insert into logs(value) values ('alpha'), ('beta')").expect("insert rows");

		assert!(!session.is_empty());
		assert!(!session.changeset().expect("get changeset").is_empty());
		assert!(!session.patchset().expect("get patchset").is_empty());
	}
}
