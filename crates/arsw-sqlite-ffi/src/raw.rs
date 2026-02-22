use core::ffi::{c_char, c_int, c_void};

pub use crate::ffi::*;

pub const SQLITE_CARRAY_INT32: c_int = 0;
pub const SQLITE_CARRAY_INT64: c_int = 1;
pub const SQLITE_CARRAY_DOUBLE: c_int = 2;
pub const SQLITE_CARRAY_TEXT: c_int = 3;
pub const SQLITE_CARRAY_BLOB: c_int = 4;

pub const SQLITE_SESSION_OBJCONFIG_SIZE: c_int = 1;
pub const SQLITE_SESSION_OBJCONFIG_ROWID: c_int = 2;

unsafe extern "C" {
	pub fn sqlite3_carray_bind(
		p_stmt: *mut Sqlite3Stmt,
		i: c_int,
		a_data: *mut c_void,
		n_data: c_int,
		m_flags: c_int,
		x_del: Sqlite3DestructorType,
	) -> c_int;

	pub fn sqlite3_preupdate_blobwrite(db: *mut Sqlite3) -> c_int;

	pub fn sqlite3session_object_config(
		p_session: *mut Sqlite3Session,
		op: c_int,
		p_arg: *mut c_void,
	) -> c_int;

	pub fn sqlite3session_changeset_size(p_session: *mut Sqlite3Session) -> Sqlite3Int64;

	pub fn sqlite3session_memory_used(p_session: *mut Sqlite3Session) -> Sqlite3Int64;

	pub fn sqlite3changegroup_schema(
		group: *mut Sqlite3Changegroup,
		db: *mut Sqlite3,
		z_db: *const c_char,
	) -> c_int;

	pub fn sqlite3changegroup_add_change(
		group: *mut Sqlite3Changegroup,
		iter: *mut Sqlite3ChangesetIter,
	) -> c_int;
}
