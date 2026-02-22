use core::ffi::{c_char, c_int, c_uchar, c_void};

pub type Sqlite3Int64 = i64;
pub type Sqlite3UInt64 = u64;
pub type Sqlite3DestructorType = Option<unsafe extern "C" fn(*mut c_void)>;

pub type Sqlite3Callback =
	Option<unsafe extern "C" fn(*mut Sqlite3Context, c_int, *mut *mut Sqlite3Value)>;
pub type Sqlite3StepCallback = Sqlite3Callback;
pub type Sqlite3FinalCallback = Option<unsafe extern "C" fn(*mut Sqlite3Context)>;
pub type Sqlite3ValueCallback = Sqlite3FinalCallback;
pub type Sqlite3InverseCallback = Sqlite3Callback;
pub type Sqlite3CompareCallback =
	Option<unsafe extern "C" fn(*mut c_void, c_int, *const c_void, c_int, *const c_void) -> c_int>;
pub type Sqlite3SessionOutputCallback =
	Option<unsafe extern "C" fn(*mut c_void, *const c_void, c_int) -> c_int>;
pub type Sqlite3SessionFilterCallback =
	Option<unsafe extern "C" fn(*mut c_void, *const c_char) -> c_int>;
pub type Sqlite3TraceCallback =
	Option<unsafe extern "C" fn(c_int, *mut c_void, *mut c_void, *mut c_void) -> c_int>;

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct Sqlite3 {
	_unused: [u8; 0],
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct Sqlite3Stmt {
	_unused: [u8; 0],
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct Sqlite3Blob {
	_unused: [u8; 0],
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct Sqlite3Backup {
	_unused: [u8; 0],
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct Sqlite3Context {
	_unused: [u8; 0],
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct Sqlite3Value {
	_unused: [u8; 0],
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct Sqlite3Vfs {
	pub i_version: c_int,
	pub sz_os_file: c_int,
	pub mx_pathname: c_int,
	pub p_next: *mut Sqlite3Vfs,
	pub z_name: *const c_char,
	pub p_app_data: *mut c_void,
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct Sqlite3Session {
	_unused: [u8; 0],
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct Sqlite3ChangesetIter {
	_unused: [u8; 0],
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct Sqlite3Changegroup {
	_unused: [u8; 0],
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct Sqlite3Rebaser {
	_unused: [u8; 0],
}

unsafe extern "C" {
	pub fn sqlite3_libversion_number() -> c_int;
	pub fn sqlite3_libversion() -> *const c_char;
	pub fn sqlite3_sourceid() -> *const c_char;
	pub fn sqlite3_complete(sql: *const c_char) -> c_int;
	pub fn sqlite3_compileoption_used(name: *const c_char) -> c_int;
	pub fn sqlite3_compileoption_get(n: c_int) -> *const c_char;
	pub fn sqlite3_open_v2(
		filename: *const c_char,
		pp_db: *mut *mut Sqlite3,
		flags: c_int,
		z_vfs: *const c_char,
	) -> c_int;
	pub fn sqlite3_close_v2(db: *mut Sqlite3) -> c_int;
	pub fn sqlite3_changes64(db: *mut Sqlite3) -> Sqlite3Int64;
	pub fn sqlite3_total_changes64(db: *mut Sqlite3) -> Sqlite3Int64;
	pub fn sqlite3_db_filename(db: *mut Sqlite3, z_db_name: *const c_char) -> *const c_char;
	pub fn sqlite3_db_readonly(db: *mut Sqlite3, z_db_name: *const c_char) -> c_int;
	pub fn sqlite3_errmsg(db: *mut Sqlite3) -> *const c_char;
	pub fn sqlite3_errcode(db: *mut Sqlite3) -> c_int;
	pub fn sqlite3_error_offset(db: *mut Sqlite3) -> c_int;
	pub fn sqlite3_extended_errcode(db: *mut Sqlite3) -> c_int;
	pub fn sqlite3_extended_result_codes(db: *mut Sqlite3, onoff: c_int) -> c_int;
	pub fn sqlite3_prepare_v3(
		db: *mut Sqlite3,
		z_sql: *const c_char,
		n_byte: c_int,
		prep_flags: u32,
		pp_stmt: *mut *mut Sqlite3Stmt,
		pz_tail: *mut *const c_char,
	) -> c_int;
	pub fn sqlite3_next_stmt(db: *mut Sqlite3, p_stmt: *mut Sqlite3Stmt) -> *mut Sqlite3Stmt;
	pub fn sqlite3_step(p_stmt: *mut Sqlite3Stmt) -> c_int;
	pub fn sqlite3_finalize(p_stmt: *mut Sqlite3Stmt) -> c_int;
	pub fn sqlite3_reset(p_stmt: *mut Sqlite3Stmt) -> c_int;
	pub fn sqlite3_bind_parameter_count(p_stmt: *mut Sqlite3Stmt) -> c_int;
	pub fn sqlite3_bind_parameter_name(p_stmt: *mut Sqlite3Stmt, i: c_int) -> *const c_char;
	pub fn sqlite3_bind_parameter_index(p_stmt: *mut Sqlite3Stmt, z_name: *const c_char) -> c_int;
	pub fn sqlite3_bind_null(p_stmt: *mut Sqlite3Stmt, i: c_int) -> c_int;
	pub fn sqlite3_bind_int64(p_stmt: *mut Sqlite3Stmt, i: c_int, value: Sqlite3Int64) -> c_int;
	pub fn sqlite3_bind_double(p_stmt: *mut Sqlite3Stmt, i: c_int, value: f64) -> c_int;
	pub fn sqlite3_bind_text64(
		p_stmt: *mut Sqlite3Stmt,
		i: c_int,
		value: *const c_char,
		n: Sqlite3UInt64,
		destructor: Sqlite3DestructorType,
		encoding: c_uchar,
	) -> c_int;
	pub fn sqlite3_bind_blob64(
		p_stmt: *mut Sqlite3Stmt,
		i: c_int,
		value: *const c_void,
		n: Sqlite3UInt64,
		destructor: Sqlite3DestructorType,
	) -> c_int;
	pub fn sqlite3_column_count(p_stmt: *mut Sqlite3Stmt) -> c_int;
	pub fn sqlite3_column_name(p_stmt: *mut Sqlite3Stmt, i_col: c_int) -> *const c_char;
	pub fn sqlite3_column_decltype(p_stmt: *mut Sqlite3Stmt, i_col: c_int) -> *const c_char;
	pub fn sqlite3_column_database_name(p_stmt: *mut Sqlite3Stmt, i_col: c_int) -> *const c_char;
	pub fn sqlite3_column_table_name(p_stmt: *mut Sqlite3Stmt, i_col: c_int) -> *const c_char;
	pub fn sqlite3_column_origin_name(p_stmt: *mut Sqlite3Stmt, i_col: c_int) -> *const c_char;
	pub fn sqlite3_column_type(p_stmt: *mut Sqlite3Stmt, i_col: c_int) -> c_int;
	pub fn sqlite3_column_int64(p_stmt: *mut Sqlite3Stmt, i_col: c_int) -> Sqlite3Int64;
	pub fn sqlite3_column_double(p_stmt: *mut Sqlite3Stmt, i_col: c_int) -> f64;
	pub fn sqlite3_column_text(p_stmt: *mut Sqlite3Stmt, i_col: c_int) -> *const c_uchar;
	pub fn sqlite3_column_blob(p_stmt: *mut Sqlite3Stmt, i_col: c_int) -> *const c_void;
	pub fn sqlite3_column_bytes(p_stmt: *mut Sqlite3Stmt, i_col: c_int) -> c_int;
	pub fn sqlite3_sql(p_stmt: *mut Sqlite3Stmt) -> *const c_char;
	pub fn sqlite3_backup_init(
		p_dest: *mut Sqlite3,
		z_dest_name: *const c_char,
		p_source: *mut Sqlite3,
		z_source_name: *const c_char,
	) -> *mut Sqlite3Backup;
	pub fn sqlite3_backup_step(p: *mut Sqlite3Backup, n_page: c_int) -> c_int;
	pub fn sqlite3_backup_finish(p: *mut Sqlite3Backup) -> c_int;
	pub fn sqlite3_backup_remaining(p: *mut Sqlite3Backup) -> c_int;
	pub fn sqlite3_backup_pagecount(p: *mut Sqlite3Backup) -> c_int;
	pub fn sqlite3_blob_open(
		db: *mut Sqlite3,
		z_db: *const c_char,
		z_table: *const c_char,
		z_column: *const c_char,
		i_row: Sqlite3Int64,
		flags: c_int,
		pp_blob: *mut *mut Sqlite3Blob,
	) -> c_int;
	pub fn sqlite3_blob_close(blob: *mut Sqlite3Blob) -> c_int;
	pub fn sqlite3_blob_bytes(blob: *mut Sqlite3Blob) -> c_int;
	pub fn sqlite3_blob_read(
		blob: *mut Sqlite3Blob,
		z: *mut c_void,
		n: c_int,
		i_offset: c_int,
	) -> c_int;
	pub fn sqlite3_blob_write(
		blob: *mut Sqlite3Blob,
		z: *const c_void,
		n: c_int,
		i_offset: c_int,
	) -> c_int;
	pub fn sqlite3_blob_reopen(blob: *mut Sqlite3Blob, i_row: Sqlite3Int64) -> c_int;
	pub fn sqlite3_create_function_v2(
		db: *mut Sqlite3,
		z_function_name: *const c_char,
		n_arg: c_int,
		e_text_rep: c_int,
		p_app: *mut c_void,
		x_func: Sqlite3Callback,
		x_step: Sqlite3StepCallback,
		x_final: Sqlite3FinalCallback,
		x_destroy: Sqlite3DestructorType,
	) -> c_int;
	pub fn sqlite3_create_window_function(
		db: *mut Sqlite3,
		z_function_name: *const c_char,
		n_arg: c_int,
		e_text_rep: c_int,
		p_app: *mut c_void,
		x_step: Sqlite3StepCallback,
		x_final: Sqlite3FinalCallback,
		x_value: Sqlite3ValueCallback,
		x_inverse: Sqlite3InverseCallback,
		x_destroy: Sqlite3DestructorType,
	) -> c_int;
	pub fn sqlite3_create_collation_v2(
		db: *mut Sqlite3,
		z_name: *const c_char,
		e_text_rep: c_int,
		p_arg: *mut c_void,
		x_compare: Sqlite3CompareCallback,
		x_destroy: Sqlite3DestructorType,
	) -> c_int;
	pub fn sqlite3_user_data(context: *mut Sqlite3Context) -> *mut c_void;
	pub fn sqlite3_aggregate_context(context: *mut Sqlite3Context, n_bytes: c_int) -> *mut c_void;
	pub fn sqlite3_result_null(context: *mut Sqlite3Context);
	pub fn sqlite3_result_int64(context: *mut Sqlite3Context, value: Sqlite3Int64);
	pub fn sqlite3_result_double(context: *mut Sqlite3Context, value: f64);
	pub fn sqlite3_result_text64(
		context: *mut Sqlite3Context,
		value: *const c_char,
		n: Sqlite3UInt64,
		destructor: Sqlite3DestructorType,
		encoding: c_uchar,
	);
	pub fn sqlite3_result_blob64(
		context: *mut Sqlite3Context,
		value: *const c_void,
		n: Sqlite3UInt64,
		destructor: Sqlite3DestructorType,
	);
	pub fn sqlite3_result_error(context: *mut Sqlite3Context, value: *const c_char, n: c_int);
	pub fn sqlite3_value_type(value: *mut Sqlite3Value) -> c_int;
	pub fn sqlite3_value_int64(value: *mut Sqlite3Value) -> Sqlite3Int64;
	pub fn sqlite3_value_double(value: *mut Sqlite3Value) -> f64;
	pub fn sqlite3_value_text(value: *mut Sqlite3Value) -> *const c_uchar;
	pub fn sqlite3_value_blob(value: *mut Sqlite3Value) -> *const c_void;
	pub fn sqlite3_value_bytes(value: *mut Sqlite3Value) -> c_int;
	pub fn sqlite3_free(p: *mut c_void);
	pub fn sqlite3_vfs_find(z_vfs_name: *const c_char) -> *mut Sqlite3Vfs;
	pub fn sqlite3session_create(
		db: *mut Sqlite3,
		z_db: *const c_char,
		pp_session: *mut *mut Sqlite3Session,
	) -> c_int;
	pub fn sqlite3session_delete(p_session: *mut Sqlite3Session);
	pub fn sqlite3session_attach(p_session: *mut Sqlite3Session, z_tab: *const c_char) -> c_int;
	pub fn sqlite3session_changeset(
		p_session: *mut Sqlite3Session,
		pn_changeset: *mut c_int,
		pp_changeset: *mut *mut c_void,
	) -> c_int;
	pub fn sqlite3session_patchset(
		p_session: *mut Sqlite3Session,
		pn_patchset: *mut c_int,
		pp_patchset: *mut *mut c_void,
	) -> c_int;
	pub fn sqlite3session_enable(p_session: *mut Sqlite3Session, b_enable: c_int) -> c_int;
	pub fn sqlite3session_indirect(p_session: *mut Sqlite3Session, b_indirect: c_int) -> c_int;
	pub fn sqlite3session_isempty(p_session: *mut Sqlite3Session) -> c_int;
	pub fn sqlite3session_table_filter(
		p_session: *mut Sqlite3Session,
		x_filter: Sqlite3SessionFilterCallback,
		p_ctx: *mut c_void,
	);
	pub fn sqlite3session_changeset_strm(
		p_session: *mut Sqlite3Session,
		x_output: Sqlite3SessionOutputCallback,
		p_out: *mut c_void,
	) -> c_int;
	pub fn sqlite3session_patchset_strm(
		p_session: *mut Sqlite3Session,
		x_output: Sqlite3SessionOutputCallback,
		p_out: *mut c_void,
	) -> c_int;
	pub fn sqlite3session_diff(
		db: *mut Sqlite3,
		z_from: *const c_char,
		z_tbl: *const c_char,
		pz_err_msg: *mut *mut c_char,
	) -> c_int;
	pub fn sqlite3session_config(op: c_int, p_arg: *mut c_void) -> c_int;
	pub fn sqlite3rebaser_create(pp_rebaser: *mut *mut Sqlite3Rebaser) -> c_int;
	pub fn sqlite3rebaser_delete(p_rebaser: *mut Sqlite3Rebaser);
	pub fn sqlite3rebaser_configure(
		p_rebaser: *mut Sqlite3Rebaser,
		n_rebase: c_int,
		p_rebase: *const c_void,
	) -> c_int;
	pub fn sqlite3rebaser_rebase(
		p_rebaser: *mut Sqlite3Rebaser,
		n_in: c_int,
		p_in: *const c_void,
		pn_out: *mut c_int,
		pp_out: *mut *mut c_void,
	) -> c_int;
	pub fn sqlite3_trace_v2(
		db: *mut Sqlite3,
		mask: c_int,
		callback: Sqlite3TraceCallback,
		p_ctx: *mut c_void,
	) -> c_int;
	pub fn sqlite3_stmt_readonly(p_stmt: *mut Sqlite3Stmt) -> c_int;
	pub fn sqlite3_stmt_isexplain(p_stmt: *mut Sqlite3Stmt) -> c_int;
	pub fn sqlite3_stmt_status(p_stmt: *mut Sqlite3Stmt, op: c_int, reset: c_int) -> c_int;
}
