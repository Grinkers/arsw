use super::*;

pyo3::create_exception!(apsw, Error, pyo3::exceptions::PyException);
pyo3::create_exception!(apsw, AbortError, Error);
pyo3::create_exception!(apsw, AuthError, Error);
pyo3::create_exception!(apsw, BindingsError, Error);
pyo3::create_exception!(apsw, BusyError, Error);
pyo3::create_exception!(apsw, CantOpenError, Error);
pyo3::create_exception!(apsw, ConnectionClosedError, Error);
pyo3::create_exception!(apsw, ConnectionNotClosedError, Error);
pyo3::create_exception!(apsw, ConstraintError, Error);
pyo3::create_exception!(apsw, CorruptError, Error);
pyo3::create_exception!(apsw, CursorClosedError, Error);
pyo3::create_exception!(apsw, EmptyError, Error);
pyo3::create_exception!(apsw, ExecTraceAbort, Error);
pyo3::create_exception!(apsw, ExecutionCompleteError, Error);
pyo3::create_exception!(apsw, ExtensionLoadingError, Error);
pyo3::create_exception!(apsw, ForkingViolationError, Error);
pyo3::create_exception!(apsw, FormatError, Error);
pyo3::create_exception!(apsw, FullError, Error);
pyo3::create_exception!(apsw, IOError, Error);
pyo3::create_exception!(apsw, IncompleteExecutionError, Error);
pyo3::create_exception!(apsw, InternalError, Error);
pyo3::create_exception!(apsw, InterruptError, Error);
pyo3::create_exception!(apsw, InvalidContextError, Error);
pyo3::create_exception!(apsw, LockedError, Error);
pyo3::create_exception!(apsw, MismatchError, Error);
pyo3::create_exception!(apsw, MisuseError, Error);
pyo3::create_exception!(apsw, NoFTS5Error, Error);
pyo3::create_exception!(apsw, NoLFSError, Error);
pyo3::create_exception!(apsw, NoMemError, Error);
pyo3::create_exception!(apsw, NotADBError, Error);
pyo3::create_exception!(apsw, NotFoundError, Error);
pyo3::create_exception!(apsw, PermissionsError, Error);
pyo3::create_exception!(apsw, ProtocolError, Error);
pyo3::create_exception!(apsw, RangeError, Error);
pyo3::create_exception!(apsw, ReadOnlyError, Error);
pyo3::create_exception!(apsw, SQLError, Error);
pyo3::create_exception!(apsw, SchemaChangeError, Error);
pyo3::create_exception!(apsw, ThreadingViolationError, Error);
pyo3::create_exception!(apsw, TooBigError, Error);
pyo3::create_exception!(apsw, VFSFileClosedError, Error);
pyo3::create_exception!(apsw, VFSNotImplementedError, Error);

macro_rules! add_exception {
	($module:expr, $py:expr, $name:ident) => {
		$module.add(stringify!($name), $py.get_type::<$name>())?;
	};
}

pub(crate) fn add_module_exceptions(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
	add_exception!(m, py, Error);
	add_exception!(m, py, AbortError);
	add_exception!(m, py, AuthError);
	add_exception!(m, py, BindingsError);
	add_exception!(m, py, BusyError);
	add_exception!(m, py, CantOpenError);
	add_exception!(m, py, ConnectionClosedError);
	add_exception!(m, py, ConnectionNotClosedError);
	add_exception!(m, py, ConstraintError);
	add_exception!(m, py, CorruptError);
	add_exception!(m, py, CursorClosedError);
	add_exception!(m, py, EmptyError);
	add_exception!(m, py, ExecTraceAbort);
	add_exception!(m, py, ExecutionCompleteError);
	add_exception!(m, py, ExtensionLoadingError);
	add_exception!(m, py, ForkingViolationError);
	add_exception!(m, py, FormatError);
	add_exception!(m, py, FullError);
	add_exception!(m, py, IOError);
	add_exception!(m, py, IncompleteExecutionError);
	add_exception!(m, py, InternalError);
	add_exception!(m, py, InterruptError);
	add_exception!(m, py, InvalidContextError);
	add_exception!(m, py, LockedError);
	add_exception!(m, py, MismatchError);
	add_exception!(m, py, MisuseError);
	add_exception!(m, py, NoFTS5Error);
	add_exception!(m, py, NoLFSError);
	add_exception!(m, py, NoMemError);
	add_exception!(m, py, NotADBError);
	add_exception!(m, py, NotFoundError);
	add_exception!(m, py, PermissionsError);
	add_exception!(m, py, ProtocolError);
	add_exception!(m, py, RangeError);
	add_exception!(m, py, ReadOnlyError);
	add_exception!(m, py, SQLError);
	add_exception!(m, py, SchemaChangeError);
	add_exception!(m, py, ThreadingViolationError);
	add_exception!(m, py, TooBigError);
	add_exception!(m, py, VFSFileClosedError);
	add_exception!(m, py, VFSNotImplementedError);
	Ok(())
}
