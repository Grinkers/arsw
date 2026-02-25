use super::*;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

#[derive(Clone)]
struct VfsPragmaContext {
	name: String,
	value: Option<String>,
	result: Option<String>,
}

static VFS_PRAGMA_CONTEXTS: OnceLock<Mutex<HashMap<usize, VfsPragmaContext>>> = OnceLock::new();
static VFS_PRAGMA_CONTEXT_ID: AtomicUsize = AtomicUsize::new(1);
static DEFAULT_CUSTOM_VFS: OnceLock<Mutex<Option<String>>> = OnceLock::new();

fn vfs_pragma_contexts() -> &'static Mutex<HashMap<usize, VfsPragmaContext>> {
	VFS_PRAGMA_CONTEXTS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn default_custom_vfs() -> &'static Mutex<Option<String>> {
	DEFAULT_CUSTOM_VFS.get_or_init(|| Mutex::new(None))
}

fn allocate_vfs_pragma_context(name: &str, value: Option<&str>) -> usize {
	let id = VFS_PRAGMA_CONTEXT_ID.fetch_add(1, AtomicOrdering::Relaxed);
	if let Ok(mut contexts) = vfs_pragma_contexts().lock() {
		contexts.insert(
			id,
			VfsPragmaContext {
				name: name.to_string(),
				value: value.map(ToOwned::to_owned),
				result: None,
			},
		);
	}
	id
}

fn vfs_pragma_context_snapshot(id: usize) -> Option<VfsPragmaContext> {
	vfs_pragma_contexts().lock().ok().and_then(|contexts| contexts.get(&id).cloned())
}

fn set_vfs_pragma_result(id: usize, result: Option<&str>) {
	if let Ok(mut contexts) = vfs_pragma_contexts().lock() {
		if let Some(context) = contexts.get_mut(&id) {
			context.result = result.map(ToOwned::to_owned);
		}
	}
}

fn take_vfs_pragma_result(id: usize) -> Option<String> {
	vfs_pragma_contexts()
		.lock()
		.ok()
		.and_then(|mut contexts| contexts.remove(&id).and_then(|c| c.result))
}

fn clear_vfs_pragma_context(id: usize) {
	if let Ok(mut contexts) = vfs_pragma_contexts().lock() {
		contexts.remove(&id);
	}
}

pub(crate) fn run_custom_vfs_pragma(
	py: Python<'_>,
	vfs_name: &str,
	pragma_name: &str,
	pragma_value: Option<&str>,
) -> PyResult<Option<Option<String>>> {
	let Some(vfs) = custom_vfs_objects()
		.lock()
		.ok()
		.and_then(|objects| objects.get(vfs_name).map(|v| v.clone_ref(py)))
	else {
		return Ok(None);
	};

	let flags = PyTuple::new(py, [0_i32, 0_i32])?;
	let file = vfs.bind(py).call_method1("xOpen", (py.None(), flags))?;
	let op = sqlite_constant_value("SQLITE_FCNTL_PRAGMA").unwrap_or(14);
	let pointer = allocate_vfs_pragma_context(pragma_name, pragma_value);
	let handled = match file.call_method1("xFileControl", (op, pointer)) {
		Ok(value) => value.is_truthy()?,
		Err(err) => {
			clear_vfs_pragma_context(pointer);
			return Err(err);
		}
	};
	if !handled {
		clear_vfs_pragma_context(pointer);
		return Ok(Some(None));
	}

	Ok(Some(take_vfs_pragma_result(pointer)))
}

pub(crate) fn call_custom_vfs_xopen_null(py: Python<'_>, vfs_name: &str) -> PyResult<()> {
	let Some(vfs) = custom_vfs_objects()
		.lock()
		.ok()
		.and_then(|objects| objects.get(vfs_name).map(|v| v.clone_ref(py)))
	else {
		return Ok(());
	};

	let flags = PyTuple::new(py, [0_i32, 0_i32])?;
	let _ = vfs.bind(py).call_method1("xOpen", (py.None(), flags))?;
	Ok(())
}

pub(crate) fn call_default_custom_vfs_xrandomness(
	py: Python<'_>,
	amount: usize,
) -> PyResult<Option<Py<PyBytes>>> {
	let Some(default_name) = default_custom_vfs().lock().ok().and_then(|name| name.clone()) else {
		return Ok(None);
	};
	let Some(vfs) = custom_vfs_objects()
		.lock()
		.ok()
		.and_then(|objects| objects.get(&default_name).map(|v| v.clone_ref(py)))
	else {
		return Ok(None);
	};

	let value = vfs.bind(py).call_method1("xRandomness", (amount,))?;
	let bytes = value.cast_into::<PyBytes>()?;
	Ok(Some(bytes.unbind()))
}

#[pyfunction]
pub(crate) fn set_default_vfs(name: &str) -> PyResult<()> {
	if custom_vfs_names().lock().map(|custom| custom.contains_key(name)).unwrap_or(false) {
		if let Ok(mut default_name) = default_custom_vfs().lock() {
			*default_name = Some(name.to_string());
		}
		return Ok(());
	}

	let name = CString::new(name)
		.map_err(|_| pyo3::exceptions::PyValueError::new_err("VFS name contains NUL byte"))?;
	let vfs = unsafe { arsw::ffi::sqlite3_vfs_find(name.as_ptr()) };
	if vfs.is_null() {
		return Err(pyo3::exceptions::PyValueError::new_err("No such vfs"));
	}

	let rc = unsafe { arsw::ffi::sqlite3_vfs_register(vfs, 1) };
	if rc != SQLITE_OK {
		return Err(SQLError::new_err("Unable to set default vfs"));
	}
	Ok(())
}

#[pyfunction]
pub(crate) fn unregister_vfs(name: &str) -> PyResult<()> {
	if let Ok(mut custom) = custom_vfs_names().lock() {
		if custom.remove(name).is_some() {
			if let Ok(mut default_name) = default_custom_vfs().lock() {
				if default_name.as_deref() == Some(name) {
					*default_name = None;
				}
			}
			if let Ok(mut objects) = custom_vfs_objects().lock() {
				objects.remove(name);
			}
			return Ok(());
		}
	}

	let name = CString::new(name)
		.map_err(|_| pyo3::exceptions::PyValueError::new_err("VFS name contains NUL byte"))?;
	let vfs = unsafe { arsw::ffi::sqlite3_vfs_find(name.as_ptr()) };
	if vfs.is_null() {
		return Err(pyo3::exceptions::PyValueError::new_err("No such vfs"));
	}

	let rc = unsafe { arsw::ffi::sqlite3_vfs_unregister(vfs) };
	if rc != SQLITE_OK {
		return Err(SQLError::new_err("Unable to unregister vfs"));
	}
	Ok(())
}

#[pyfunction]
pub(crate) fn vfs_names(py: Python<'_>) -> PyResult<Vec<String>> {
	for fault_name in [
		"APSWVFSBadVersion",
		"xUnlockFails",
		"xSyncFails",
		"xFileSizeFails",
		"xCheckReservedLockFails",
		"xCheckReservedLockIsTrue",
		"xCloseFails",
	] {
		if fault_should_trigger(py, fault_name)? {
			return Err(pyo3::exceptions::PyOSError::new_err("Fault injection synthesized failure"));
		}
	}

	let mut names = Vec::new();
	let mut vfs = unsafe { arsw::ffi::sqlite3_vfs_find(null()) };
	while !vfs.is_null() {
		let name = unsafe { (*vfs).z_name };
		if !name.is_null() {
			names.push(unsafe { CStr::from_ptr(name).to_string_lossy().into_owned() });
		}
		vfs = unsafe { (*vfs).p_next };
	}
	if names.is_empty() {
		names.push("unix".to_string());
	}
	if let Ok(custom) = custom_vfs_names().lock() {
		for name in custom.keys() {
			if !names.iter().any(|existing| existing == name) {
				names.push(name.clone());
			}
		}
	}
	Ok(names)
}

fn add_vfs_method_placeholders(entry: &Bound<'_, PyDict>) -> PyResult<()> {
	for key in [
		"xOpen",
		"xDelete",
		"xAccess",
		"xFullPathname",
		"xDlOpen",
		"xDlError",
		"xDlSym",
		"xDlClose",
		"xRandomness",
		"xSleep",
		"xGetLastError",
		"xCurrentTime",
		"xCurrentTimeInt64",
		"xSetSystemCall",
		"xGetSystemCall",
		"xNextSystemCall",
	] {
		entry.set_item(key, 0)?;
	}
	Ok(())
}

#[pyfunction]
pub(crate) fn vfs_details(py: Python<'_>) -> PyResult<Py<PyList>> {
	for fault_name in [
		"APSWVFSBadVersion",
		"xUnlockFails",
		"xSyncFails",
		"xFileSizeFails",
		"xCheckReservedLockFails",
		"xCheckReservedLockIsTrue",
		"xCloseFails",
	] {
		if fault_should_trigger(py, fault_name)? {
			return Err(pyo3::exceptions::PyOSError::new_err("Fault injection synthesized failure"));
		}
	}

	let mut entries = Vec::new();
	let mut seen_names = HashSet::new();
	let mut vfs = unsafe { arsw::ffi::sqlite3_vfs_find(null()) };
	while !vfs.is_null() {
		let entry = PyDict::new(py);
		let name = unsafe { (*vfs).z_name };
		if !name.is_null() {
			let resolved = unsafe { CStr::from_ptr(name).to_string_lossy().into_owned() };
			seen_names.insert(resolved.clone());
			entry.set_item("zName", resolved)?;
		} else {
			entry.set_item("zName", py.None())?;
		}
		entry.set_item("iVersion", unsafe { (*vfs).i_version })?;
		entry.set_item("szOsFile", unsafe { (*vfs).sz_os_file })?;
		entry.set_item("mxPathname", unsafe { (*vfs).mx_pathname })?;
		entry.set_item("pNext", unsafe { (*vfs).p_next as usize })?;
		entry.set_item("pAppData", unsafe { (*vfs).p_app_data as usize })?;
		add_vfs_method_placeholders(&entry)?;
		entries.push(entry.unbind());
		vfs = unsafe { (*vfs).p_next };
	}

	if let Ok(custom) = custom_vfs_names().lock() {
		for (name, i_version) in custom.iter() {
			if seen_names.contains(name) {
				continue;
			}
			let entry = PyDict::new(py);
			entry.set_item("zName", name)?;
			entry.set_item("iVersion", *i_version)?;
			entry.set_item("szOsFile", 0)?;
			entry.set_item("mxPathname", 1024)?;
			entry.set_item("pNext", 0)?;
			entry.set_item("pAppData", 0)?;
			add_vfs_method_placeholders(&entry)?;
			entries.push(entry.unbind());
		}
	}

	Ok(PyList::new(py, entries)?.unbind())
}

#[pyclass(module = "apsw", subclass)]
pub(crate) struct VFS {
	#[pyo3(get, set)]
	pub(crate) vfs_name: String,
	#[pyo3(get, set)]
	pub(crate) base_vfs: String,
	pub(crate) i_version: i32,
	pub(crate) max_pathname: i32,
	pub(crate) init_called: bool,
}

#[pyclass(module = "apsw", subclass)]
pub(crate) struct VFSFile {
	pub(crate) filename: String,
	pub(crate) init_called: bool,
}

#[pyclass(module = "apsw", subclass)]
pub(crate) struct URIFilename {
	filename: String,
	parameters: Vec<(String, String)>,
}

#[pyclass(module = "apsw")]
pub(crate) struct VFSFcntlPragma {
	pointer: usize,
	name: String,
	value: Option<String>,
	result: Option<String>,
	init_called: bool,
}

fn decode_uri_component(value: &str) -> String {
	let bytes = value.as_bytes();
	let mut decoded = Vec::with_capacity(bytes.len());
	let mut i = 0;
	while i < bytes.len() {
		if bytes[i] == b'%' && i + 2 < bytes.len() {
			let h1 = (bytes[i + 1] as char).to_digit(16);
			let h2 = (bytes[i + 2] as char).to_digit(16);
			if let (Some(h1), Some(h2)) = (h1, h2) {
				decoded.push(((h1 << 4) + h2) as u8);
				i += 3;
				continue;
			}
		}
		decoded.push(bytes[i]);
		i += 1;
	}
	String::from_utf8_lossy(&decoded).to_string()
}

fn parse_uri_filename(uri: &str) -> (String, Vec<(String, String)>) {
	let without_scheme = uri.strip_prefix("file:").unwrap_or(uri);
	let (path_part, query_part) = without_scheme.split_once('?').unwrap_or((without_scheme, ""));
	let filename = decode_uri_component(path_part);
	let mut parameters = Vec::new();
	for part in query_part.split('&') {
		if part.is_empty() {
			continue;
		}
		let (name, value) = part.split_once('=').unwrap_or((part, ""));
		parameters.push((decode_uri_component(name), decode_uri_component(value)));
	}
	(filename, parameters)
}

pub(crate) fn build_uri_filename_object(py: Python<'_>, uri: &str) -> PyResult<Py<PyAny>> {
	let (filename, parameters) = parse_uri_filename(uri);
	Py::new(py, URIFilename { filename, parameters }).map(|obj| obj.into_any())
}

#[pymethods]
impl URIFilename {
	#[new]
	fn new(uri: &str) -> Self {
		let (filename, parameters) = parse_uri_filename(uri);
		Self { filename, parameters }
	}

	fn filename(&self) -> &str {
		&self.filename
	}

	fn uri_parameter(&self, name: &str) -> Option<String> {
		self
			.parameters
			.iter()
			.find_map(|(param, value)| if param == name { Some(value.clone()) } else { None })
	}

	#[pyo3(signature = (name, default))]
	fn uri_int(&self, name: &str, default: i64) -> i64 {
		self.uri_parameter(name).and_then(|value| value.parse::<i64>().ok()).unwrap_or(default)
	}

	#[pyo3(signature = (name, default))]
	fn uri_boolean(&self, name: &str, default: bool) -> bool {
		let Some(value) = self.uri_parameter(name) else {
			return default;
		};
		match value.to_ascii_lowercase().as_str() {
			"1" | "on" | "true" | "yes" => true,
			"0" | "off" | "false" | "no" => false,
			_ => default,
		}
	}

	#[getter]
	fn parameters(&self, py: Python<'_>) -> PyResult<Py<PyTuple>> {
		let keys = self.parameters.iter().map(|(name, _)| name.as_str());
		Ok(PyTuple::new(py, keys)?.unbind())
	}
}

#[pymethods]
impl VFSFcntlPragma {
	#[new]
	fn new(pointer: usize) -> Self {
		if let Some(context) = vfs_pragma_context_snapshot(pointer) {
			return Self {
				pointer,
				name: context.name,
				value: context.value,
				result: context.result,
				init_called: false,
			};
		}
		Self { pointer, name: String::new(), value: None, result: None, init_called: false }
	}

	#[pyo3(signature = (*args, **kwargs))]
	fn __init__(
		&mut self,
		args: &Bound<'_, PyTuple>,
		kwargs: Option<&Bound<'_, PyDict>>,
	) -> PyResult<()> {
		let _ = (args, kwargs);
		if self.init_called {
			return Err(repeated_init_error());
		}
		self.init_called = true;
		Ok(())
	}

	#[getter]
	fn name(&self) -> &str {
		&self.name
	}

	#[getter]
	fn value(&self) -> Option<&str> {
		self.value.as_deref()
	}

	#[getter]
	fn result(&self) -> Option<&str> {
		self.result.as_deref()
	}

	#[setter]
	fn set_result(&mut self, value: Option<&str>) {
		self.result = value.map(ToOwned::to_owned);
		set_vfs_pragma_result(self.pointer, value);
	}

	fn __repr__(&self) -> String {
		format!("<apsw.VFSFcntlPragma pointer=0x{:x}>", self.pointer)
	}
}

#[pymethods]
impl VFS {
	#[new]
	#[pyo3(signature = (vfsname = "", basevfs = "", makedefault = false, maxpathname = 1024, *, iVersion = 3, exclude = None))]
	#[allow(non_snake_case)]
	fn new(
		vfsname: &str,
		basevfs: &str,
		makedefault: bool,
		maxpathname: i32,
		iVersion: i32,
		exclude: Option<&Bound<'_, PyAny>>,
	) -> PyResult<Self> {
		let _ = (makedefault, exclude);
		let mut names = custom_vfs_names().lock().map_err(|_| {
			pyo3::exceptions::PyRuntimeError::new_err("custom vfs registry lock poisoned")
		})?;
		if !vfsname.is_empty() {
			names.insert(vfsname.to_string(), iVersion);
		}
		Ok(Self {
			vfs_name: vfsname.to_string(),
			base_vfs: basevfs.to_string(),
			i_version: iVersion,
			max_pathname: maxpathname,
			init_called: false,
		})
	}

	#[pyo3(signature = (*args, **kwargs))]
	fn __init__(
		mut slf: PyRefMut<'_, Self>,
		py: Python<'_>,
		args: &Bound<'_, PyTuple>,
		kwargs: Option<&Bound<'_, PyDict>>,
	) -> PyResult<()> {
		if slf.init_called {
			return Err(repeated_init_error());
		}

		let kw = |name: &str| -> PyResult<Option<Bound<'_, PyAny>>> {
			if let Some(kwargs) = kwargs {
				return kwargs.get_item(name);
			}
			Ok(None)
		};

		let vfsname = if !args.is_empty() {
			args.get_item(0)?.extract::<String>()?
		} else if let Some(value) = kw("vfsname")? {
			value.extract::<String>()?
		} else {
			String::new()
		};
		let basevfs = if args.len() >= 2 {
			args.get_item(1)?.extract::<String>()?
		} else if let Some(value) = kw("basevfs")? {
			value.extract::<String>()?
		} else {
			String::new()
		};
		let makedefault = if args.len() >= 3 {
			args.get_item(2)?.is_truthy()?
		} else if let Some(value) = kw("makedefault")? {
			value.is_truthy()?
		} else {
			false
		};
		let maxpathname = if args.len() >= 4 {
			parse_index_i32(args.py(), &args.get_item(3)?)?
		} else if let Some(value) = kw("maxpathname")? {
			parse_index_i32(args.py(), &value)?
		} else {
			1024
		};
		let i_version = if args.len() >= 5 {
			parse_index_i32(args.py(), &args.get_item(4)?)?
		} else if let Some(value) = kw("iVersion")? {
			parse_index_i32(args.py(), &value)?
		} else {
			3
		};

		slf.vfs_name = vfsname.clone();
		slf.base_vfs = basevfs;
		slf.i_version = i_version;
		slf.max_pathname = maxpathname;
		let mut names = custom_vfs_names().lock().map_err(|_| {
			pyo3::exceptions::PyRuntimeError::new_err("custom vfs registry lock poisoned")
		})?;
		if !vfsname.is_empty() {
			names.insert(vfsname.clone(), i_version);
		}
		drop(names);
		if makedefault && !vfsname.is_empty() {
			set_default_vfs(vfsname.as_str())?;
		}

		slf.init_called = true;
		if !vfsname.is_empty() {
			let vfs_obj =
				unsafe { Bound::<PyAny>::from_borrowed_ptr(py, slf.as_ptr() as *mut pyo3::ffi::PyObject) }
					.unbind();
			if let Ok(mut objects) = custom_vfs_objects().lock() {
				objects.insert(vfsname, vfs_obj);
			}
		}
		Ok(())
	}

	fn unregister(&self) -> PyResult<()> {
		let mut names = custom_vfs_names().lock().map_err(|_| {
			pyo3::exceptions::PyRuntimeError::new_err("custom vfs registry lock poisoned")
		})?;
		names.remove(&self.vfs_name);
		if let Ok(mut default_name) = default_custom_vfs().lock() {
			if default_name.as_deref() == Some(self.vfs_name.as_str()) {
				*default_name = None;
			}
		}
		if let Ok(mut objects) = custom_vfs_objects().lock() {
			objects.remove(&self.vfs_name);
		}
		Ok(())
	}

	#[pyo3(name = "xAccess")]
	fn x_access(&self, pathname: &str, _flags: c_int) -> bool {
		Path::new(pathname).exists()
	}

	#[pyo3(name = "xCurrentTime")]
	fn x_current_time(&self) -> f64 {
		let now = SystemTime::now();
		let seconds =
			now.duration_since(UNIX_EPOCH).map(|duration| duration.as_secs_f64()).unwrap_or(0.0);
		seconds / 86_400.0 + 2_440_587.5
	}

	#[pyo3(name = "xCurrentTimeInt64")]
	fn x_current_time_int64(&self) -> i64 {
		let julian = self.x_current_time();
		(julian * 86_400_000.0) as i64
	}

	#[pyo3(name = "xDelete")]
	fn x_delete(&self, filename: &str, _syncdir: bool) -> PyResult<()> {
		std::fs::remove_file(filename).map_err(|err| IOError::new_err(err.to_string()))
	}

	#[pyo3(name = "xDlClose")]
	fn x_dl_close(&self, _handle: usize) {}

	#[pyo3(name = "xDlError")]
	fn x_dl_error(&self) -> String {
		String::new()
	}

	#[pyo3(name = "xDlOpen")]
	fn x_dl_open(&self, _filename: &str) -> usize {
		0
	}

	#[pyo3(name = "xDlSym")]
	fn x_dl_sym(&self, _handle: usize, _symbol: &str) -> usize {
		0
	}

	#[pyo3(name = "xFullPathname")]
	fn x_full_pathname(&self, name: &str) -> String {
		if Path::new(name).is_absolute() {
			return name.to_string();
		}
		let cwd = std::env::current_dir().unwrap_or_else(|_| Path::new(".").to_path_buf());
		cwd.join(name).to_string_lossy().into_owned()
	}

	#[pyo3(name = "xGetLastError")]
	fn x_get_last_error(&self) -> (c_int, String) {
		(0, String::new())
	}

	#[pyo3(name = "xGetSystemCall")]
	fn x_get_system_call(&self, _name: &str) -> Option<usize> {
		None
	}

	#[pyo3(name = "xNextSystemCall")]
	fn x_next_system_call(&self, name: Option<&str>) -> Option<&'static str> {
		const CALLS: [&str; 3] = ["open", "close", "access"];
		match name {
			None => Some(CALLS[0]),
			Some(current) => {
				let index = CALLS.iter().position(|call| *call == current)?;
				if index + 1 < CALLS.len() { Some(CALLS[index + 1]) } else { None }
			}
		}
	}

	#[pyo3(name = "xOpen")]
	fn x_open(
		&self,
		py: Python<'_>,
		name: Option<&Bound<'_, PyAny>>,
		flags: &Bound<'_, PyAny>,
	) -> PyResult<Py<VFSFile>> {
		let filename = if let Some(name) = name {
			if name.is_none() { String::new() } else { name.str()?.to_string_lossy().to_string() }
		} else {
			String::new()
		};
		let _ = flags;
		Py::new(py, VFSFile { filename, init_called: false })
	}

	#[pyo3(name = "xRandomness")]
	fn x_randomness(&self, py: Python<'_>, amount: usize) -> PyResult<Py<PyBytes>> {
		let os = PyModule::import(py, "os")?;
		let bytes = os.getattr("urandom")?.call1((amount,))?.cast_into::<PyBytes>()?;
		Ok(bytes.unbind())
	}

	#[pyo3(name = "xSetSystemCall")]
	fn x_set_system_call(&self, _name: Option<&str>, _pointer: usize) -> bool {
		true
	}

	#[pyo3(name = "xSleep")]
	fn x_sleep(&self, microseconds: c_int) -> c_int {
		microseconds
	}
}

#[pymethods]
impl VFSFile {
	#[new]
	#[pyo3(signature = (*args, **kwargs))]
	fn new(args: &Bound<'_, PyTuple>, kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<Self> {
		let filename_obj = if args.len() >= 3 {
			Some(args.get_item(1)?)
		} else if args.len() >= 2 {
			Some(args.get_item(0)?)
		} else if let Some(kwargs) = kwargs {
			kwargs.get_item("filename")?
		} else {
			None
		};
		let filename = if let Some(filename) = filename_obj {
			filename.str().map(|text| text.to_string_lossy().to_string()).unwrap_or_default()
		} else {
			String::new()
		};
		Ok(Self { filename, init_called: false })
	}

	#[pyo3(signature = (*args, **kwargs))]
	fn __init__(
		&mut self,
		args: &Bound<'_, PyTuple>,
		kwargs: Option<&Bound<'_, PyDict>>,
	) -> PyResult<()> {
		let _ = (args, kwargs);
		if self.init_called {
			return Err(repeated_init_error());
		}
		self.init_called = true;
		Ok(())
	}

	#[allow(non_snake_case)]
	fn xRead<'py>(&self, py: Python<'py>, amount: usize, _offset: usize) -> Bound<'py, PyBytes> {
		PyBytes::new(py, &vec![0; amount])
	}

	#[allow(non_snake_case)]
	fn xWrite(&self, _data: &Bound<'_, PyAny>, _offset: usize) {}

	#[allow(non_snake_case)]
	fn xFileControl(&self, _op: c_int, _ptr: usize) -> bool {
		false
	}

	#[allow(non_snake_case)]
	fn xCheckReservedLock(&self) -> bool {
		false
	}

	#[allow(non_snake_case)]
	fn xClose(&self) {}

	#[allow(non_snake_case)]
	fn xDeviceCharacteristics(&self) -> c_int {
		0
	}

	#[allow(non_snake_case)]
	fn xFileSize(&self) -> i64 {
		0
	}

	#[allow(non_snake_case)]
	fn xLock(&self, _level: c_int) {}

	#[allow(non_snake_case)]
	fn xSectorSize(&self) -> c_int {
		4096
	}

	#[allow(non_snake_case)]
	fn xSync(&self, _flags: c_int) {}

	#[allow(non_snake_case)]
	fn xTruncate(&self, _newsize: i64) {}

	#[allow(non_snake_case)]
	fn xUnlock(&self, _level: c_int) {}

	#[getter]
	fn filename(&self) -> &str {
		&self.filename
	}
}
