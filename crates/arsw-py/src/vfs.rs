use super::*;

#[pyfunction]
pub(crate) fn set_default_vfs(_name: &str) {}

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
}

#[pyclass(module = "apsw", subclass)]
pub(crate) struct VFSFile {
	pub(crate) filename: String,
}

#[pymethods]
impl VFS {
	#[new]
	#[pyo3(signature = (vfsname = "", basevfs = "", makedefault = false, maxpathname = 1024, *, iVersion = 3, exclude = None))]
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
		})
	}

	#[pyo3(signature = (vfsname = "", basevfs = "", makedefault = false, maxpathname = 1024, *, iVersion = 3, exclude = None))]
	fn __init__(
		&mut self,
		vfsname: &str,
		basevfs: &str,
		makedefault: bool,
		maxpathname: i32,
		iVersion: i32,
		exclude: Option<&Bound<'_, PyAny>>,
	) -> PyResult<()> {
		let _ = (makedefault, exclude);
		self.vfs_name = vfsname.to_string();
		self.base_vfs = basevfs.to_string();
		self.i_version = iVersion;
		self.max_pathname = maxpathname;
		let mut names = custom_vfs_names().lock().map_err(|_| {
			pyo3::exceptions::PyRuntimeError::new_err("custom vfs registry lock poisoned")
		})?;
		if !vfsname.is_empty() {
			names.insert(vfsname.to_string(), iVersion);
		}
		Ok(())
	}

	fn unregister(&self) -> PyResult<()> {
		let mut names = custom_vfs_names().lock().map_err(|_| {
			pyo3::exceptions::PyRuntimeError::new_err("custom vfs registry lock poisoned")
		})?;
		names.remove(&self.vfs_name);
		Ok(())
	}

	fn xAccess(&self, pathname: &str, _flags: c_int) -> bool {
		Path::new(pathname).exists()
	}

	fn xCurrentTime(&self) -> f64 {
		let now = SystemTime::now();
		let seconds =
			now.duration_since(UNIX_EPOCH).map(|duration| duration.as_secs_f64()).unwrap_or(0.0);
		seconds / 86_400.0 + 2_440_587.5
	}

	fn xCurrentTimeInt64(&self) -> i64 {
		let julian = self.xCurrentTime();
		(julian * 86_400_000.0) as i64
	}

	fn xDelete(&self, filename: &str, _syncdir: bool) -> PyResult<()> {
		std::fs::remove_file(filename).map_err(|err| IOError::new_err(err.to_string()))
	}

	fn xDlClose(&self, _handle: usize) {}

	fn xDlError(&self) -> String {
		String::new()
	}

	fn xDlOpen(&self, _filename: &str) -> usize {
		0
	}

	fn xDlSym(&self, _handle: usize, _symbol: &str) -> usize {
		0
	}

	fn xFullPathname(&self, name: &str) -> String {
		if Path::new(name).is_absolute() {
			return name.to_string();
		}
		let cwd = std::env::current_dir().unwrap_or_else(|_| Path::new(".").to_path_buf());
		cwd.join(name).to_string_lossy().into_owned()
	}

	fn xGetLastError(&self) -> (c_int, String) {
		(0, String::new())
	}

	fn xGetSystemCall(&self, _name: &str) -> Option<usize> {
		None
	}

	fn xNextSystemCall(&self, name: Option<&str>) -> Option<&'static str> {
		const CALLS: [&str; 3] = ["open", "close", "access"];
		match name {
			None => Some(CALLS[0]),
			Some(current) => {
				let index = CALLS.iter().position(|call| *call == current)?;
				if index + 1 < CALLS.len() { Some(CALLS[index + 1]) } else { None }
			}
		}
	}

	fn xOpen(
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
		Py::new(py, VFSFile { filename })
	}

	fn xRandomness(&self, py: Python<'_>, amount: usize) -> PyResult<Py<PyBytes>> {
		let os = PyModule::import(py, "os")?;
		let bytes = os.getattr("urandom")?.call1((amount,))?.cast_into::<PyBytes>()?;
		Ok(bytes.unbind())
	}

	fn xSetSystemCall(&self, _name: Option<&str>, _pointer: usize) -> bool {
		true
	}

	fn xSleep(&self, microseconds: c_int) -> c_int {
		microseconds
	}
}

#[pymethods]
impl VFSFile {
	#[new]
	fn new(
		_inheritfromvfsname: &str,
		filename: &Bound<'_, PyAny>,
		_flags: &Bound<'_, PyAny>,
	) -> Self {
		let filename =
			filename.str().map(|text| text.to_string_lossy().to_string()).unwrap_or_default();
		Self { filename }
	}

	fn __init__(
		&mut self,
		_inheritfromvfsname: &str,
		filename: &Bound<'_, PyAny>,
		_flags: &Bound<'_, PyAny>,
	) {
		self.filename =
			filename.str().map(|text| text.to_string_lossy().to_string()).unwrap_or_default();
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
