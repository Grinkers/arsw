use core::ffi::c_int;
use std::ffi::{CStr, CString};

pub mod ffi;
pub mod raw;

pub fn sqlite_lib_version_number() -> i32 {
	unsafe { ffi::sqlite3_libversion_number() }
}

pub fn sqlite_lib_version() -> String {
	unsafe { CStr::from_ptr(ffi::sqlite3_libversion()).to_string_lossy().into_owned() }
}

pub fn sqlite_source_id() -> String {
	unsafe { CStr::from_ptr(ffi::sqlite3_sourceid()).to_string_lossy().into_owned() }
}

pub fn sqlite_compile_option_used(name: &str) -> bool {
	let Ok(name) = CString::new(name) else {
		return false;
	};
	unsafe { ffi::sqlite3_compileoption_used(name.as_ptr()) != 0 }
}

pub fn sqlite_compile_options() -> Vec<String> {
	let mut result = Vec::new();
	let mut index: c_int = 0;

	loop {
		let option = unsafe { ffi::sqlite3_compileoption_get(index) };
		if option.is_null() {
			break;
		}

		result.push(unsafe { CStr::from_ptr(option).to_string_lossy().into_owned() });
		index += 1;
	}

	result
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn has_sqlite_version_number() {
		assert!(sqlite_lib_version_number() >= 3_000_000);
	}

	#[test]
	fn has_sqlite_version_text() {
		assert!(!sqlite_lib_version().is_empty());
	}

	#[test]
	fn reports_compile_options() {
		assert!(!sqlite_compile_options().is_empty());
	}

	#[cfg(feature = "bundled-sqlite")]
	#[test]
	fn bundled_build_has_phase2_compile_options() {
		assert!(sqlite_compile_option_used("ENABLE_SESSION"));
		assert!(sqlite_compile_option_used("ENABLE_PREUPDATE_HOOK"));
		assert!(sqlite_compile_option_used("ENABLE_CARRAY"));
	}
}
