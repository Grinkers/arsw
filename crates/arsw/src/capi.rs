use core::ffi::{c_char, c_int};

#[unsafe(no_mangle)]
pub unsafe extern "C" fn arsw_sqlite_lib_version_number() -> c_int {
	unsafe { arsw_sqlite_ffi::ffi::sqlite3_libversion_number() }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn arsw_sqlite_lib_version() -> *const c_char {
	unsafe { arsw_sqlite_ffi::ffi::sqlite3_libversion() }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn arsw_sqlite_source_id() -> *const c_char {
	unsafe { arsw_sqlite_ffi::ffi::sqlite3_sourceid() }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn arsw_sqlite_compile_option_used(name: *const c_char) -> c_int {
	if name.is_null() {
		return 0;
	}
	unsafe { arsw_sqlite_ffi::ffi::sqlite3_compileoption_used(name) }
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn capi_libversion_number_is_sane() {
		let version = unsafe { arsw_sqlite_lib_version_number() };
		assert!(version >= 3_000_000);
	}

	#[test]
	fn capi_compile_option_used_handles_null() {
		let used = unsafe { arsw_sqlite_compile_option_used(core::ptr::null()) };
		assert_eq!(used, 0);
	}
}
