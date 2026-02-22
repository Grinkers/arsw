use std::env;
use std::path::PathBuf;

fn main() {
	let bundled = env::var_os("CARGO_FEATURE_BUNDLED_SQLITE").is_some();

	match bundled {
		true => build_bundled(),
		false => link_system(),
	}
}

fn build_bundled() {
	let manifest_dir =
		PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set"));
	let sqlite_dir = manifest_dir.join("..").join("..").join("sqlite3");
	let sqlite_c = sqlite_dir.join("sqlite3.c");

	assert!(
		sqlite_c.exists(),
		"missing bundled SQLite amalgamation at {} (run setup.py fetch or make build_ext)",
		sqlite_c.display()
	);

	println!("cargo:rerun-if-changed={}", sqlite_c.display());
	println!("cargo:rerun-if-env-changed=LIBSQLITE3_FLAGS");

	let mut build = cc::Build::new();
	build.file(&sqlite_c).include(&sqlite_dir).warnings(false);

	for (name, value) in [
		("SQLITE_CORE", None),
		("SQLITE_DEFAULT_FOREIGN_KEYS", Some("1")),
		("SQLITE_ENABLE_API_ARMOR", None),
		("SQLITE_ENABLE_CARRAY", None),
		("SQLITE_ENABLE_COLUMN_METADATA", None),
		("SQLITE_ENABLE_DBSTAT_VTAB", None),
		("SQLITE_ENABLE_FTS3", None),
		("SQLITE_ENABLE_FTS3_PARENTHESIS", None),
		("SQLITE_ENABLE_FTS5", None),
		("SQLITE_ENABLE_JSON1", None),
		("SQLITE_ENABLE_LOAD_EXTENSION", Some("1")),
		("SQLITE_ENABLE_MEMORY_MANAGEMENT", None),
		("SQLITE_ENABLE_PREUPDATE_HOOK", None),
		("SQLITE_ENABLE_RTREE", None),
		("SQLITE_ENABLE_SESSION", None),
		("SQLITE_ENABLE_STAT4", None),
		("SQLITE_ENABLE_TRACE_V2", None),
		("SQLITE_SOUNDEX", None),
		("SQLITE_THREADSAFE", Some("1")),
		("SQLITE_USE_URI", None),
		("HAVE_ISNAN", None),
		("HAVE_USLEEP", Some("1")),
		("_POSIX_THREAD_SAFE_FUNCTIONS", None),
	] {
		build.define(name, value);
	}

	if !env::var("CARGO_CFG_WINDOWS").is_ok_and(|v| !v.is_empty()) {
		build.define("HAVE_LOCALTIME_R", None);
	}

	if let Ok(extras) = env::var("LIBSQLITE3_FLAGS") {
		for extra in extras.split_whitespace() {
			if let Some(rest) = extra.strip_prefix("-D") {
				define_flag(&mut build, rest);
			} else if let Some(rest) = extra.strip_prefix("SQLITE_") {
				define_flag(&mut build, &format!("SQLITE_{rest}"));
			}
		}
	}

	build.compile("sqlite3");
}

fn link_system() {
	let linked = pkg_config::Config::new()
		.atleast_version("3.34.1")
		.print_system_libs(false)
		.probe("sqlite3")
		.is_ok();

	if !linked {
		println!("cargo:rustc-link-lib=dylib=sqlite3");
	}
}

fn define_flag(build: &mut cc::Build, define: &str) {
	if let Some((name, value)) = define.split_once('=') {
		build.define(name, Some(value));
	} else {
		build.define(define, None);
	}
}
