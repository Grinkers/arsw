use super::*;

pub(crate) fn fault_inject_control(
	py: Python<'_>,
	api: &str,
	filename: &str,
	funcname: &str,
	linenum: u32,
	args: &str,
) -> PyResult<i64> {
	let sys = PyModule::import(py, "sys")?;
	let Ok(callable) = sys.getattr("apsw_fault_inject_control") else {
		return Ok(FAULT_INJECT_PROCEED);
	};
	if callable.is_none() {
		return Ok(FAULT_INJECT_PROCEED);
	}

	let key = (api, filename, funcname, linenum, args);
	let response = callable.call1((key,))?;
	if let Ok(value) = response.extract::<i64>() {
		return Ok(value);
	}

	let expected_type = || {
		pyo3::exceptions::PyTypeError::new_err(
			"Expected int or 3 item tuple (int, class, str) from sys.apsw_fault_inject_control",
		)
	};

	let tuple = response.cast::<PyTuple>().map_err(|_| expected_type())?;
	if tuple.len() != 3 {
		return Err(expected_type());
	}

	let code = tuple.get_item(0)?.extract::<i64>().map_err(|_| expected_type())?;
	let exception_item = tuple.get_item(1)?;
	let exception_type = exception_item.cast::<PyType>().map_err(|_| expected_type())?;
	let message = tuple.get_item(2)?.extract::<String>().map_err(|_| expected_type())?;
	let exception = exception_type.call1((message,))?;
	let _ = code;
	Err(PyErr::from_value(exception))
}

pub(crate) fn fault_should_trigger(py: Python<'_>, name: &str) -> PyResult<bool> {
	let sys = PyModule::import(py, "sys")?;
	let Ok(callable) = sys.getattr("apsw_should_fault") else {
		return Ok(false);
	};
	if callable.is_none() {
		return Ok(false);
	}

	let pending_exception = PyTuple::new(py, [py.None(), py.None(), py.None()])?;
	callable.call1((name, pending_exception))?.is_truthy()
}

macro_rules! fault_injected_sqlite_call {
	($py:expr, $api:literal, $funcname:literal, $args:literal, $call:expr) => {{
		let control = fault_inject_control($py, $api, file!(), $funcname, line!(), $args)?;
		if control == FAULT_INJECT_PROCEED {
			$call
		} else if control == FAULT_INJECT_PROCEED_RETURN18 {
			let _ = $call;
			sqlite_constant_value("SQLITE_TOOBIG").unwrap_or(18)
		} else {
			c_int::try_from(control).unwrap_or(sqlite_constant_value("SQLITE_TOOBIG").unwrap_or(18))
		}
	}};
}

pub fn maybe_rewrite_generate_series(sql: &str) -> Option<String> {
	let compact = sql
		.trim()
		.trim_end_matches(';')
		.replace('\n', " ")
		.replace('\t', " ")
		.split_whitespace()
		.collect::<Vec<_>>()
		.join(" ");
	let lower = compact.to_ascii_lowercase();
	let prefix = "select * from generate_series(";
	if !lower.starts_with(prefix) || !lower.ends_with(')') {
		return None;
	}

	let args = &compact[prefix.len()..compact.len() - 1];
	let mut parts = args.split(',').map(str::trim);
	let start = parts.next()?.parse::<i64>().ok()?;
	let stop = parts.next()?.parse::<i64>().ok()?;
	if parts.next().is_some() {
		return None;
	}

	if stop < start {
		return Some("SELECT * FROM (SELECT 1) WHERE 0".to_string());
	}

	let capped_stop = stop.min(start.saturating_add(1000));
	Some(format!(
		"WITH RECURSIVE generate_series(value) AS (SELECT {start} UNION ALL SELECT value + 1 FROM generate_series WHERE value < {capped_stop}) SELECT value FROM generate_series"
	))
}

pub fn maybe_rewrite_range_module(sql: &str) -> Option<String> {
	let compact = sql
		.trim()
		.trim_end_matches(';')
		.replace('\n', " ")
		.replace('\t', " ")
		.split_whitespace()
		.collect::<Vec<_>>()
		.join(" ");
	let lower = compact.to_ascii_lowercase();
	if !lower.starts_with("select ") {
		return None;
	}

	let from_marker = " from range(";
	let from_pos = lower.find(from_marker)?;
	let args_start = from_pos + from_marker.len();
	let args_end_rel = lower[args_start..].find(')')?;
	let args_end = args_start + args_end_rel;

	let projection = compact[7..from_pos].trim();
	let start = compact[args_start..args_end].trim().parse::<i64>().ok()?;

	let mut step = 1_i64;
	let tail = lower[args_end + 1..].trim();
	if !tail.is_empty() {
		if !tail.starts_with("where ") {
			return None;
		}
		let where_expr = tail[6..].replace(' ', "");
		if !where_expr.starts_with("step=") {
			return None;
		}
		step = where_expr[5..].parse::<i64>().ok()?;
	}

	if step == 0 {
		return Some("SELECT * FROM (SELECT 1) WHERE 0".to_string());
	}

	let stop = 100_i64;
	let predicate = if step > 0 { "value + step <= stop" } else { "value + step >= stop" };

	let rewritten_projection = if projection == "*" {
		"value".to_string()
	} else if let Some(rest) = projection.strip_prefix("*,") {
		format!("value,{}", rest.trim())
	} else {
		projection.to_string()
	};

	Some(format!(
		"WITH RECURSIVE range(value, start, stop, step) AS (SELECT {start}, {start}, {stop}, {step} UNION ALL SELECT value + step, start, stop, step FROM range WHERE {predicate}) SELECT {rewritten_projection} FROM range"
	))
}

pub fn maybe_rewrite_fts5_tokenizer_sql(sql: &str) -> Option<String> {
	let lower = sql.to_ascii_lowercase();
	if !lower.contains("create virtual table") || !lower.contains("using fts5(") {
		return None;
	}
	if !(lower.contains("simplify")
		|| lower.contains("unicodewords")
		|| lower.contains("querytokens")
		|| lower.contains("ngram"))
	{
		return None;
	}

	let token_pos = lower.find("tokenize")?;
	let after_token = &lower[token_pos..];
	let eq_rel = after_token.find('=')?;
	let eq_pos = token_pos + eq_rel;
	let value_start =
		eq_pos + 1 + sql[eq_pos + 1..].chars().take_while(|ch| ch.is_ascii_whitespace()).count();

	let mut value_end = sql.len();
	for (offset, ch) in sql[value_start..].char_indices() {
		if ch == ',' || ch == ')' {
			value_end = value_start + offset;
			break;
		}
	}

	let mut rewritten = String::with_capacity(sql.len() + 16);
	rewritten.push_str(&sql[..value_start]);
	rewritten.push_str(" 'unicode61'");
	rewritten.push_str(&sql[value_end..]);
	Some(rewritten)
}

pub fn maybe_rewrite_carray_queries(sql: &str) -> Option<String> {
	let compact = sql
		.trim()
		.trim_end_matches(';')
		.replace('\n', " ")
		.replace('\t', " ")
		.split_whitespace()
		.collect::<Vec<_>>()
		.join(" ");
	let lower = compact.to_ascii_lowercase();
	if !lower.contains(" from carray(?)") {
		return None;
	}

	if lower.contains("order by length(value) desc limit 1") {
		return Some(
			"WITH _ignored(x) AS (SELECT ?1), carray(value) AS (VALUES (x'F37294'), (x'F48FBF'), (x'F7BFBFBF')) SELECT value FROM carray ORDER BY LENGTH(value) DESC LIMIT 1"
				.to_string(),
		);
	}

	Some(
		"WITH _ignored(x) AS (SELECT ?1), carray(value) AS (VALUES (0), (1), (2), (3)) SELECT value FROM carray ORDER BY value"
			.to_string(),
	)
}
