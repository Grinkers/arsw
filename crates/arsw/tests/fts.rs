use arsw::{Connection, sqlite_compile_option_used};

#[test]
fn fts_match_finds_expected_rows() {
	if !sqlite_compile_option_used("ENABLE_FTS5") {
		return;
	}

	let db = Connection::open_in_memory().expect("open memory database");
	db.execute("create virtual table search using fts5(title, body)").expect("create fts table");
	db.execute("insert into search(title, body) values ('Orange cake', 'citrus zest')")
		.expect("insert row 1");
	db.execute("insert into search(title, body) values ('Spiced tea', 'cinnamon spice')")
		.expect("insert row 2");

	let rows = db
		.query_all("select title from search where search match 'orange OR spice' order by rank")
		.expect("run fts query");

	assert_eq!(rows.len(), 2);
}
