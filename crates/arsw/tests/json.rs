use arsw::{Connection, Value};

#[test]
fn json_extract_returns_expected_values() {
	let db = Connection::open_in_memory().expect("open memory database");
	db.execute("create table items(payload text not null)").expect("create table");

	let mut insert = db.prepare("insert into items(payload) values (?)").expect("prepare insert");
	insert
		.bind_text(1, r#"{"origin":"Spain","tags":["citrus","sweet"]}"#)
		.expect("bind json payload");
	let _ = insert.step().expect("execute insert");

	let rows = db
		.query_all(
			"select json_extract(payload, '$.origin'), json_extract(payload, '$.tags[1]') from items",
		)
		.expect("run json query");

	assert_eq!(rows, vec![vec![Value::Text("Spain".to_string()), Value::Text("sweet".to_string())]]);
}
