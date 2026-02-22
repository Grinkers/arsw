use arsw::{Connection, Value};

#[test]
fn tour_flow_round_trips_values() {
	let db = Connection::open_in_memory().expect("open memory database");
	db.execute("create table t(name text, qty integer)").expect("create table");

	let mut insert = db.prepare("insert into t(name, qty) values (?, ?)").expect("prepare insert");
	insert.bind_text(1, "widgets").expect("bind name");
	insert.bind_i64(2, 12).expect("bind qty");
	let _ = insert.step().expect("run insert");

	let rows = db.query_all("select name, qty from t").expect("select rows");
	assert_eq!(rows, vec![vec![Value::Text("widgets".to_string()), Value::Integer(12)]]);
}
