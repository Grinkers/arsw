use arsw::{Connection, Value};

fn main() -> Result<(), Box<dyn std::error::Error>> {
	let db = Connection::open_in_memory()?;
	db.execute("create table items(name text not null, payload text not null)")?;

	let mut insert = db.prepare("insert into items(name, payload) values (?, ?)")?;
	insert.bind_text(1, "orange")?;
	insert.bind_text(2, r#"{"origin":"Spain","diameter":7.5,"tags":["citrus","sweet","juice"]}"#)?;
	let _ = insert.step()?;

	let rows = db.query_all(
		"select name, json_extract(payload, '$.origin'), json_extract(payload, '$.tags[0]') from items",
	)?;

	for row in rows {
		match row.as_slice() {
			[Value::Text(name), Value::Text(origin), Value::Text(first_tag)] => {
				println!("{name} from {origin}; first tag: {first_tag}");
			}
			_ => return Err("unexpected row shape from json query".into()),
		}
	}

	Ok(())
}
