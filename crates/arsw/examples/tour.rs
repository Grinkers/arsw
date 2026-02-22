use arsw::{Connection, Value};

fn main() -> Result<(), Box<dyn std::error::Error>> {
	let db = Connection::open_in_memory()?;
	db.execute("create table inventory(sku text primary key, qty integer not null)")?;

	let tx = db.transaction()?;
	let mut insert = tx.prepare("insert into inventory(sku, qty) values (?, ?)")?;
	for (sku, qty) in [("A100", 8_i64), ("B205", 14), ("C330", 3)] {
		insert.bind_values(&[Value::Text(sku.to_string()), Value::Integer(qty)])?;
		let _ = insert.step()?;
		insert.reset()?;
	}
	tx.commit()?;

	for row in db.query_all("select sku, qty from inventory order by sku")? {
		match row.as_slice() {
			[Value::Text(sku), Value::Integer(qty)] => {
				println!("{sku}: {qty}");
			}
			_ => return Err("unexpected row shape".into()),
		}
	}

	Ok(())
}
