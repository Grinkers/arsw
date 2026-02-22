use arsw::{Connection, Value, sqlite_compile_option_used};

fn main() -> Result<(), Box<dyn std::error::Error>> {
	if !sqlite_compile_option_used("ENABLE_FTS5") {
		println!("FTS5 not available in this SQLite build");
		return Ok(());
	}

	let db = Connection::open_in_memory()?;
	db.execute("create virtual table search using fts5(title, body)")?;

	let mut insert = db.prepare("insert into search(title, body) values (?, ?)")?;
	for (title, body) in [
		("Orange cake", "citrus zest and sweet glaze"),
		("Spiced tea", "cinnamon clove and warm spice"),
		("Fruit salad", "orange slices and mint"),
	] {
		insert.bind_text(1, title)?;
		insert.bind_text(2, body)?;
		let _ = insert.step()?;
		insert.reset()?;
	}

	let mut query =
		db.prepare("select title from search where search match ? order by rank limit 5")?;
	query.bind_text(1, "orange OR spice")?;

	while query.step()? {
		if let Value::Text(title) = query.column_value(0) {
			println!("hit: {title}");
		}
	}

	Ok(())
}
