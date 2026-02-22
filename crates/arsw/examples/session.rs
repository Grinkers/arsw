use arsw::{Connection, sqlite_compile_option_used};

fn main() -> Result<(), Box<dyn std::error::Error>> {
	if !sqlite_compile_option_used("ENABLE_SESSION") {
		println!("Session extension not available in this SQLite build");
		return Ok(());
	}

	let db = Connection::open_in_memory()?;
	db.execute("create table items(id integer primary key, value text not null)")?;

	let mut session = db.create_session("main")?;
	session.attach(Some("items"))?;

	db.execute("insert into items(value) values ('apple'), ('pear')")?;

	let changeset = session.changeset()?;
	let patchset = session.patchset()?;

	println!("captured changeset bytes: {}", changeset.len());
	println!("captured patchset bytes: {}", patchset.len());

	Ok(())
}
