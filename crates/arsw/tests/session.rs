use arsw::{Connection, sqlite_compile_option_used};

#[test]
fn session_changeset_is_produced() {
	if !sqlite_compile_option_used("ENABLE_SESSION") {
		return;
	}

	let db = Connection::open_in_memory().expect("open memory database");
	db.execute("create table audit(id integer primary key, detail text)").expect("create table");

	let mut session = db.create_session("main").expect("create session");
	session.attach(Some("audit")).expect("attach audit table");

	db.execute("insert into audit(detail) values ('created')").expect("insert row");

	let changeset = session.changeset().expect("get changeset");
	let patchset = session.patchset().expect("get patchset");

	assert!(!changeset.is_empty());
	assert!(!patchset.is_empty());
}
