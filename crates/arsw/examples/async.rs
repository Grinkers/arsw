#[cfg(feature = "async")]
mod async_example {
	use arsw::{Connection, Value};
	use std::future::Future;
	use std::pin::Pin;
	use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

	fn noop_raw_waker() -> RawWaker {
		fn clone(_: *const ()) -> RawWaker {
			noop_raw_waker()
		}
		const fn wake(_: *const ()) {}
		const fn wake_by_ref(_: *const ()) {}
		const fn drop(_: *const ()) {}
		let vtable = &RawWakerVTable::new(clone, wake, wake_by_ref, drop);
		RawWaker::new(std::ptr::null(), vtable)
	}

	fn block_on<F>(future: F) -> F::Output
	where
		F: Future,
	{
		let waker = unsafe { Waker::from_raw(noop_raw_waker()) };
		let mut cx = Context::from_waker(&waker);
		let mut future = Box::pin(future);

		loop {
			match Future::poll(Pin::as_mut(&mut future), &mut cx) {
				Poll::Ready(output) => return output,
				Poll::Pending => std::thread::yield_now(),
			}
		}
	}

	async fn run() -> Result<(), Box<dyn std::error::Error>> {
		std::future::ready(()).await;

		let db = Connection::open_in_memory()?;
		db.execute("create table jobs(id integer primary key, name text not null)")?;

		let mut insert = db.prepare("insert into jobs(name) values (?)")?;
		for name in ["fetch", "index", "vacuum"] {
			insert.bind_text(1, name)?;
			let _ = insert.step()?;
			insert.reset()?;
		}

		for row in db.query_all("select id, name from jobs order by id")? {
			if let [Value::Integer(id), Value::Text(name)] = row.as_slice() {
				println!("job {id}: {name}");
			}
		}

		Ok(())
	}

	pub fn main() -> Result<(), Box<dyn std::error::Error>> {
		block_on(run())
	}
}

#[cfg(feature = "async")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
	async_example::main()
}

#[cfg(not(feature = "async"))]
fn main() {
	println!(
		"Enable feature `async` to run this example: cargo run -p arsw --example async --features async"
	);
}
