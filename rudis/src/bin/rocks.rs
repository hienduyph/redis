use rocksdb::{DB, Options};

fn main() {
    let path = ".direnv/trydocks";
    let db = DB::open_default(path).unwrap();

    let key = "my key";
    db.put(key, b"val").unwrap();

    match db.get(key) {
        Ok(Some(vale)) => println!("get value {}", String::from_utf8(vale).unwrap()),
        Ok(None) => println!("Value not found"),
        Err(e)  => println!("got err: {}", e),
    }

    db.delete(key).unwrap();

    // let _ = DB::destroy(&Options::default(), path);
}
