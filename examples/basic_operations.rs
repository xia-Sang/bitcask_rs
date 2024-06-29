use bitcask_rs::{db, options::Options};
use bytes::Bytes;
fn main() {
    let opts = Options::default();
    let engine = db::Engine::open(opts).unwrap();

    let res1 = engine.put(Bytes::from("xia"), Bytes::from("sang"));
    assert!(res1.is_ok());

    let res2 = engine.get(Bytes::from("xia"));
    assert!(res2.is_ok());
    let val = res2.ok().unwrap();
    println!("{:?}", String::from_utf8(val.to_vec()));

    let res3 = engine.delete(Bytes::from("xia"));
    assert!(res3.is_ok());
}
