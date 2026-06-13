#[durust::workflow(name = "bad.rand-random", version = 1)]
async fn bad(_: ()) -> durust::Result<()> {
    let _value: u64 = rand::random();
    Ok(())
}

fn main() {}
