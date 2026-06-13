#[durust::workflow(name = "bad.system-time", version = 1)]
async fn bad(_: ()) -> durust::Result<()> {
    let _now = std::time::SystemTime::now();
    Ok(())
}

fn main() {}
