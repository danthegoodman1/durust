#[durust::workflow(name = "bad.instant-now", version = 1)]
async fn bad(_: ()) -> durust::Result<()> {
    let _now = std::time::Instant::now();
    Ok(())
}

fn main() {}
