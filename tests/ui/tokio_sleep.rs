#[durust::workflow(name = "bad.tokio-sleep", version = 1)]
async fn bad(_: ()) -> durust::Result<()> {
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    Ok(())
}

fn main() {}
