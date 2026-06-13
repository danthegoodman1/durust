#[durust::workflow(name = "bad.tokio-select", version = 1)]
async fn bad(_: ()) -> durust::Result<()> {
    tokio::select! {
        _ = std::future::ready(()) => {}
    }
    Ok(())
}

fn main() {}
