#[durust::workflow(name = "bad.unknown-await", version = 1)]
async fn bad(_: ()) -> durust::Result<()> {
    std::future::ready(()).await;
    Ok(())
}

fn main() {}
