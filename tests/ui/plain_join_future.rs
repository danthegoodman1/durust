#[durust::workflow(name = "bad.plain-join-future", version = 1)]
async fn bad(_: ()) -> durust::Result<()> {
    let _ = durust::join!(
        std::future::ready(Ok::<(), durust::Error>(())),
        durust::sleep(std::time::Duration::from_millis(1)),
    )
    .await?;
    Ok(())
}

fn main() {}
