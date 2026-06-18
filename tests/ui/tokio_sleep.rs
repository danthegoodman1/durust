#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct BadInput {}

#[durust::workflow(name = "bad.tokio-sleep", version = 1)]
async fn bad(_: BadInput) -> durust::Result<()> {
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    Ok(())
}

fn main() {}
