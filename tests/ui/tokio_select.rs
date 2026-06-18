#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct BadInput {}

#[durust::workflow(name = "bad.tokio-select", version = 1)]
async fn bad(_: BadInput) -> durust::Result<()> {
    tokio::select! {
        _ = std::future::ready(()) => {}
    }
    Ok(())
}

fn main() {}
