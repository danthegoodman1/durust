#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct BadInput {}

#[durust::workflow(name = "bad.unknown-await", version = 1)]
async fn bad(_: BadInput) -> durust::Result<()> {
    std::future::ready(()).await;
    Ok(())
}

fn main() {}
