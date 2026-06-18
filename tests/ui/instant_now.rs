#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct BadInput {}

#[durust::workflow(name = "bad.instant-now", version = 1)]
async fn bad(_: BadInput) -> durust::Result<()> {
    let _now = std::time::Instant::now();
    Ok(())
}

fn main() {}
