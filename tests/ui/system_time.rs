#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct BadInput {}

#[durust::workflow(name = "bad.system-time", version = 1)]
async fn bad(_: BadInput) -> durust::Result<()> {
    let _now = std::time::SystemTime::now();
    Ok(())
}

fn main() {}
