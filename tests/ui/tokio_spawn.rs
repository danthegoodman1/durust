#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct BadInput {}

#[durust::workflow(name = "bad.tokio-spawn", version = 1)]
async fn bad(_: BadInput) -> durust::Result<()> {
    let _handle = tokio::spawn(async {});
    Ok(())
}

fn main() {}
