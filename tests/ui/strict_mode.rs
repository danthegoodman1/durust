#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct BadInput {}

#[durust::workflow(name = "bad.strict-mode", version = 1, strict)]
async fn bad(_: BadInput) -> durust::Result<()> {
    Ok(())
}

fn main() {}
