// A workflow may await any future: an async helper that awaits durable APIs,
// and the durable APIs themselves. Determinism is enforced by replay and
// command fingerprints, and the path lint catches the common host-clock and
// scheduler calls; there is no allowlist of awaitable expressions.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct Input {}

async fn pause_twice() -> durust::Result<()> {
    durust::sleep(std::time::Duration::from_millis(1)).await?;
    durust::sleep(std::time::Duration::from_millis(1)).await?;
    Ok(())
}

#[durust::workflow(name = "ok.async-helper", version = 1)]
async fn ok(_: Input) -> durust::Result<i64> {
    pause_twice().await?;
    let now = durust::now().await?;
    Ok(now.0)
}

fn main() {}
