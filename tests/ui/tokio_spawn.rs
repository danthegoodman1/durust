#[durust::workflow(name = "bad.tokio-spawn", version = 1)]
async fn bad(_: ()) -> durust::Result<()> {
    let _handle = tokio::spawn(async {});
    Ok(())
}

fn main() {}
