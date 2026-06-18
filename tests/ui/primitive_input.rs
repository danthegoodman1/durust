#[durust::workflow(name = "bad.primitive-input", version = 1)]
async fn bad(input: u64) -> durust::Result<()> {
    let _ = input;
    Ok(())
}

fn main() {}
