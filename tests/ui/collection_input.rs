#[durust::workflow(name = "bad.collection-input", version = 1)]
async fn bad(input: Vec<u64>) -> durust::Result<()> {
    let _ = input;
    Ok(())
}

fn main() {}
