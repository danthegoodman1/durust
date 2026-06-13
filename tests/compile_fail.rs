#[test]
fn workflow_determinism_lints_compile_fail() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui/*.rs");
}
