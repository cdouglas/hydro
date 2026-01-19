mod counter_incremental_shared;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    counter_incremental_shared::run_from_cli().await
}

#[test]
fn test() {
    use example_test::run_current_example;

    let mut run = run_current_example!();
    run.read_regex(r"Counter server listening on port");
}
