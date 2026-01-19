mod counter_incremental_shared;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    counter_incremental_shared::run_from_cli().await
}

