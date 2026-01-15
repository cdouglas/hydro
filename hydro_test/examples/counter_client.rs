use clap::Parser;
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

/// A simple increment request with a payload value.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IncrementRequest {
    /// Unique identifier for this request
    pub id: u32,
    /// Amount to increment by
    pub amount: u32,
}

/// Response sent when a request is processed
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IncrementResponse {
    /// Request ID that was processed
    pub id: u32,
    /// Counter value after this increment
    pub counter_value: u32,
    /// Whether the server is shutting down (threshold reached)
    pub shutdown: bool,
}

#[derive(Parser, Debug)]
struct Args {
    /// Server port to connect to
    #[arg(long, default_value = "4002")]
    port: u16,

    /// Number of requests to send
    #[arg(long, default_value = "5")]
    count: u32,

    /// Amount to increment by for each request
    #[arg(long, default_value = "10")]
    amount: u32,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    println!(
        "Connecting to counter server on port {}...",
        args.port
    );

    let stream = TcpStream::connect(format!("127.0.0.1:{}", args.port)).await?;
    let (read, write) = stream.into_split();

    let mut framed_write = FramedWrite::new(write, LengthDelimitedCodec::new());
    let mut framed_read = FramedRead::new(read, LengthDelimitedCodec::new());

    println!(
        "Sending {} requests with amount={}...",
        args.count, args.amount
    );

    // Send requests
    for id in 0..args.count {
        let request = IncrementRequest {
            id,
            amount: args.amount,
        };
        let encoded = bincode::serialize(&request)?;
        framed_write.send(encoded.into()).await?;
        println!("Sent request {}: amount={}", id, args.amount);
    }

    // Receive responses
    for _ in 0..args.count {
        if let Some(Ok(data)) = framed_read.next().await {
            let response: IncrementResponse = bincode::deserialize(&data)?;
            println!(
                "Response {}: counter={}, shutdown={}",
                response.id, response.counter_value, response.shutdown
            );

            if response.shutdown {
                println!("Server indicated shutdown!");
                break;
            }
        }
    }

    println!("Client finished.");
    Ok(())
}
