use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

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
}

/// A simple counter that accumulates increments (NO CYCLES).
/// 
/// All requests are processed in a single tick - the counter
/// accumulates all increments and returns the final value to each request.
pub fn counter_simple<'a>(
    requests: Stream<IncrementRequest, Process<'a>, Unbounded>,
) -> Stream<IncrementResponse, Process<'a>, Unbounded> {
    sliced! {
        let batch_requests = use(requests.clone(), nondet!(/** requests arrive */));
        
        // Extract amounts and sum them
        let counter_value = batch_requests
            .clone()
            .map(q!(|req| req.amount))
            .fold(q!(|| 0), q!(|a: &mut u32, b| *a += b));
        
        // Pair each request with the final counter value
        batch_requests
            .cross_singleton(counter_value)
            .map(q!(|(req, count): (IncrementRequest, u32)| {
                IncrementResponse {
                    id: req.id,
                    counter_value: count,
                }
            }))
    }
}

#[cfg(test)]
mod tests {
    use futures::{SinkExt, StreamExt};
    use hydro_deploy::Deployment;
    use hydro_lang::location::Location;
    use hydro_lang::nondet::nondet;
    use hydro_lang::prelude::FlowBuilder;
    use tokio::time::{timeout, Duration};

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_counter_simple_basic() {
        let mut deployment = Deployment::new();

        let builder = FlowBuilder::new();
        let external = builder.external::<()>();
        let p1 = builder.process();

        let (requests_send, requests) = p1.source_external_bincode(&external);
        let out = counter_simple(requests);
        let out_recv = out.send_bincode_external(&external);

        let built = builder.with_default_optimize();

        let nodes = built
            .with_process(&p1, deployment.Localhost())
            .with_external(&external, deployment.Localhost())
            .deploy(&mut deployment);

        deployment.deploy().await.unwrap();

        let mut requests_send = nodes.connect(requests_send).await;
        let mut out_recv = nodes.connect(out_recv).await;

        deployment.start().await.unwrap();

        // Send three requests AFTER starting
        requests_send
            .send(IncrementRequest { id: 1, amount: 5 })
            .await
            .unwrap();
        requests_send
            .send(IncrementRequest { id: 2, amount: 3 })
            .await
            .unwrap();
        requests_send
            .send(IncrementRequest { id: 3, amount: 2 })
            .await
            .unwrap();

        // Collect results with timeout
        let results = timeout(Duration::from_secs(5), out_recv.take(3).collect::<Vec<_>>())
            .await
            .expect("Test timed out");

        // All requests should see the same final counter value
        assert_eq!(results.len(), 3);

        // Total: 5 + 3 + 2 = 10
        for result in &results {
            assert_eq!(
                result.counter_value, 10,
                "Expected counter value of 10, got {}",
                result.counter_value
            );
        }

        // Check all IDs are present
        let mut ids: Vec<u32> = results.iter().map(|r| r.id).collect();
        ids.sort();
        assert_eq!(ids, vec![1, 2, 3]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_counter_simple_many() {
        let mut deployment = Deployment::new();

        let builder = FlowBuilder::new();
        let external = builder.external::<()>();
        let p1 = builder.process();

        let (requests_send, requests) = p1.source_external_bincode(&external);
        let out = counter_simple(requests);
        let out_recv = out.send_bincode_external(&external);

        let built = builder.with_default_optimize();

        let nodes = built
            .with_process(&p1, deployment.Localhost())
            .with_external(&external, deployment.Localhost())
            .deploy(&mut deployment);

        deployment.deploy().await.unwrap();

        let mut requests_send = nodes.connect(requests_send).await;
        let mut out_recv = nodes.connect(out_recv).await;

        deployment.start().await.unwrap();

        // Send 10 requests AFTER starting, each incrementing by 1
        for id in 0..10 {
            requests_send
                .send(IncrementRequest { id, amount: 1 })
                .await
                .unwrap();
        }

        // Collect results with timeout
        let results = timeout(Duration::from_secs(5), out_recv.take(10).collect::<Vec<_>>())
            .await
            .expect("Test timed out");

        // All requests should see the same final counter value
        assert_eq!(results.len(), 10);

        // Total: 10 * 1 = 10
        for result in &results {
            assert_eq!(
                result.counter_value, 10,
                "Expected counter value of 10, got {}",
                result.counter_value
            );
        }
    }
}
