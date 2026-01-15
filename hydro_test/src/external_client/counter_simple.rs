use hydro_lang::live_collections::stream::TotalOrder;
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
    /// Unused part of the increment request
    pub remaining: u32,
    /// Counter value after this increment
    pub counter_value: u32,
    /// Whether threshold was reached (kept for compatibility)
    pub shutdown: bool,
}
pub fn counter_server<'a, P>(
    requests: KeyedStream<u64, IncrementRequest, Process<'a, P>, Unbounded, TotalOrder>,
    threshold: u32,
) -> KeyedStream<u64, IncrementResponse, Process<'a, P>, Unbounded> {
    sliced! {
        let batch_requests = use(requests.clone(), nondet!(/** requests arrive */));

        // Re-key everything to a single global key (0) so the fold is truly global
        let unified = batch_requests
            .clone()
            .values()                          // drop the original client key
            .map(q!(|req| (0u64, req)))        // attach a single shared key
            .into_keyed()                      // make it a keyed stream on key 0
            .assume_ordering::<TotalOrder>(nondet!(/** enforce global order */));

        // Fold globally: (total_sum, first_breaker_id, remaining_excess)
        let final_state = unified
            .clone()
            .fold(
                q!(|| (0u32, None::<u32>, 0u32)),
                q!(move |state: &mut (u32, Option<u32>, u32), req: IncrementRequest| {
                    if state.1.is_none() {
                        let new_sum = state.0.saturating_add(req.amount);
                        if new_sum >= threshold {
                            state.1 = Some(req.id);
                            state.2 = new_sum - threshold;
                        }
                        state.0 = new_sum;
                    }
                }),
            )
            .into_singleton();

        // Cross back with original keyed requests to preserve routing
        batch_requests
            .cross_singleton(final_state)
            .map(q!(|(req, state_map)| {
                // Global state is keyed only by 0
                let (counter_value, breaker_id, excess) = state_map.get(&0u64).cloned().unwrap_or((0, None, 0));

                let remaining = if let Some(bid) = breaker_id {
                    if req.id == bid {
                        // Breaker message returns the excess over threshold
                        excess
                    } else if req.id > bid {
                        // After breaker: return full amount
                        req.amount
                    } else {
                        // Before breaker: nothing remaining
                        0
                    }
                } else {
                    // Threshold not yet reached
                    0
                };

                IncrementResponse {
                    id: req.id,
                    remaining,
                    counter_value,
                    shutdown: breaker_id.is_some(),
                }
            }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{SinkExt, StreamExt};
    use hydro_deploy::Deployment;
    use hydro_lang::location::{Location, NetworkHint};
    use hydro_lang::prelude::FlowBuilder;
    use tokio::time::{timeout, Duration};

    #[tokio::test(flavor = "multi_thread")]
    async fn test_counter_threshold_not_reached() {
        let mut deployment = Deployment::new();

        let builder = FlowBuilder::new();
        let external = builder.external::<()>();
        let p1: Process<'_, ()> = builder.process();

        let (port, requests, _membership, output_ref) =
            p1.bidi_external_many_bincode(&external);
        output_ref.complete(counter_server::<_>(requests, 100u32));

        let built = builder.with_default_optimize();

        let nodes = built
            .with_process(&p1, deployment.Localhost())
            .with_external(&external, deployment.Localhost())
            .deploy(&mut deployment);

        deployment.deploy().await.unwrap();

        let (mut output_stream, mut input_sink) = nodes.connect_bincode(port).await;

        deployment.start().await.unwrap();

        // Send requests that don't reach threshold
        input_sink
            .send(IncrementRequest { id: 1, amount: 10 })
            .await
            .unwrap();
        input_sink
            .send(IncrementRequest { id: 2, amount: 20 })
            .await
            .unwrap();
        input_sink
            .send(IncrementRequest { id: 3, amount: 30 })
            .await
            .unwrap();

        // Collect responses
        let results = timeout(Duration::from_secs(5), output_stream.take(3).collect::<Vec<_>>())
            .await
            .expect("Test timed out");

        assert_eq!(results.len(), 3);

        // Total: 10 + 20 + 30 = 60 (below threshold of 100)
        for result in &results {
            assert_eq!(result.counter_value, 60);
            assert!(!result.shutdown, "Should not shutdown below threshold");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_counter_threshold_reached() {
        let mut deployment = Deployment::new();

        let builder = FlowBuilder::new();
        let external = builder.external::<()>();
        let p1: Process<'_, ()> = builder.process();

        let (port, requests, _membership, output_ref) =
            p1.bidi_external_many_bincode(&external);
        output_ref.complete(counter_server::<_>(requests, 50u32));

        let built = builder.with_default_optimize();

        let nodes = built
            .with_process(&p1, deployment.Localhost())
            .with_external(&external, deployment.Localhost())
            .deploy(&mut deployment);

        deployment.deploy().await.unwrap();

        let (mut output_stream, mut input_sink) = nodes.connect_bincode(port).await;

        deployment.start().await.unwrap();

        // Send requests that reach threshold
        // id=1, amount=20: sum=20
        // id=2, amount=15: sum=35
        // id=3, amount=25: sum=60 (breaker, excess=10)
        input_sink
            .send(IncrementRequest { id: 1, amount: 20 })
            .await
            .unwrap();
        input_sink
            .send(IncrementRequest { id: 2, amount: 15 })
            .await
            .unwrap();
        input_sink
            .send(IncrementRequest { id: 3, amount: 25 })
            .await
            .unwrap();

        // Collect responses
        let results = timeout(Duration::from_secs(5), output_stream.take(3).collect::<Vec<_>>())
            .await
            .expect("Test timed out");

        assert_eq!(results.len(), 3);

        // Total: 20 + 15 + 25 = 60 (above threshold of 50)
        let result1 = results.iter().find(|r| r.id == 1).expect("Response 1 missing");
        let result2 = results.iter().find(|r| r.id == 2).expect("Response 2 missing");
        let result3 = results.iter().find(|r| r.id == 3).expect("Response 3 missing");

        assert_eq!(result1.counter_value, 60);
        assert_eq!(result1.remaining, 0, "Message 1 before breaker should have 0 remaining");
        assert!(result1.shutdown);

        assert_eq!(result2.counter_value, 60);
        assert_eq!(result2.remaining, 0, "Message 2 before breaker should have 0 remaining");
        assert!(result2.shutdown);

        assert_eq!(result3.counter_value, 60);
        assert_eq!(result3.remaining, 10, "Message 3 (breaker) should have excess of 10");
        assert!(result3.shutdown);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_counter_threshold_exact() {
        let mut deployment = Deployment::new();

        let builder = FlowBuilder::new();
        let external = builder.external::<()>();
        let p1: Process<'_, ()> = builder.process();

        let (port, requests, _membership, output_ref) =
            p1.bidi_external_many_bincode(&external);
        output_ref.complete(counter_server::<_>(requests, 30u32));

        let built = builder.with_default_optimize();

        let nodes = built
            .with_process(&p1, deployment.Localhost())
            .with_external(&external, deployment.Localhost())
            .deploy(&mut deployment);

        deployment.deploy().await.unwrap();

        let (mut output_stream, mut input_sink) = nodes.connect_bincode(port).await;

        deployment.start().await.unwrap();

        // Send requests that exactly reach threshold
        input_sink
            .send(IncrementRequest { id: 1, amount: 10 })
            .await
            .unwrap();
        input_sink
            .send(IncrementRequest { id: 2, amount: 10 })
            .await
            .unwrap();
        input_sink
            .send(IncrementRequest { id: 3, amount: 10 })
            .await
            .unwrap();

        // Collect responses
        let results = timeout(Duration::from_secs(5), output_stream.take(3).collect::<Vec<_>>())
            .await
            .expect("Test timed out");

        assert_eq!(results.len(), 3);

        // Total: 10 + 10 + 10 = 30 (exactly at threshold)
        let result1 = results.iter().find(|r| r.id == 1).expect("Response 1 missing");
        let result2 = results.iter().find(|r| r.id == 2).expect("Response 2 missing");
        let result3 = results.iter().find(|r| r.id == 3).expect("Response 3 missing");

        assert_eq!(result1.counter_value, 30);
        assert_eq!(result1.remaining, 0, "Message 1 before breaker should have 0 remaining");
        assert!(result1.shutdown);

        assert_eq!(result2.counter_value, 30);
        assert_eq!(result2.remaining, 0, "Message 2 before breaker should have 0 remaining");
        assert!(result2.shutdown);

        assert_eq!(result3.counter_value, 30);
        assert_eq!(result3.remaining, 0, "Message 3 (breaker) has no excess, should be 0");
        assert!(result3.shutdown);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_counter_with_messages_after_breaker() {
        let mut deployment = Deployment::new();

        let builder = FlowBuilder::new();
        let external = builder.external::<()>();
        let p1: Process<'_, ()> = builder.process();

        let (port, requests, _membership, output_ref) =
            p1.bidi_external_many_bincode(&external);
        output_ref.complete(counter_server::<_>(requests, 50u32));

        let built = builder.with_default_optimize();

        let nodes = built
            .with_process(&p1, deployment.Localhost())
            .with_external(&external, deployment.Localhost())
            .deploy(&mut deployment);

        deployment.deploy().await.unwrap();

        let (mut output_stream, mut input_sink) = nodes.connect_bincode(port).await;

        deployment.start().await.unwrap();

        // Send requests:
        // id=1, amount=20: sum=20
        // id=2, amount=15: sum=35
        // id=3, amount=25: sum=60 (breaker, excess=10)
        // id=4, amount=30: sum=90 (after breaker)
        input_sink
            .send(IncrementRequest { id: 1, amount: 20 })
            .await
            .unwrap();
        input_sink
            .send(IncrementRequest { id: 2, amount: 15 })
            .await
            .unwrap();
        input_sink
            .send(IncrementRequest { id: 3, amount: 25 })
            .await
            .unwrap();
        input_sink
            .send(IncrementRequest { id: 4, amount: 30 })
            .await
            .unwrap();

        // Collect responses
        let results = timeout(Duration::from_secs(5), output_stream.take(4).collect::<Vec<_>>())
            .await
            .expect("Test timed out");

        assert_eq!(results.len(), 4);

        let result1 = results.iter().find(|r| r.id == 1).expect("Response 1 missing");
        let result2 = results.iter().find(|r| r.id == 2).expect("Response 2 missing");
        let result3 = results.iter().find(|r| r.id == 3).expect("Response 3 missing");
        let result4 = results.iter().find(|r| r.id == 4).expect("Response 4 missing");

        // Counter stops accumulating after the breaker; it stays at 60
        assert_eq!(result1.counter_value, 60);
        assert_eq!(result2.counter_value, 60);
        assert_eq!(result3.counter_value, 60);
        assert_eq!(result4.counter_value, 60);

        // Message 1 & 2: before breaker, 0 remaining
        assert_eq!(result1.remaining, 0, "Message 1 before breaker");
        assert_eq!(result2.remaining, 0, "Message 2 before breaker");

        // Message 3: breaker, excess = 60 - 50 = 10
        assert_eq!(result3.remaining, 10, "Message 3 (breaker) excess");

        // Message 4: after breaker, full amount
        assert_eq!(result4.remaining, 30, "Message 4 after breaker should return full amount");

        // All shutdown
        assert!(result1.shutdown);
        assert!(result2.shutdown);
        assert!(result3.shutdown);
        assert!(result4.shutdown);
    }
}