use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

/// A simple counter increment request with a target count.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IncrementRequest {
    /// Unique identifier for this request
    pub id: u32,
    /// How many times to increment (remaining)
    pub remaining: u32,
}

/// Response sent when a request completes
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IncrementComplete {
    /// Request ID that completed
    pub id: u32,
    /// Final counter value when this request completed
    pub final_count: u32,
}

/// A simple counter that increments over multiple ticks.
/// 
/// This demonstrates a continuation pattern where computations take
/// multiple ticks to resolve. Each request specifies how many times
/// to increment a shared counter. The counter cycles through the dataflow,
/// accumulating increments from all active requests each tick.
pub fn counter_cycle<'a>(
    requests: Stream<IncrementRequest, Process<'a>, Unbounded>,
) -> Stream<IncrementComplete, Process<'a>, Unbounded> {
    let spinner = requests.location().spin();

    sliced! {
        // Active requests that still need processing
        let mut active = use::state_null::<Stream<_, _, _>>();
        
        // Counter state that cycles through (using a keyed stream with unit key)
        let mut counter = use::state_null::<Stream<((), u32), _, _>>();
        
        // New requests enter the system
        let new_requests = use(requests, nondet!(/** requests can arrive on any tick */));
        
        // Spin to force infinite loop for fixpoint
        let spin = use(spinner, nondet!(/** force infinite loop */));
        
        // Combine new and continuing requests
        active = active.chain(new_requests);
        
        // Initialize counter to 0 if not set
        counter = counter.chain(Singleton::new(q!(0u32), requests.location()).into_stream().map(q!(|v| ((), v))));
        
        // Count how many active requests need incrementing
        let increment_count = active
            .clone()
            .filter(q!(|req: &IncrementRequest| req.remaining > 0))
            .map(q!(|_| ((), 1u32)));
        
        // Combine counter with increments
        let counter_updates = counter.clone().chain(increment_count);
        
        // Reduce to get new counter value
        let new_counter = counter_updates
            .reduce_keyed(q!(|old: &mut u32, new: u32| *old += new))
            .map(q!(|(_key, count)| count));
        
        counter = new_counter.clone().map(q!(|v| ((), v)));
        
        // Pair each request with the current counter value
        let requests_with_count = active
            .clone()
            .cross_singleton(new_counter.clone().into_singleton());
        
        // Update request state
        let updated = requests_with_count.map(q!(|(req, count): (IncrementRequest, u32)| {
            if req.remaining > 0 {
                // Still has work to do - decrement and continue
                (
                    None,
                    Some(IncrementRequest {
                        id: req.id,
                        remaining: req.remaining - 1,
                    })
                )
            } else {
                // Complete!
                (
                    Some(IncrementComplete {
                        id: req.id,
                        final_count: count,
                    }),
                    None
                )
            }
        }));
        
        // Completed requests go to output
        let completed = updated
            .clone()
            .filter_map(q!(|(complete, _continuing)| complete));
        
        // Continuing requests loop back
        active = updated.filter_map(q!(|(_complete, continuing)| continuing));
        
        // Force spin to be used
        completed.cross_singleton(spin.count().into_singleton()).map(q!(|(c, _)| c))
    }
}

#[cfg(test)]
mod tests {
    use futures::{SinkExt, StreamExt};
    use hydro_deploy::Deployment;
    use hydro_lang::location::Location;
    use tokio::time::{timeout, Duration};

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_counter_cycle_basic() {
        let mut deployment = Deployment::new();

        let builder = hydro_lang::compile::builder::FlowBuilder::new();
        let external = builder.external::<()>();
        let p1 = builder.process();

        let (requests_send, requests) = p1.source_external_bincode(&external);
        let out = super::counter_cycle(requests);
        let out_recv = out.send_bincode_external(&external);

        let mut built = builder.with_default_optimize();

        let nodes = built
            .with_process(&p1, deployment.Localhost())
            .with_external(&external, deployment.Localhost())
            .deploy(&mut deployment);

        deployment.deploy().await.unwrap();

        let mut requests_send = nodes.connect(requests_send).await;
        let mut out_recv = nodes.connect(out_recv).await;

        deployment.start().await.unwrap();

        // Send two requests
        requests_send
            .send(IncrementRequest { id: 1, remaining: 3 })
            .await
            .unwrap();
        requests_send
            .send(IncrementRequest { id: 2, remaining: 5 })
            .await
            .unwrap();

        // Collect results with timeout
        let results = timeout(Duration::from_secs(5), out_recv.take(2).collect::<Vec<_>>())
            .await
            .expect("Test timed out");

        // Both requests should complete
        assert_eq!(results.len(), 2);

        // Total increments: 3 + 5 = 8
        for result in &results {
            assert_eq!(
                result.final_count, 8,
                "Expected final count of 8, got {}",
                result.final_count
            );
        }

        // Check both IDs completed
        let mut ids: Vec<u32> = results.iter().map(|r| r.id).collect();
        ids.sort();
        assert_eq!(ids, vec![1, 2]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_counter_cycle_concurrent() {
        let mut deployment = Deployment::new();

        let builder = hydro_lang::compile::builder::FlowBuilder::new();
        let external = builder.external::<()>();
        let p1 = builder.process();

        let (requests_send, requests) = p1.source_external_bincode(&external);
        let out = super::counter_cycle(requests);
        let out_recv = out.send_bincode_external(&external);

        let mut built = builder.with_default_optimize();

        let nodes = built
            .with_process(&p1, deployment.Localhost())
            .with_external(&external, deployment.Localhost())
            .deploy(&mut deployment);

        deployment.deploy().await.unwrap();

        let mut requests_send = nodes.connect(requests_send).await;
        let mut out_recv = nodes.connect(out_recv).await;

        deployment.start().await.unwrap();

        // Send 10 concurrent requests, each needing 3 increments
        for id in 0..10 {
            requests_send
                .send(IncrementRequest { id, remaining: 3 })
                .await
                .unwrap();
        }

        // Collect results with timeout
        let results = timeout(Duration::from_secs(5), out_recv.take(10).collect::<Vec<_>>())
            .await
            .expect("Test timed out");

        // All 10 requests should complete
        assert_eq!(results.len(), 10);

        // Total increments: 10 * 3 = 30
        for result in &results {
            assert_eq!(
                result.final_count, 30,
                "Expected final count of 30, got {}",
                result.final_count
            );
        }

        // Check all IDs completed
        let mut ids: Vec<u32> = results.iter().map(|r| r.id).collect();
        ids.sort();
        assert_eq!(ids, (0..10).collect::<Vec<_>>());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_counter_cycle_staggered() {
        let mut deployment = Deployment::new();

        let builder = hydro_lang::compile::builder::FlowBuilder::new();
        let external = builder.external::<()>();
        let p1 = builder.process();

        let (requests_send, requests) = p1.source_external_bincode(&external);
        let out = super::counter_cycle(requests);
        let out_recv = out.send_bincode_external(&external);

        let mut built = builder.with_default_optimize();

        let nodes = built
            .with_process(&p1, deployment.Localhost())
            .with_external(&external, deployment.Localhost())
            .deploy(&mut deployment);

        deployment.deploy().await.unwrap();

        let mut requests_send = nodes.connect(requests_send).await;
        let mut out_recv = nodes.connect(out_recv).await;

        deployment.start().await.unwrap();

        // Send requests with different remaining counts
        requests_send
            .send(IncrementRequest { id: 1, remaining: 1 })
            .await
            .unwrap();
        requests_send
            .send(IncrementRequest { id: 2, remaining: 2 })
            .await
            .unwrap();
        requests_send
            .send(IncrementRequest { id: 3, remaining: 3 })
            .await
            .unwrap();

        // Collect results with timeout
        let results = timeout(Duration::from_secs(5), out_recv.take(3).collect::<Vec<_>>())
            .await
            .expect("Test timed out");

        // All 3 requests should complete
        assert_eq!(results.len(), 3);

        // Total increments: 1 + 2 + 3 = 6
        for result in &results {
            assert_eq!(
                result.final_count, 6,
                "Expected final count of 6, got {}",
                result.final_count
            );
        }
    }
}
