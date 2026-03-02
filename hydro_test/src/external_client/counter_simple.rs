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
    let tick = requests.location().tick();

    // Ensure ticks advance even when there is no new input.
    tick.spin_batch(q!(1)).all_ticks().for_each(q!(|_| {}));

    // Preserve total order of incoming requests.
    let ordered_requests = requests
        .entries()
        .assume_ordering::<TotalOrder>(nondet!(/** preserve original total order */));

    // Batch incoming requests by tick.
    let tick_requests = ordered_requests
        .batch(&tick, nondet!(/** batch boundaries are non-deterministic */));

    // Cycle to carry unfinished requests to the next tick.
    let (carry_handle, carry_stream) = tick.cycle::<Stream<(u64, IncrementRequest), Tick<Process<'a, P>>, Bounded, TotalOrder>>();

    // Active requests for this tick: carried over + newly batched.
    let active_requests = carry_stream.chain(tick_requests);

    // Process requests in order, decrementing the shared threshold.
    // Accumulator: (remaining_threshold, total_consumed_in_tick)
    let processed = active_requests.scan(
        q!(move || (threshold, 0u32)),
        q!(move |state, (client_id, req): (u64, IncrementRequest)| {
            let (remaining_threshold, total_consumed) = state;

            let remaining = if *remaining_threshold == 0 {
                req.amount
            } else if req.amount <= *remaining_threshold {
                *remaining_threshold -= req.amount;
                *total_consumed += req.amount;
                0
            } else {
                let leftover = req.amount - *remaining_threshold;
                *total_consumed += *remaining_threshold;
                *remaining_threshold = 0;
                leftover
            };

            Some((client_id, req, remaining, *total_consumed))
        }),
    );

    // Reinsert unfinished requests on the next tick.
    let carry_next = processed
        .clone()
        .filter(q!(|(_, _, remaining, _)| *remaining > 0))
        .map(q!(|(client_id, req, remaining, _)| {
            (client_id, IncrementRequest { id: req.id, amount: remaining })
        }));
    carry_handle.complete_next_tick(carry_next);

    // Emit responses only when the request amount reaches 0.
    let responses = processed
        .filter(q!(|(_, _, remaining, _)| *remaining == 0))
        .map(q!(|(client_id, req, _remaining, consumed)| {
            (client_id, IncrementResponse {
                id: req.id,
                remaining: 0,
                counter_value: consumed,
                shutdown: false,
            })
        }))
        .into_keyed()
        .all_ticks();

    responses
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{SinkExt, StreamExt};
    use hydro_deploy::Deployment;
    use hydro_lang::prelude::FlowBuilder;
    use tokio::time::{timeout, Duration};

    #[tokio::test(flavor = "multi_thread")]
    async fn test_request_reinserted_until_complete() {
        let mut deployment = Deployment::new();

        let mut builder = FlowBuilder::new();
        let external = builder.external::<()>();
        let p1: Process<'_, ()> = builder.process();

        let (port, requests, _membership, output_ref) =
            p1.bidi_external_many_bincode(&external);
        output_ref.complete(counter_server::<_>(requests, 10u32));

        let built = builder.with_default_optimize();

        let nodes = built
            .with_process(&p1, deployment.Localhost())
            .with_external(&external, deployment.Localhost())
            .deploy(&mut deployment);

        deployment.deploy().await.unwrap();

        let (mut output_stream, mut input_sink) = nodes.connect_bincode(port).await;

        deployment.start().await.unwrap();

        input_sink
            .send(IncrementRequest { id: 1, amount: 25 })
            .await
            .unwrap();

        let results = timeout(Duration::from_secs(5), output_stream.take(1).collect::<Vec<_>>())
            .await
            .expect("Test timed out");

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, 1);
        assert_eq!(results[0].remaining, 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_multiple_requests_complete_after_threshold_reached() {
        let mut deployment = Deployment::new();

        let mut builder = FlowBuilder::new();
        let external = builder.external::<()>();
        let p1: Process<'_, ()> = builder.process();

        let (port, requests, _membership, output_ref) =
            p1.bidi_external_many_bincode(&external);
        output_ref.complete(counter_server::<_>(requests, 10u32));

        let built = builder.with_default_optimize();

        let nodes = built
            .with_process(&p1, deployment.Localhost())
            .with_external(&external, deployment.Localhost())
            .deploy(&mut deployment);

        deployment.deploy().await.unwrap();

        let (mut output_stream, mut input_sink) = nodes.connect_bincode(port).await;

        deployment.start().await.unwrap();

        input_sink
            .send(IncrementRequest { id: 1, amount: 6 })
            .await
            .unwrap();
        input_sink
            .send(IncrementRequest { id: 2, amount: 7 })
            .await
            .unwrap();
        input_sink
            .send(IncrementRequest { id: 3, amount: 4 })
            .await
            .unwrap();

        let results = timeout(Duration::from_secs(5), output_stream.take(3).collect::<Vec<_>>())
            .await
            .expect("Test timed out");

        assert_eq!(results.len(), 3);
        let mut ids: Vec<u32> = results.iter().map(|r| r.id).collect();
        ids.sort();
        assert_eq!(ids, vec![1, 2, 3]);

        for result in &results {
            assert_eq!(result.remaining, 0);
        }
    }
}