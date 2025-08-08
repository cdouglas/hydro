use std::hash::Hash;

use hydro_lang::{location::tick::NoAtomic, *};
use hydro_lang::keyed_stream::KeyedStream;
use lattices::algebra::abelian_group;
use location::NoTick;
use serde::{Deserialize, Serialize};

/// Commands for counter operations using abelian group structure
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum CounterCommand<K> {
    /// Increment counter by a value (uses group operation)
    Increment(K, i32),
    /// Decrement counter by a value (uses group inverse operation)
    Decrement(K, i32),
    /// Get current counter value
    Get(K),
    /// Reset counter to identity element (0)
    Reset(K),
}

/// Response from counter operations
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CounterResponse<K> {
    pub key: K,
    pub value: i32,
    pub operation: String,
}

/// Abelian group operations for integer counters
const COUNTER_IDENTITY: i32 = 0;

/// Verify that our counter operations form an abelian group
fn verify_abelian_group() -> Result<(), &'static str> {
    let test_items = [-100, -10, -1, 0, 1, 10, 100, 1000, 10000];
    abelian_group(
        &test_items, 
        &|a: i32, b: i32| a.wrapping_add(b), 
        COUNTER_IDENTITY, 
        &|a: i32| a.wrapping_neg()
    )
 //    abelian_group(TEST_ITEMS, &u32::wrapping_add, 0, &|x| 0u32.wrapping_sub(x)).is_ok()
}

/// Create a distributed counter that maintains state using abelian group properties
#[expect(clippy::type_complexity, reason = "complex stream types for distributed systems")]
pub fn local_counter<'a, L: Location<'a> + NoTick + NoAtomic, Order, K: Clone + Eq + Hash>(
    commands: KeyedStream<u64, CounterCommand<K>, Atomic<L>, Unbounded, Order>,
) -> (
    KeyedStream<u64, CounterResponse<K>, L, Unbounded, NoOrder>,
    KeyedStream<u64, (K, String), L, Unbounded, NoOrder>, // error stream
) {
    // Verify abelian group properties at startup
    let _verification = verify_abelian_group();
    
    // Convert commands to (key, operation) pairs for state tracking
    let operations = commands.clone().filter_map(q!(|cmd| match cmd {
        CounterCommand::Increment(key, delta) => Some((key, delta)),
        CounterCommand::Decrement(key, delta) => Some((key, -delta)), // Use abelian group inverse
        CounterCommand::Reset(key) => Some((key, i32::MIN)), // Special marker for reset
        CounterCommand::Get(_) => None, // Get operations don't modify state
    }));

    // Use keyed fold to maintain counter state with abelian group operations
    let counter_states = operations.clone().values()
        .into_keyed()
        .fold_commutative(
            q!(|| 0i32), // Start with identity element
            q!(|counter: &mut i32, delta| {
                if delta == i32::MIN {
                    // Reset operation - set to identity element
                    *counter = 0; // COUNTER_IDENTITY
                } else {
                    // Use abelian group operation (addition)
                    *counter = counter.wrapping_add(delta);
                }
            }),
        );

    // set of operations processed in this batch
    let change_responses = unsafe {
        operations.batch()
        .entries()
        .map(q!(|(c, (k, d))| (k, (c, d))))
        .join(counter_states.clone().snapshot().entries())
        .map(q!(|(k, ((c, d), v))| {
            (c,
            CounterResponse {
                key: k,
                value: v,
                operation: if d == i32::MIN {
                    "reset".to_string()
                } else if d > 0 {
                    "increment".to_string()
                } else {
                    "decrement".to_string()
                },
            })
        }))
        .into_keyed()
        .all_ticks()
    };
    
    // // Create responses for state changes
    // let tick = commands.atomic_source();
    // let current_state = unsafe {
    //     counter_states
    //         .snapshot(tick) // snapshot current state at the tick
    //         .entries()
    // };

    // let change_responses = unsafe { counter_states
    //     .snapshot()
    //     .entries()
    //     .map(q!(|(key, value)| {
    //         CounterResponse {
    //             key,
    //             value,
    //             operation: "update".to_string(),
    //         }
    //     }))};
    
    // Handle get operations by looking up current state
    let get_operations = commands.clone().filter_map(q!(|cmd| match cmd {
        CounterCommand::Get(key) => Some(key),
        _ => None,
    }));
    
    // For get operations, create a simple response (simplified for demo)
    let get_responses = unsafe {
        get_operations.batch()
        .entries()
        .map(q!(|(c, key)|(key, c)))
        .join(counter_states.snapshot().entries())
        .map(q!(|(key, (c, value))| {
            (c,
            CounterResponse {
                key,
                value,
                operation: "get".to_string(),
            })
        }))
        .into_keyed()
        .all_ticks()
    };
    
    // Combine all responses
    let responses = change_responses
        .entries().union(get_responses.entries()).into_keyed();
    
    // Error stream (empty for now, but could include validation errors)
    let errors = commands.filter_map(q!(|_| None::<(K, String)>));
    
    (responses, errors.entries().end_atomic().into_keyed())
}

/// Create a distributed counter (alias for local_counter for backward compatibility)
#[expect(clippy::type_complexity, reason = "complex stream types for distributed systems")]
pub fn distributed_counter<'a, L: Location<'a> + NoTick + NoAtomic, Order, K: Clone + Eq + Hash>(
    commands: KeyedStream<u64, CounterCommand<K>, Atomic<L>, Unbounded, Order>,
) -> (
    KeyedStream<u64, CounterResponse<K>, L, Unbounded, NoOrder>,
    KeyedStream<u64, (K, String), L, Unbounded, NoOrder>, // error stream
) {
    local_counter(commands)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::SinkExt;
    use futures::StreamExt;

    #[test]
    fn test_abelian_group_verification() {
        assert!(verify_abelian_group().is_ok());
    }

    #[tokio::test]
    async fn test_counter_operations() {
        use hydro_deploy::Deployment;
        use hydro_lang::FlowBuilder;
        use std::collections::HashSet;

        let mut deployment = Deployment::new();

        let flow = FlowBuilder::new();
        let process_node = flow.process::<()>();
        let external = flow.external::<()>();

        let (in_port, input, _membership, complete_sink) =
            process_node.bidi_external_many_bincode(&external);
        
        // Use the distributed counter
        let tick = process_node.tick();
        let (responses, _errors) = distributed_counter(input.atomic(&tick));
        // let out = responses.send_bincode_external(&external);
        
        complete_sink.complete(responses);

        let nodes = flow
            .with_process(&process_node, deployment.Localhost())
            .with_external(&external, deployment.Localhost())
            .deploy(&mut deployment);

        deployment.deploy().await.unwrap();

        let (external_out, mut external_in) = nodes.connect_bincode(in_port).await;
        // let mut external_out = nodes.connect_source_bincode(out).await;

        deployment.start().await.unwrap();

        // Test increment operation
        external_in.send(CounterCommand::Increment("test_counter".to_string(), 5)).await.unwrap();
        
        // Test decrement operation  
        external_in.send(CounterCommand::Decrement("test_counter".to_string(), 2)).await.unwrap();
        
        // Test get operation
        external_in.send(CounterCommand::Get("test_counter".to_string())).await.unwrap();

        // Collect responses
        let responses: Vec<_> = external_out.take(3).collect().await;
        
        // Verify we got responses for all operations
        assert_eq!(responses.len(), 3);
        dbg!(&responses);
        
        // Check that we have the expected operations
        let operations: HashSet<_> = responses.iter().map(|r| r.operation.as_str()).collect();
        assert!(operations.contains("increment") || operations.contains("update"));
        assert!(operations.contains("decrement") || operations.contains("update"));
        assert!(operations.contains("get") || operations.contains("update"));
    }
}
