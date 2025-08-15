use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;

use hydro_lang::keyed_stream::KeyedStream;
use hydro_lang::location::tick::NoAtomic;
use hydro_lang::*;
use lattices::{Addition, AdditiveInverse};
use location::NoTick;
use serde::{Deserialize, Serialize};

/// Simple integer type that implements the needed traits for demonstration
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct SimpleInt(i32);

impl SimpleInt {
    pub fn new(value: i32) -> Self {
        SimpleInt(value)
    }

    pub fn value(&self) -> i32 {
        self.0
    }

    pub fn identity() -> Self {
        SimpleInt(0)
    }
}

impl Default for SimpleInt {
    fn default() -> Self {
        SimpleInt(0)
    }
}

impl Addition<SimpleInt> for SimpleInt {
    fn add(&mut self, other: Self) {
        self.0 += other.0;
    }

    fn add_owned(mut self, other: Self) -> Self {
        self.add(other);
        self
    }
}

impl AdditiveInverse for SimpleInt {
    fn inverse(&self) -> Self {
        SimpleInt(-self.0)
    }
}

/// Commands for counter operations
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum GroupCommand<K, V>
where
    K: Debug,
{
    /// Increment counter by a value
    Increment(K, V),
    /// Decrement counter by a value  
    Decrement(K, V),
    /// Get current counter value
    Get(K),
    /// Reset counter to identity element (0)
    Reset(K),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CounterValue<'a, T> {
    pub value: T,
    _phantom: PhantomData<&'a ()>,
}

impl<'a, T: Addition<T> + Clone> Addition<CounterValue<'a, T>> for CounterValue<'a, T> {
    fn add(&mut self, other: Self) {
        self.value.add(other.value);
    }

    fn add_owned(mut self, other: Self) -> Self {
        self.add(other);
        self
    }
}

impl<'a, T: Default> CounterValue<'a, T> {
    pub fn new(value: T) -> Self {
        CounterValue {
            value,
            _phantom: PhantomData,
        }
    }

    pub fn value(&self) -> &T {
        &self.value
    }
}

impl<'a, T: AdditiveInverse> AdditiveInverse for CounterValue<'a, T> {
    fn inverse(&self) -> Self {
        CounterValue {
            value: self.value.inverse(),
            _phantom: PhantomData,
        }
    }
}

impl<'a, T: Default> Default for CounterValue<'a, T> {
    fn default() -> Self {
        CounterValue {
            value: T::default(),
            _phantom: PhantomData,
        }
    }
}

/// Response from counter operations
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CounterResponse<K, V>
where
    K: Debug,
{
    pub key: K,
    pub value: V,
    pub operation: String,
}

/// Create a distributed counter that maintains state using group properties
#[expect(
    clippy::type_complexity,
    reason = "complex stream types for distributed systems"
)]
pub fn abelian_group_register<
    'a,
    T,
    L: Location<'a> + NoTick + NoAtomic,
    Order,
    K: Clone + Debug + Eq + Hash,
>(
    commands: KeyedStream<u64, GroupCommand<K, T>, Atomic<L>, Unbounded, Order>,
) -> (
    KeyedStream<u64, CounterResponse<K, T>, L, Unbounded, NoOrder>,
    KeyedStream<u64, (K, String), L, Unbounded, NoOrder>, // error stream
)
where
    T: Addition<T> + AdditiveInverse + Clone + Debug + Default,
{
    // Use keyed fold to maintain counter state with abelian group operations
    let commmand_stream = commands.clone().values();
    let counter_states = commmand_stream
        .filter_map(q!(|cmd| match cmd {
            GroupCommand::Increment(ref key, _)
            | GroupCommand::Decrement(ref key, _)
            | GroupCommand::Reset(ref key) => Some((key.clone(), cmd.clone())),
            GroupCommand::Get(_) => None,
        }))
        .into_keyed()
        .fold_commutative(
            q!(|| CounterValue::default()),
            q!(|counter, cmd| {
                match cmd {
                    GroupCommand::Increment(_key, delta) => {
                        counter.add(CounterValue::new(delta));
                    }
                    GroupCommand::Decrement(_key, delta) => {
                        counter.add(CounterValue::new(delta.inverse()));
                    }
                    GroupCommand::Reset(_key) => {
                        *counter = CounterValue::default();
                    }
                    _ => {}
                }
            }),
        );

    // set of operations processed in this batch
    let change_responses_prefix =
        commands
            .clone()
            .batch(nondet!(/** group of commands */))
            .entries()
            .filter_map(q!(|(c, cmd)| match cmd {
                GroupCommand::Increment(ref key, _)
                | GroupCommand::Decrement(ref key, _)
                | GroupCommand::Reset(ref key) => Some((key.clone(), (c, cmd.clone()))),
                _ => None,
            }));
    let change_responses =
        change_responses_prefix
            .join(counter_states.clone().snapshot(nondet!(/** rollup counter state */)).entries())
            .map(q!(|(k, ((c, cmd), v))| {
                (
                    c,
                    CounterResponse {
                        key: k,
                        value: v.value().clone(),
                        operation: format!("{:?}", cmd),
                    },
                )
            }))
            .into_keyed()
            .all_ticks();

    // Handle get operations by looking up current state
    let get_operations = commands.clone().filter_map(q!(|cmd| match cmd {
        GroupCommand::Get(key) => Some(key),
        _ => None,
    }));

    // For get operations, create a simple response (simplified for demo)
    let get_responses =
        get_operations
            .batch(nondet!(/** Group */))
            .entries()
            .map(q!(|(c, key)| (key, c)))
            .join(counter_states.snapshot(nondet!(/** Dingos */)).entries())
            .map(q!(|(key, (c, value))| {
                (
                    c,
                    CounterResponse {
                        key,
                        value: value.value().clone(),
                        operation: "Get".to_string(),
                    },
                )
            }))
            .into_keyed()
            .all_ticks();

    // Combine all responses
    let responses = change_responses
        .entries()
        .interleave(get_responses.entries())
        .into_keyed();

    // Error stream (empty for now, but could include validation errors)
    let errors = commands.filter_map(q!(|_| None::<(K, String)>));

    (responses, errors.entries().end_atomic().into_keyed())
}

#[cfg(test)]
mod tests {
    use futures::{SinkExt, StreamExt};

    use super::*;

    #[tokio::test]
    async fn test_counter_operations() {
        use hydro_deploy::Deployment;
        use hydro_lang::FlowBuilder;

        let mut deployment = Deployment::new();

        let flow = FlowBuilder::new();
        let process_node = flow.process::<()>();
        let external = flow.external::<()>();

        let (in_port, input, _membership, complete_sink) =
            process_node.bidi_external_many_bincode(&external);

        // Use the distributed counter
        let tick = process_node.tick();
        let (responses, _errors) =
            abelian_group_register::<SimpleInt, _, _, _>(input.atomic(&tick));
        // let out = responses.send_bincode_external(&external);

        complete_sink.complete(responses);

        let nodes = flow
            .with_process(&process_node, deployment.Localhost())
            .with_external(&external, deployment.Localhost())
            .deploy(&mut deployment);

        deployment.deploy().await.unwrap();

        let (external_out, mut external_in) = nodes.connect_bincode(in_port).await;
        // let mut external_out = nodes.connect_source_bincode(out).await;
        let mut external_out = Box::pin(external_out);

        deployment.start().await.unwrap();

        // Test increment operation
        external_in
            .send(GroupCommand::Increment(
                "test_counter".to_string(),
                SimpleInt::new(5),
            ))
            .await
            .unwrap();

        // Test decrement operation
        external_in
            .send(GroupCommand::Decrement(
                "test_counter".to_string(),
                SimpleInt::new(2),
            ))
            .await
            .unwrap();

        // Test get operation
        external_in
            .send(GroupCommand::Get("test_counter".to_string()))
            .await
            .unwrap();

        // Collect responses
        let responses: Vec<_> = external_out.by_ref().take(3).collect().await;

        // Verify we got responses for all operations
        assert_eq!(responses.len(), 3);
        dbg!(&responses);

        external_in
            .send(GroupCommand::Get("test_counter".to_string()))
            .await
            .unwrap();
        let responses: Vec<_> = external_out.by_ref().take(1).collect().await;
        assert_eq!(responses.len(), 1);
        dbg!(&responses);
        assert!(responses.iter().all(|r| r.value.value() == 3));
    }

}