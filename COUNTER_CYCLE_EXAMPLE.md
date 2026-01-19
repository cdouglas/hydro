# Counter Cycle Example

This document describes a simple counter example demonstrating dataflow cycles in Hydro.

## Simple Counter (No Cycles) ✅

**Location:** `hydro/hydro_test/src/local/counter_simple.rs`

**Status:** Working and tested

### What it does:
- Accepts increment requests with an `amount` field
- Accumulates all increments in a single tick using `fold`
- Returns the final counter value to each request
- All requests see the same final counter value

### Key patterns demonstrated:
1. **Using `sliced!` macro** for dataflow composition
2. **Using `fold`** to accumulate values: `.fold(q!(|| 0), q!(|a: &mut u32, b| *a += b))`
3. **Using `cross_singleton`** to broadcast a singleton value to a stream
4. **Proper test structure** with external connections

### Example:
```rust
// Send 3 requests: amounts 5, 3, 2
// All requests receive counter_value = 10 (5+3+2)
```

### Tests:
- `test_counter_simple_basic`: 3 requests with different amounts
- `test_counter_simple_many`: 10 requests with amount=1 each

Both tests pass successfully.

## Next Step: Counter with Cycles

The next step is to modify `counter_simple` to add a cycle where:
1. The counter persists across ticks (not just within one tick)
2. Each request specifies `remaining` iterations
3. Each tick, the counter increments by 1 for each active request
4. Requests complete when `remaining` reaches 0

### Pattern to use:
Based on `graph_reachability.rs`, the cycle pattern uses:
- `use::state_null::<Stream<_, _, _>>()` to maintain state across iterations
- `chain()` to combine new data with cycled data
- A spinner to force infinite loop for fixpoint: `requests.location().spin()`
- `cross_singleton(spin.count())` to force the spin to be used

### Key differences from simple version:
- State cycles through the dataflow using `chain()` and implicit feedback
- Multiple ticks are required to complete requests
- Need to track which requests are still active

## Cycle Examples in Hydro

### Graph Reachability (`graph_reachability.rs`)
Shows how to:
- Use `use::state_null` for cyclic state
- Chain new data with existing state
- Use a spinner for fixpoint iteration
- Handle cycles with `NoOrder` streams

### K-means (in `dfir_rs/tests/surface_loop.rs`)
Shows how to:
- Use explicit `loop { ... }` blocks
- Use `next_iteration()` to pass data across loop iterations
- Use `reduce_keyed` for aggregation within loops
- Handle multiple iterations with `repeat_n()`

## Implementation Notes

### Why the simple counter works:
1. All data arrives in a single tick
2. `fold` accumulates within that tick
3. `cross_singleton` broadcasts the result to all requests
4. No state needs to persist across ticks

### Why cycles are needed for continuation:
1. Requests take multiple ticks to complete
2. Counter state must persist and accumulate across ticks
3. Active requests must be tracked and decremented each tick
4. Completed requests must be filtered out

### Testing gotchas:
1. **Send data AFTER `deployment.start()`** - not before!
2. Use `timeout()` to prevent hanging tests
3. Run tests with `--exact` to avoid running multiple tests concurrently
4. The deployment process takes ~10-20 seconds even for simple tests

## Files Created

- `hydro/hydro_test/src/local/counter_simple.rs` - Working simple counter (no cycles)
- `hydro/hydro_test/src/local/counter_cycle.rs` - Placeholder for cycle version (not working yet)

## References

- Graph reachability example: `hydro/hydro_test/src/local/graph_reachability.rs`
- Chat app example: `hydro/hydro_test/src/local/chat_app.rs`
- Count elements example: `hydro/hydro_test/src/local/count_elems.rs`
- DFIR loop examples: `hydro/dfir_rs/tests/surface_loop.rs`
