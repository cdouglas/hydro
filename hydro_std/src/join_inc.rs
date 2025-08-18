use std::fmt::Debug;
use std::hash::Hash;

use hydro_lang::keyed_stream::KeyedStream;
use hydro_lang::location::tick::NoAtomic;
use hydro_lang::*;
use location::NoTick;
use serde::{Deserialize, Serialize};

/// placeholder (replace w/ veriadics?)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum RawTuple {
    R { a: u32, b: u32 },
    S { a: u32, c: u32, d: u32 },
    T { a: u32, b: u32, c: u32, d: u32 }, // result
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ZTuple
{
    pub tuple: RawTuple,
    pub count: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Op {
    Insert { id: u32, tuple: ZTuple },
    Get { id: u32, key: u32 },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum OpResponse {
    Insert { id: u32 },
    Get { id: u32, tuple: Vec<ZTuple> },
}

pub fn demux_rs<'a,
    L: Location<'a> + NoTick + NoAtomic,
    Order,>(
    stream: KeyedStream<u64, ZTuple, Atomic<L>, Unbounded, Order>,
) -> (
    KeyedStream<(u32, u32), i32, Atomic<L>, Unbounded, NoOrder>,
    KeyedStream<(u32, u32, u32), i32, Atomic<L>, Unbounded, NoOrder>,
) {
    let r_stream = stream.clone()
        .values()
        .filter_map(q!(|ztuple| match ztuple.tuple {
            RawTuple::R { a, b } => Some(((a, b), ztuple.count)),
            _ => None,
        }))
        .into_keyed();

    let s_stream = stream.clone()
        .values()
        .filter_map(q!(|ztuple| match ztuple.tuple {
            RawTuple::S { a, c, d } => Some(((a, c, d), ztuple.count)),
            _ => None,
        }))
        .into_keyed();

    (r_stream, s_stream)
}

pub fn inc_join<
    'a,
    L: Location<'a> + NoTick + NoAtomic,
    Order,
>(
    ops: KeyedStream<u64, Op, Atomic<L>, Unbounded, Order>,
) -> (
    KeyedStream<u64, OpResponse, L, Unbounded, NoOrder>,
    KeyedStream<u64, String, L, Unbounded, NoOrder>,
) {
    let (r_stream, s_stream) = demux_rs(ops.clone()
        .filter_map(q!(|op| match op {
            Op::Insert { tuple, .. } => Some(tuple),
            Op::Get { .. } => None
        })));

    let r =
        r_stream.clone()
        .fold_commutative(
            q!(|| 0i32),
            q!(|acc, count| *acc += count))
        .snapshot(nondet!(/** rollup R state */))
        .entries()
        .defer_tick()
        .inspect(q!(|((a, b), count)| { println!("R state: ({}, {})#{}", a, b, count); }))
        .filter(q!(|(_, count)| *count != 0));

    let s =
        s_stream.clone()
        .fold_commutative(
            q!(|| 0i32),
            q!(|acc, count| *acc += count))
        .snapshot(nondet!(/** rollup S state */))
        .entries()
        .defer_tick()
        .inspect(q!(|((a, c, d), count)| { println!("S state: ({}, {}, {})#{}", a, c, d, count); }))
        .filter(q!(|(_, count)| *count != 0));

    let delta_r =  r_stream.clone().batch(nondet!(/** group of commands */));
    let delta_s =  s_stream.clone().batch(nondet!(/** group of commands */));

    r_stream.clone().entries().for_each(q!(|((a, b), count)| {
        println!("delta R: ({}, {})#{}", a, b, count);
    }));
    s_stream.clone().entries().for_each(q!(|((a, c, d), count)| {
        println!("delta S: ({}, {}, {})#{}", a, c, d, count);
    }));

    // ΔR x ΔS (new R tuples × new S tuples in same tick)
    let delta_r_x_delta_s =
        delta_r
            .clone()
            .entries()
            .map(q!(|((a, b), r_count)| (a, (b, r_count))))
            .join(delta_s
                .clone()
                .entries()
                .map(q!(|((a, c, d), s_count)| (a, (c, d, s_count)))))
            .map(q!(|(a, ((b, r_count), (c, d, s_count)))| ((a, b, c, d), r_count * s_count)));

    // R × ΔS (existing R tuples × new S tuples)
    let r_x_delta_s =
        r
            .clone()
            .map(q!(|((a, b), r_count)| (a, (b, r_count))))
            .join(delta_s
                    .entries()
                    .map(q!(|((a, c, d), s_count)| (a, (c, d, s_count)))))
            .map(q!(|(a, ((b, r_count), (c, d, s_count)))| ((a, b, c, d), r_count * s_count)));

    // S × ΔR (existing S tuples x new R tuples)
    let s_x_delta_r =
        s
            .clone()
            .map(q!(|((a, c, d), s_count)| (a, (c, d, s_count))))
            .join(delta_r
                .entries()
                .map(q!(|((a, b), r_count)| (a, (b, r_count)))))
            .map(q!(|(a, ((c, d, s_count), (b, r_count)))| ((a, b, c, d), r_count * s_count)));

    let join_result = s_x_delta_r
                .chain(r_x_delta_s)
                .chain(delta_r_x_delta_s)
                .into_keyed()
                .fold_commutative(
                    q!(|| 0i32),
                    q!(|acc, count| *acc += count))
                .filter(q!(|count| *count != 0));

    join_result.clone().entries()
            .map(q!(|((a, b, c, d), count)| (a, b, c, d, count)))
            .all_ticks()
            .for_each(q!(|(a, b, c, d, count)| {
        println!("output delta: ({}, {}, {}, {})#{}", a, b, c, d, count);
    }));

    // let key = 1;
    // let tmp = join_result.clone().entries()
    //     .filter_map(q!(|((a, b, c, d), count)| {
    //         if a == key {
    //             Some(ZTuple {
    //                 tuple: RawTuple::T { a, b, c, d },
    //                 count,
    //             })
    //         } else {
    //             None
    //         }
    //     }))
    //     .all_ticks()
    //     .collect::<Vec<_>>();

    let acks =
        ops
            .clone()
            .entries()
            .map(q!(|(client_id, op)| match op {
                Op::Insert{ id, .. } => (client_id, OpResponse::Insert { id }),
                Op::Get { id, key } => (client_id, OpResponse::Get {
                    id,
                    tuple: join_result
                        .clone()
                        .filter_map(|((a, b, c, d), count)|
                             if a == key {
                                 Some(ZTuple {
                                     tuple: RawTuple::T { a, b, c, d },
                                     count,
                                 })
                            } else {
                                None
                            })
                        .collect::<Vec<_>>()
                    }),
            }))
            .end_atomic()
            .into_keyed();

    let errors = ops
        .filter_map(q!(|_| None::<String>))
        .entries()
        .end_atomic()
        .into_keyed();

    (acks, errors)
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc};

    use futures::{SinkExt, StreamExt};

    use super::*;

    // Helper to construct an Op::Insert with an auto-incrementing id
    fn make_insert(msg_id: &Rc<RefCell<u32>>, tuple: RawTuple, count: i32) -> Op {
        let id = {
            *msg_id.borrow_mut() += 1;
            *msg_id.borrow()
        };
        Op::Insert {
            id,
            tuple: ZTuple { tuple, count },
        }
    }

    #[tokio::test]
    async fn test_basic_join() {
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
            inc_join(input.atomic(&tick));
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

        let msg_id = Rc::new(RefCell::new(0u32));
        // Reusable closures (currently unused below, but available for future refactors)
        let ins_r = {
            let msg_id = Rc::clone(&msg_id);
            move |(a, b), count| make_insert(&msg_id, RawTuple::R { a, b }, count)
        };
        let ins_s = {
            let msg_id = Rc::clone(&msg_id);
            move |(a, c, d), count| make_insert(&msg_id, RawTuple::S { a, c, d }, count)
        };
        let get_k = {
            let msg_id = Rc::clone(&msg_id);
            move |k| Op::Get {
                id: {
                    *msg_id.borrow_mut() += 1;
                    *msg_id.borrow()
                },
                key: k,
            }
        };

        // Test increment operation
        external_in.send(ins_r((1, 2), 1)).await.unwrap();
        external_in.send(ins_s((1, 3, 4), 1)).await.unwrap();
        external_in.send(ins_s((2, 5, 6), 1)).await.unwrap();

        let responses: Vec<_> = external_out.by_ref().take(3).collect().await;
        dbg!(&responses);
        assert_eq!(responses.len(), 3);

        external_in.send(ins_s((1, 3, 5), 1)).await.unwrap();
        let responses: Vec<_> = external_out.by_ref().take(1).collect().await;
        dbg!(&responses);
        assert_eq!(responses.len(), 1);

        external_in.send(ins_r((1, 2), -1)).await.unwrap();
        external_in.send(ins_r((1, 7), 2)).await.unwrap();
        let responses: Vec<_> = external_out.by_ref().take(2).collect().await;
        dbg!(&responses);
        assert_eq!(responses.len(), 2);

    }

    #[tokio::test]
    async fn test_basic_join2() {
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
        let (r_stream, s_stream) = demux_rs(input.atomic(&tick));
        let s_stream =
            s_stream.entries();
        let s_stream = s_stream
            .map(q!(|((a, c, d), s_count)| (a, (c, d, s_count))))
            .inspect(q!(|(a, (c, d, s_count))| {
                println!("S stream: ({}, {}, {})#{}", a, c, d, s_count);
            }));
        let r_stream = r_stream
            .clone()
            .entries();
        let r_stream = r_stream.map(q!(|((a, b), r_count)| (a, (b, r_count))))
            .inspect(q!(|(a, (b, r_count))| {
                println!("R stream: ({}, {})#{}", a, b, r_count);
            }));
            // .join(s_stream)
            // .inspect(q!(|(a, ((b, r_count), (c, d, s_count)))| {
            //     println!("output: ({}, {}, {}, {})#{}", a, b, c, d, r_count * s_count);
            // }));
        let responses = r_stream.join(s_stream).end_atomic()
            .map(q!(|x| (0u64, x)))
            .inspect(q!(|(id, (a, ((b, r_count), (c, d, s_count))))| {
                println!("response: id={:?} ({:?}, {:?}, {:?}, {:?})#{:?}", id, a, b, c, d, r_count * s_count);
            })).into_keyed();

        complete_sink.complete(responses);

        let nodes = flow
            .with_process(&process_node, deployment.Localhost())
            .with_external(&external, deployment.Localhost())
            .deploy(&mut deployment);

        deployment.deploy().await.unwrap();

        let (external_out, mut external_in) = nodes.connect_bincode(in_port).await;
        let mut external_out = Box::pin(external_out);

        deployment.start().await.unwrap();

        // Test increment operation
        external_in
            .send(ZTuple {
                tuple: RawTuple::R { a: 1, b: 2 },
                count: 1,
            })
            .await
            .unwrap();
        external_in
            .send(ZTuple {
                    tuple: RawTuple::S { a: 1, c: 3, d: 4 },
                    count: 1,
                })
            .await
            .unwrap();
        external_in
            .send(ZTuple {
                    tuple: RawTuple::S { a: 2, c: 5, d: 6 },
                    count: 1,
                })
            .await
            .unwrap();


        external_in
            .send(ZTuple {
                    tuple: RawTuple::S { a: 1, c: 3, d: 5 },
                    count: 1,
                })
            .await
            .unwrap();

        external_in
            .send(ZTuple {
                    tuple: RawTuple::R { a: 1, b: 2 },
                    count: -1,
                })
            .await
            .unwrap();
        external_in
            .send(ZTuple {
                    tuple: RawTuple::R { a: 1, b: 7 },
                    count: 2,
                })
            .await
            .unwrap();

        tokio::signal::ctrl_c().await.unwrap();
    }
}