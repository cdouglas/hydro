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
pub struct ZTuple {
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
    Insert {
        id: u32,
    },
    Get {
        id: u32,
        tuples: Option<Vec<ZTuple>>,
    },
}

pub fn inc_join<'a, L: Location<'a> + NoTick + NoAtomic, Order>(
    ops: KeyedStream<u64, Op, Atomic<L>, Unbounded, Order>,
) -> (
    KeyedStream<u64, OpResponse, L, Unbounded, NoOrder>,
    KeyedStream<u64, String, L, Unbounded, NoOrder>,
) {
    // KeyedStream is Atomic; snapshot and batch are aligned
    let (r_stream, s_stream) = {
        let stream = ops.clone().filter_map(q!(|op| match op {
            Op::Insert { tuple, .. } => Some(tuple),
            Op::Get { .. } => None,
        }));
        let r_stream = stream
            .clone()
            .values()
            .filter_map(q!(|ztuple| match ztuple.tuple {
                RawTuple::R { a, b } => Some(((a, b), ztuple.count)),
                _ => None,
            }))
            .into_keyed();

        let s_stream = stream
            .clone()
            .values()
            .filter_map(q!(|ztuple| match ztuple.tuple {
                RawTuple::S { a, c, d } => Some(((a, c, d), ztuple.count)),
                _ => None,
            }))
            .into_keyed();

        (r_stream, s_stream)
    };
    let ops_batch = ops.batch(nondet!(/** group of commands */));

    // R relation
    let r = r_stream
        .clone()
        .fold_commutative(q!(|| 0i32), q!(|acc, count| *acc += count))
        .snapshot(nondet!(/** rollup R state */))
        .entries()
        .defer_tick()
        .filter(q!(|(_, count)| *count != 0));

    // S relation
    let s = s_stream
        .clone()
        .fold_commutative(q!(|| 0i32), q!(|acc, count| *acc += count))
        .snapshot(nondet!(/** rollup S state */))
        .entries()
        .defer_tick()
        .filter(q!(|(_, count)| *count != 0));

    // ΔR in this tick
    let delta_r = r_stream.clone().batch(nondet!(/** group of commands */));
    // ΔS in this tick
    let delta_s = s_stream.clone().batch(nondet!(/** group of commands */));

    // ΔR x ΔS (new R tuples × new S tuples in same tick)
    let delta_r_x_delta_s = delta_r
        .clone()
        .entries()
        .map(q!(|((a, b), r_count)| (a, (b, r_count))))
        .join(
            delta_s
                .clone()
                .entries()
                .map(q!(|((a, c, d), s_count)| (a, (c, d, s_count)))),
        )
        .map(q!(|(a, ((b, r_count), (c, d, s_count)))| (
            (a, b, c, d),
            r_count * s_count
        )));

    // R × ΔS (existing R tuples × new S tuples)
    let r_x_delta_s = r
        .clone()
        .map(q!(|((a, b), r_count)| (a, (b, r_count))))
        .join(
            delta_s
                .entries()
                .map(q!(|((a, c, d), s_count)| (a, (c, d, s_count)))),
        )
        .map(q!(|(a, ((b, r_count), (c, d, s_count)))| (
            (a, b, c, d),
            r_count * s_count
        )));

    // S × ΔR (existing S tuples x new R tuples)
    let s_x_delta_r = s
        .clone()
        .map(q!(|((a, c, d), s_count)| (a, (c, d, s_count))))
        .join(
            delta_r
                .entries()
                .map(q!(|((a, b), r_count)| (a, (b, r_count)))),
        )
        .map(q!(|(a, ((c, d, s_count), (b, r_count)))| (
            (a, b, c, d),
            r_count * s_count
        )));

    let join_result = s_x_delta_r
        .chain(r_x_delta_s)
        .chain(delta_r_x_delta_s)
        .all_ticks_atomic()
        .into_keyed()
        .fold_commutative(q!(|| 0i32), q!(|acc, count| *acc += count))
        .filter(q!(|count| *count != 0))
        .snapshot(nondet!(/** rollup join result */));

    join_result
        .clone()
        .entries()
        .map(q!(|((a, b, c, d), count)| (a, b, c, d, count)))
        .all_ticks()
        .for_each(q!(|(a, b, c, d, count)| {
            println!("output delta: ({}, {}, {}, {})#{}", a, b, c, d, count);
        }));

    // TODO had to break this into multiple stmt; can't embed e.g.,
    // let result = some_stream.clone()
    //   ...
    //   .filter_map(q!(|(...)| match op {
    //     Op::Get { id, key } => Some((id, StructType {
    //       id,
    //       field: another_stream.clone()
    //              .map(q!(|...| ...)) // XXX not allowed?
    //              .etc()
    //     })),
    //     // ...
    //   }))

    // response: insert ACKs
    let insert_resp = ops_batch
        .clone()
        .entries()
        .filter_map(q!(|(client_id, op)| match op {
            Op::Insert { id, .. } => Some((client_id, OpResponse::Insert { id })),
            _ => None,
        }));

    // (key, (client_id, id)) from ops
    let get_reqs = ops_batch
        .clone()
        .entries()
        .filter_map(q!(|(client_id, op)| match op {
            Op::Get { id, key } => Some((key, (client_id, id))),
            _ => None,
        }));

    // (key, ztuple) from join_result
    let result_keyed = join_result
        .clone()
        .entries()
        .map(q!(|((a, b, c, d), count)| (
            a,
            ZTuple {
                tuple: RawTuple::T { a, b, c, d },
                count,
            }
        )));

    // (_key, ((client_id, id), ztuple)) -> (client_id, OpResponse::Get { .. })
    let get_resp = get_reqs
        .clone()
        .join(result_keyed.clone())
        .map(q!(|(_key, ((client_id, id), ztuple))| (
            (client_id, id),
            ztuple.clone()
        )))
        .into_keyed()
        .fold_commutative(q!(|| Vec::<ZTuple>::new()), q!(|acc, zt| acc.push(zt)))
        .entries()
        .map(q!(|((client_id, id), vtup)| ((client_id, id), Some(vtup))));

    let missing_resp = get_reqs
        .clone()
        .map(q!(|(_key, (client_id, id))| ((client_id, id), None)))
        .chain(get_resp.clone())
        .into_keyed()
        .fold_commutative(
            q!(|| None::<Vec<ZTuple>>),
            q!(|acc, zt| {
                if let Some(v) = zt {
                    if acc.replace(v).is_some() {
                        panic!("expected at most one value from get_reqs");
                    }
                }
            }),
        )
        .entries()
        .map(q!(|((client_id, id), vtup)| (
            client_id,
            OpResponse::Get { id, tuples: vtup }
        )));

    let errors = ops_batch
        .filter_map(q!(|_| None::<String>))
        .entries()
        .all_ticks()
        .into_keyed();

    let responses = insert_resp.chain(missing_resp).all_ticks().into_keyed();

    (responses, errors)
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::rc::Rc;

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

    fn chk_get(qid: u32, responses: Vec<OpResponse>, expected: Option<Vec<ZTuple>>) -> bool {
        responses
            .iter()
            .find(|resp| match resp {
                OpResponse::Insert { id: msg_id } => *msg_id == qid,
                OpResponse::Get { id: msg_id, tuples } => {
                    if *msg_id == qid {
                        fn bag(v: &[ZTuple]) -> HashMap<ZTuple, i32> {
                            v.iter().fold(HashMap::new(), |mut acc, zt| {
                                *acc.entry(zt.clone()).or_insert(0) += zt.count;
                                acc
                            })
                        }
                        if let Some(expected) = &expected {
                            if let Some(tuples) = tuples {
                                assert_eq!(bag(&tuples), bag(&expected));
                            } else {
                                panic!("Expected tuples, but got None");
                            }
                        } else {
                            assert!(tuples.is_none(), "Expected no tuples, but got Some(..)");
                        }
                        true
                    } else {
                        false
                    }
                }
            })
            .is_some()
    }

    #[tokio::test]
    async fn test_inc_join_basic() {
        use hydro_deploy::Deployment;
        use hydro_lang::FlowBuilder;

        let mut deployment = Deployment::new();

        let flow = FlowBuilder::new();
        let process_node = flow.process::<()>();
        let external = flow.external::<()>();

        let (in_port, input, _membership, complete_sink) =
            process_node.bidi_external_many_bincode(&external);

        let tick = process_node.tick();
        let (responses, _errors) = inc_join(input.atomic(&tick));

        complete_sink.complete(responses);

        let nodes = flow
            .with_process(&process_node, deployment.Localhost())
            .with_external(&external, deployment.Localhost())
            .deploy(&mut deployment);

        deployment.deploy().await.unwrap();

        let (mut external_out, mut external_in) = nodes.connect_bincode(in_port).await;

        deployment.start().await.unwrap();

        let msg_id = Rc::new(RefCell::new(0u32));
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

        //  R: (1, 2)
        //  S: (1, 3, 4), (2, 5, 6)
        // ΔT: (1, 2, 3, 4)#1
        //  T: (1, 2, 3, 4)#1
        external_in.send(ins_r((1, 2), 1)).await.unwrap();
        external_in.send(ins_s((1, 3, 4), 1)).await.unwrap();
        external_in.send(ins_s((2, 5, 6), 1)).await.unwrap();
        external_in.send(get_k(1)).await.unwrap(); // id 4

        let responses: Vec<_> = external_out.by_ref().take(4).collect().await;
        dbg!(&responses);
        assert_eq!(responses.len(), 4);
        assert!(chk_get(
            4,
            responses,
            Some(vec![ZTuple {
                tuple: RawTuple::T {
                    a: 1,
                    b: 2,
                    c: 3,
                    d: 4
                },
                count: 1
            }])
        ));

        //  R: (1, 2)#1
        // ΔS: (1, 3, 5)#1
        //  S: (1, 3, 4)#1, (1, 3, 5)#1, (2, 5, 6)#1
        // ΔT: (1, 2, 3, 5)#1
        //  T: (1, 2, 3, 4)#1, (1, 2, 3, 5)#1
        external_in.send(ins_s((1, 3, 5), 1)).await.unwrap();
        external_in.send(get_k(1)).await.unwrap(); // id 6
        let responses: Vec<_> = external_out.by_ref().take(2).collect().await;
        dbg!(&responses);
        assert_eq!(responses.len(), 2);
        assert!(chk_get(
            6,
            responses,
            Some(vec![
                ZTuple {
                    tuple: RawTuple::T {
                        a: 1,
                        b: 2,
                        c: 3,
                        d: 4
                    },
                    count: 1
                },
                ZTuple {
                    tuple: RawTuple::T {
                        a: 1,
                        b: 2,
                        c: 3,
                        d: 5
                    },
                    count: 1
                }
            ])
        ));

        // ΔR: (1, 2)#-1, (1, 7)#2
        //  R: (1, 7)#2
        //  S: (1, 3, 4)#1, (1, 3, 5)#1, (2, 5, 6)#1
        // ΔT: (1, 2, 3, 4)#-1, (1, 2, 3, 5)#-1
        //     (1, 7, 3, 4)#2, (1, 7, 3, 5)#2
        //  T: (1, 7, 3, 4)#2, (1, 7, 3, 5)#2
        external_in.send(ins_r((1, 2), -1)).await.unwrap();
        external_in.send(ins_r((1, 7), 2)).await.unwrap();
        external_in.send(get_k(1)).await.unwrap(); // id 9
        let responses: Vec<_> = external_out.by_ref().take(3).collect().await;
        dbg!(&responses);
        assert_eq!(responses.len(), 3);
        assert!(chk_get(
            9,
            responses,
            Some(vec![
                ZTuple {
                    tuple: RawTuple::T {
                        a: 1,
                        b: 7,
                        c: 3,
                        d: 4
                    },
                    count: 2
                },
                ZTuple {
                    tuple: RawTuple::T {
                        a: 1,
                        b: 7,
                        c: 3,
                        d: 5
                    },
                    count: 2
                }
            ])
        ));

        // ΔR: (1, 7)#-1,
        //  R: (1, 7)#1
        //  S: (1, 3, 4)#1, (1, 3, 5)#1, (2, 5, 6)#1
        // ΔT: (1, 7, 3, 4)#-1, (1, 7, 3, 5)#-1
        //  T: (1, 7, 3, 4)#1, (1, 7, 3, 5)#1
        external_in.send(ins_r((1, 7), -1)).await.unwrap();
        external_in.send(get_k(1)).await.unwrap(); // id 11
        let responses: Vec<_> = external_out.by_ref().take(2).collect().await;
        dbg!(&responses);
        assert_eq!(responses.len(), 2);
        assert!(chk_get(
            11,
            responses,
            Some(vec![
                ZTuple {
                    tuple: RawTuple::T {
                        a: 1,
                        b: 7,
                        c: 3,
                        d: 4
                    },
                    count: 1
                },
                ZTuple {
                    tuple: RawTuple::T {
                        a: 1,
                        b: 7,
                        c: 3,
                        d: 5
                    },
                    count: 1
                }
            ])
        ));

        external_in.send(get_k(1)).await.unwrap(); // id 12
        let responses: Vec<_> = external_out.by_ref().take(1).collect().await;
        dbg!(&responses);
        assert_eq!(responses.len(), 1);
        assert!(chk_get(
            12,
            responses,
            Some(vec![
                ZTuple {
                    tuple: RawTuple::T {
                        a: 1,
                        b: 7,
                        c: 3,
                        d: 4
                    },
                    count: 1
                },
                ZTuple {
                    tuple: RawTuple::T {
                        a: 1,
                        b: 7,
                        c: 3,
                        d: 5
                    },
                    count: 1
                }
            ])
        ));

        // key not in output
        external_in.send(get_k(2)).await.unwrap(); // id 13
        let responses: Vec<_> = external_out.by_ref().take(1).collect().await;
        dbg!(&responses);
        assert_eq!(responses.len(), 1);
        assert!(chk_get(13, responses, None));
    }

    #[tokio::test]
    async fn test_inc_join_multipath() {
        use hydro_deploy::Deployment;
        use hydro_lang::FlowBuilder;

        let mut deployment = Deployment::new();

        let flow = FlowBuilder::new();
        let process_node = flow.process::<()>();
        let external = flow.external::<()>();

        let (in_port, input, _membership, complete_sink) =
            process_node.bidi_external_many_bincode(&external);

        let tick = process_node.tick();
        let (responses, _errors) = inc_join(input.atomic(&tick));

        complete_sink.complete(responses);

        let nodes = flow
            .with_process(&process_node, deployment.Localhost())
            .with_external(&external, deployment.Localhost())
            .deploy(&mut deployment);

        deployment.deploy().await.unwrap();

        let (mut external_out, mut external_in) = nodes.connect_bincode(in_port).await;

        deployment.start().await.unwrap();

        let msg_id = Rc::new(RefCell::new(0u32));
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

        // ΔR: (1, 2)
        // ΔS: (1, 3, 4), (2, 5, 6)
        // ΔT: (1, 2, 3, 4)#1
        //  T: (1, 2, 3, 4)#1
        external_in.send(ins_r((1, 2), 1)).await.unwrap();
        external_in.send(ins_s((1, 3, 4), 1)).await.unwrap();
        external_in.send(ins_s((2, 5, 6), 1)).await.unwrap();
        external_in.send(get_k(1)).await.unwrap(); // id 4

        let responses: Vec<_> = external_out.by_ref().take(4).collect().await;
        dbg!(&responses);
        assert_eq!(responses.len(), 4);
        assert!(chk_get(
            4,
            responses,
            Some(vec![ZTuple {
                tuple: RawTuple::T {
                    a: 1,
                    b: 2,
                    c: 3,
                    d: 4
                },
                count: 1
            }])
        ));

        // ΔR: (1, 2)#-1, (1, 7)#1, (2, 5)#1
        //  R: (1, 2)#1
        // ΔS: (1, 3, 5)#1
        //  S: (1, 3, 4)#1, (2, 5, 6)#1
        // ΔT: (1, 2, 3, 4)#-1, (1, 7, 3, 4)#1, (1, 7, 3, 5)#1, (2, 5, 5, 6)#1
        //  T: (1, 7, 3, 4)#1, (1, 7, 3, 5)#1, (2, 5, 5, 6)#1
        external_in.send(ins_s((1, 3, 5), 1)).await.unwrap(); // ΔS x R
        external_in.send(ins_r((2, 5), 1)).await.unwrap(); // ΔR x S
        external_in.send(ins_r((1, 2), -1)).await.unwrap(); // ΔR x S
        external_in.send(ins_r((1, 7), 1)).await.unwrap(); // ΔR x S
        external_in.send(get_k(1)).await.unwrap(); // id 9
        external_in.send(get_k(2)).await.unwrap(); // id 10
        let responses: Vec<_> = external_out.by_ref().take(6).collect().await;
        dbg!(&responses);
        assert_eq!(responses.len(), 6);
        assert!(chk_get(
            9,
            responses.clone(),
            Some(vec![
                ZTuple {
                    tuple: RawTuple::T {
                        a: 1,
                        b: 7,
                        c: 3,
                        d: 4
                    },
                    count: 1
                },
                ZTuple {
                    tuple: RawTuple::T {
                        a: 1,
                        b: 7,
                        c: 3,
                        d: 5
                    },
                    count: 1
                }
            ])
        ));
        assert!(chk_get(
            10,
            responses,
            Some(vec![ZTuple {
                tuple: RawTuple::T {
                    a: 2,
                    b: 5,
                    c: 5,
                    d: 6
                },
                count: 1
            }])
        ));
    }

    #[tokio::test]
    async fn test_dbsp_batch_join() {
        /// standard sequential "semi-naive" -- i.e. bilinear -- join logic
        /// 1. snapshot r_delta, s_delta
        /// 2. union the joins of r_delta x s_old, r_old x s_delta, r_delta x s_delta and pass to output stream
        /// 3. mutate r_old = r_old merge r_delta, s_old = s_old merge s_delta
        /// We achieve having step 3 follow step 2 via defer_tick
        use hydro_deploy::Deployment;
        use hydro_lang::FlowBuilder;

        let mut deployment = Deployment::new();

        let flow = FlowBuilder::new();
        let process_node = flow.process::<()>();
        let external_r = flow.external::<()>();
        let external_s = flow.external::<()>();

        let (r_port, r_stream_in, _r_membership, r_complete_sink) =
            process_node.bidi_external_many_bincode::<(), ZTuple, ()>(&external_r);
        let (s_port, s_stream_in, _s_membership, s_complete_sink) =
            process_node.bidi_external_many_bincode::<(), ZTuple, ()>(&external_s);
        let tick = process_node.tick();

        let nodes = flow
            .with_process(&process_node, deployment.Localhost())
            .with_external(&external_r, deployment.Localhost())
            .with_external(&external_s, deployment.Localhost())
            .deploy(&mut deployment);

        deployment.deploy().await.unwrap();

        let (mut _external_out, mut r_external_in) = nodes.connect_bincode(r_port).await;
        let (mut _external_out, mut s_external_in) = nodes.connect_bincode(s_port).await;

        deployment.start().await.unwrap();

        let rtf = r_stream_in.clone().map(q!(|_| ()));
        let stf = s_stream_in.clone().map(q!(|_| ()));
        r_complete_sink.complete(rtf);
        s_complete_sink.complete(stf);

        // THE FOLLOWING FLOW SHOULD BE A DROP-IN REPLACEMENT for `join` over ZTuples
        // The "API" should just be streams of (k, v) on both sides (which it's not yet)
        // where v is a ZTuple
        let r_stream = r_stream_in
            .values()
            .filter_map(q!(|ztuple: ZTuple| match ztuple.tuple {
                RawTuple::R { a, b } => Some((a, (b, ztuple.count))),
                _ => None,
            }));
        let s_stream = s_stream_in
            .values()
            .filter_map(q!(|ztuple: ZTuple| match ztuple.tuple {
                RawTuple::S { a, c, d } => Some((a, (c, d, ztuple.count))),
                _ => None,
            }));


        // inductively put the deltas in old at *end of tick*
        // r_old_next = Z-set-merge(r_old, r_delta)
        // r_old = r_old_next.defer_tick();
        let r_delta = r_stream
            .atomic(&tick)
            .batch(nondet!(/** form a batch for incremental processing **/));
        let s_delta = s_stream
            .atomic(&tick)
            .batch(nondet!(/** form a batch for incremental processing **/));
        let r_old = r_delta
            .clone()
            .map(q!(|(a, (b, cnt))| ((a, b), cnt)))
            .defer_tick()
            .into_keyed()
            .fold_commutative(q!(|| 0i32), q!(|acc, count| *acc += count))
            .entries() // ((a, b), cnt)
            .map(q!(|((a, b), cnt)| (a, b, cnt)))
            .filter(q!(|(_a, _b, cnt)| *cnt != 0));
        let s_old = s_delta
            .clone()
            .map(q!(|(a, (c, d, cnt))| ((a, c, d), cnt)))
            .defer_tick()
            .into_keyed()
            .fold_commutative(q!(|| 0i32), q!(|acc, count| *acc += count))
            .entries() // ((a, b), cnt)
            .map(q!(|((a, c, d), cnt)| (a, c, d, cnt)))
            .filter(q!(|(_a, _c, _d, cnt)| *cnt != 0));

        // join deltas against the current versions of old
        let r_d_x_s = r_delta.clone().join(s_old.map(q!(|(a, c, d, cnt)| (a, (c, d, cnt)))));
        let r_x_s_d = r_old.map(q!(|(a, b, cnt)| (a, (b, cnt)))).join(s_delta.clone());
        // and join the deltas
        let r_d_x_s_d = r_delta.join(s_delta);

        let disagg_outputs = r_d_x_s_d.chain(r_d_x_s).chain(r_x_s_d);

        // now sum up the zset multiplicities!
        let output_flat = disagg_outputs
            .map(q!(|(a, ((b, r_count), (c, d, s_count)))| (
                (a, b, c, d),
                r_count * s_count
            )))
            .into_keyed()
            .fold_commutative(q!(|| 0i32), q!(|acc, t| *acc += t))
            .entries();
        let output = output_flat.clone().map(q!(|((a, b, c, d), cnt)| ZTuple {
            tuple: RawTuple::T { a, b, c, d },
            count: cnt,
        }));
        // END OF THE JOIN IMPLEMENTATION

        r_external_in
            .send(ZTuple {
                tuple: (RawTuple::R { a: 2, b: 4 }),
                count: 3,
            })
            .await
            .unwrap();
        s_external_in
            .send(ZTuple {
                tuple: RawTuple::S { a: 2, c: 1, d: 2 },
                count: 1,
            })
            .await
            .unwrap();
        s_external_in
            .send(ZTuple {
                tuple: RawTuple::S { a: 2, c: 5, d: 6 },
                count: 1,
            })
            .await
            .unwrap();

        output.inspect(q!(|t| println!("output tup: {:?}", t)));

        tokio::signal::ctrl_c().await.unwrap();
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
        let tick = process_node.tick();

        // (u64, Op) -> ((u64, u32), ZTuple)
        let insert_input = input
            .clone()
            .entries()
            .filter_map(q!(|(client_id, op)| match op {
                Op::Insert { id: msg_id, tuple } => Some(((client_id, msg_id), tuple)),
                _ => None,
            }));

        let r_stream = insert_input
            .clone()
            .filter_map(q!(|(_, ztuple)| match ztuple.tuple {
                RawTuple::R { a, b } => Some((a, (b, ztuple.count))),
                _ => None,
            }));
        let s_stream = insert_input
            .clone()
            .filter_map(q!(|(_, ztuple)| match ztuple.tuple {
                RawTuple::S { a, c, d } => Some((a, (c, d, ztuple.count))),
                _ => None,
            }));
        // join R, S -> ((a, b, c, d), count)
        let delta_r_x_s = r_stream
            .join(s_stream)
            .map(q!(|(a, ((b, r_count), (c, d, s_count)))| (
                (a, b, c, d),
                r_count * s_count
            )))
            .into_keyed()
            .fold_commutative(q!(|| 0i32), q!(|acc, count| *acc += count))
            .filter(q!(|count| *count != 0));

        let r_x_s = delta_r_x_s
            .atomic(&tick)
            .snapshot(nondet!(/** rollup join result */));
        let result_keyed = r_x_s.entries().map(q!(|((a, b, c, d), count)| (
            a,
            ZTuple {
                tuple: RawTuple::T { a, b, c, d },
                count,
            }
        )));

        // TODO: how to ACK after the insert applies to the join... but not make
        // it part of the join state?
        let insert_responses = insert_input
            .atomic(&tick)
            .batch(nondet!(/** batch insert requests */))
            .map(q!(|((client_id, msg_id), _)| (
                client_id,
                OpResponse::Insert { id: msg_id }
            )));

        let get_reqs = input
            .entries()
            .filter_map(q!(|(client_id, op)| match op {
                Op::Get { id: msg_id, key } => Some((key, (client_id, msg_id))),
                _ => None,
            }))
            .atomic(&tick)
            .batch(nondet!(/** batch get requests */));

        let get_resp = get_reqs
            .clone()
            .join(result_keyed)
            .map(q!(|(_key, ((client_id, id), ztuple))| (
                (client_id, id),
                ztuple
            )))
            .into_keyed()
            .fold_commutative(q!(|| Vec::<ZTuple>::new()), q!(|acc, zt| acc.push(zt)))
            .entries()
            .map(q!(|((client_id, id), vtup)| ((client_id, id), Some(vtup))));

        let missing_resp = get_reqs
            .clone()
            .map(q!(|(_key, (client_id, id))| ((client_id, id), None)))
            .chain(get_resp)
            .into_keyed()
            .fold_commutative(
                q!(|| None::<Vec<ZTuple>>),
                q!(|acc, zt| {
                    if let Some(v) = zt {
                        if acc.replace(v).is_some() {
                            panic!("expected at most one value from get_reqs");
                        }
                    }
                }),
            )
            .entries()
            .map(q!(|((client_id, id), vtups)| (
                client_id,
                OpResponse::Get { id, tuples: vtups }
            )));

        // Chain unkeyed streams then convert to keyed after collecting all ticks.
        let responses = insert_responses
            .chain(missing_resp)
            .all_ticks()
            .into_keyed();

        complete_sink.complete(responses);

        let nodes = flow
            .with_process(&process_node, deployment.Localhost())
            .with_external(&external, deployment.Localhost())
            .deploy(&mut deployment);

        deployment.deploy().await.unwrap();

        let (external_out, mut external_in) = nodes.connect_bincode(in_port).await;
        let mut external_out = Box::pin(external_out);

        deployment.start().await.unwrap();

        let msg_id = Rc::new(RefCell::new(0u32));
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

        // ΔR: (1, 2)
        // ΔS: (1, 3, 4), (2, 5, 6)
        // ΔT: (1, 2, 3, 4)#1
        //  T: (1, 2, 3, 4)#1
        external_in.send(ins_r((1, 2), 1)).await.unwrap();
        external_in.send(ins_s((1, 3, 4), 1)).await.unwrap();
        external_in.send(ins_s((2, 5, 6), 1)).await.unwrap();
        external_in.send(get_k(1)).await.unwrap(); // id 4

        let responses: Vec<_> = external_out.by_ref().take(4).collect().await;
        dbg!(&responses);
        assert_eq!(responses.len(), 4);
        assert!(chk_get(
            4,
            responses,
            Some(vec![ZTuple {
                tuple: RawTuple::T {
                    a: 1,
                    b: 2,
                    c: 3,
                    d: 4
                },
                count: 1
            }])
        ));

        // ΔR: (1, 2)#-1, (1, 7)#1, (2, 5)#1
        //  R: (1, 2)#1
        // ΔS: (1, 3, 5)#1
        //  S: (1, 3, 4)#1, (2, 5, 6)#1
        // ΔT: (1, 2, 3, 4)#-1, (1, 7, 3, 4)#1, (1, 7, 3, 5)#1, (2, 5, 5, 6)#1
        //  T: (1, 7, 3, 4)#1, (1, 7, 3, 5)#1, (2, 5, 5, 6)#1
        external_in.send(ins_s((1, 3, 5), 1)).await.unwrap(); // ΔS x R
        external_in.send(ins_r((2, 5), 1)).await.unwrap(); // ΔR x S
        external_in.send(ins_r((1, 2), -1)).await.unwrap(); // ΔR x S
        external_in.send(ins_r((1, 7), 1)).await.unwrap(); // ΔR x S
        external_in.send(get_k(1)).await.unwrap(); // id 9
        external_in.send(get_k(2)).await.unwrap(); // id 10
        let responses: Vec<_> = external_out.by_ref().take(6).collect().await;
        dbg!(&responses);
        assert_eq!(responses.len(), 6);
        assert!(chk_get(
            9,
            responses.clone(),
            Some(vec![
                ZTuple {
                    tuple: RawTuple::T {
                        a: 1,
                        b: 7,
                        c: 3,
                        d: 4
                    },
                    count: 1
                },
                ZTuple {
                    tuple: RawTuple::T {
                        a: 1,
                        b: 7,
                        c: 3,
                        d: 5
                    },
                    count: 1
                }
            ])
        ));
        assert!(chk_get(
            10,
            responses,
            Some(vec![ZTuple {
                tuple: RawTuple::T {
                    a: 2,
                    b: 5,
                    c: 5,
                    d: 6
                },
                count: 1
            }])
        ));
    }
}
