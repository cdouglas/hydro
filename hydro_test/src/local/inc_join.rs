use std::fmt::Debug;
use std::hash::Hash;

use hydro_lang::location::tick::NoAtomic;
use hydro_lang::manual_expr::ManualExpr;
use hydro_lang::*;
use location::NoTick;
use serde::{Deserialize, Serialize};
use stageleft::{IntoQuotedMut, QuotedWithContext};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ZTuple<T>
where
    T: Debug + Clone + Eq + Hash,
{
    pub tuple: T,
    pub count: i32,
}

pub fn streaming_join<'a, R, S, T, K, KR, KS, M, L, O>(
    r_stream: Stream<ZTuple<R>, L, Unbounded, O>,
    s_stream: Stream<ZTuple<S>, L, Unbounded, O>,
    tick: Tick<L>,
    r_key: impl IntoQuotedMut<'a, KR, L> + Copy,
    s_key: impl IntoQuotedMut<'a, KS, L> + Copy,
    merge: impl IntoQuotedMut<'a, M, L> + Copy,
) -> (
    Stream<ZTuple<T>, L, Unbounded, NoOrder>,
    (), // Stream<String, L, Unbounded, NoOrder>, // err
)
where
    R: Debug + Clone + Eq + Hash,
    S: Debug + Clone + Eq + Hash,
    T: Debug + Clone + Eq + Hash,
    K: Debug + Clone + PartialEq + Eq + Hash,
    KR: Fn(&R) -> K + 'a,
    KS: Fn(&S) -> K + 'a,
    M: Fn(&R, &S) -> T + 'a,
    L: Location<'a> + NoTick + NoAtomic,
{
    let r_key_quot: ManualExpr<KR, _> =
        ManualExpr::new(move |ctx: &L| r_key.splice_fn1_borrow_ctx(ctx));
    let s_key_quot: ManualExpr<KS, _> =
        ManualExpr::new(move |ctx: &L| s_key.splice_fn1_borrow_ctx(ctx));
    let merge_quot: ManualExpr<M, _> =
        ManualExpr::new(move |ctx: &L| merge.splice_fn2_borrow_ctx(ctx));

    let dr_kstream = r_stream.map(q!(move |ztuple| (
        r_key_quot(&ztuple.tuple),
        ZTuple {
            tuple: ztuple.tuple,
            count: ztuple.count
        }
    )));
    let ds_kstream = s_stream.map(q!(move |ztuple| (
        s_key_quot(&ztuple.tuple),
        ZTuple {
            tuple: ztuple.tuple,
            count: ztuple.count
        }
    )));

    let join_result = dr_kstream
        .join(ds_kstream)
        .map(q!(move |(_key, (ztuple_r, ztuple_s))| {
            let merged = merge_quot(&ztuple_r.tuple, &ztuple_s.tuple);
            let count = ztuple_r.count * ztuple_s.count;
            (merged, count)
        }))
        .batch(&tick, nondet!(/** necessary for KeyedSingleton */))
        .into_keyed()
        .fold_commutative(q!(|| 0i32), q!(|acc, count| *acc += count))
        .filter(q!(|count| *count != 0))
        .entries()
        .map(q!(|(tuple, count)| { ZTuple { tuple, count } }))
        .all_ticks();
    (join_result, ())
}

pub fn dbsp_batch_join<'a, R, S, T, K, KR, KS, M, L, O>(
    r_stream: Stream<ZTuple<R>, L, Unbounded, O>,
    s_stream: Stream<ZTuple<S>, L, Unbounded, O>,
    tick: Tick<L>,
    r_key: impl IntoQuotedMut<'a, KR, Tick<L>> + Copy,
    s_key: impl IntoQuotedMut<'a, KS, Tick<L>> + Copy,
    merge: impl IntoQuotedMut<'a, M, Tick<L>> + Copy,
) -> (
    Stream<ZTuple<T>, L, Unbounded, NoOrder>,
    (), // Stream<String, L, Unbounded, NoOrder>, // err
)
where
    R: Debug + Clone + Eq + Hash,
    S: Debug + Clone + Eq + Hash,
    T: Debug + Clone + Eq + Hash,
    K: Debug + Clone + PartialEq + Eq + Hash,
    KR: Fn(&R) -> K + 'a,
    KS: Fn(&S) -> K + 'a,
    M: Fn(&R, &S) -> T + 'a,
    L: Location<'a> + NoTick + NoAtomic,
{
    let r_key_quot: ManualExpr<KR, _> =
        ManualExpr::new(move |ctx: &Tick<L>| r_key.splice_fn1_borrow_ctx(ctx));
    let s_key_quot: ManualExpr<KS, _> =
        ManualExpr::new(move |ctx: &Tick<L>| s_key.splice_fn1_borrow_ctx(ctx));
    let merge_quot: ManualExpr<M, _> =
        ManualExpr::new(move |ctx: &Tick<L>| merge.splice_fn2_borrow_ctx(ctx));

    let r_stream = r_stream.atomic(&tick);
    let s_stream = s_stream.atomic(&tick);

    let r = r_stream
        .clone()
        .map(q!(|ztuple| (ztuple.tuple, ztuple.count)))
        .into_keyed()
        .fold_commutative(q!(|| 0i32), q!(|acc, count| *acc += count))
        .filter(q!(|count| *count != 0))
        .snapshot(nondet!(/** rollup R state */))
        .entries()
        .map(q!(|(tuple, count)| ZTuple { tuple, count }))
        .defer_tick();
    let s = s_stream
        .clone()
        .map(q!(|ztuple| (ztuple.tuple, ztuple.count)))
        .into_keyed()
        .fold_commutative(q!(|| 0i32), q!(|acc, count| *acc += count))
        .filter(q!(|count| *count != 0))
        .snapshot(nondet!(/** rollup S state */))
        .entries()
        .map(q!(|(tuple, count)| ZTuple { tuple, count }))
        .defer_tick();

    let delta_r = r_stream
        .clone()
        .map(q!(|ztuple| (ztuple.tuple, ztuple.count)))
        .batch(nondet!(/** R tuples this tick */));
    let delta_s = s_stream
        .clone()
        .map(q!(|ztuple| (ztuple.tuple, ztuple.count)))
        .batch(nondet!(/** S tuples this tick */));

    let r_kstream = r
        .clone()
        .map(q!(move |ztuple| (r_key_quot(&ztuple.tuple), ztuple)));
    let s_kstream = s
        .clone()
        .map(q!(move |ztuple| (s_key_quot(&ztuple.tuple), ztuple)));
    let dr_kstream = delta_r.map(q!(move |(tuple, count)| (
        r_key_quot(&tuple),
        ZTuple { tuple, count }
    )));
    let ds_kstream = delta_s.map(q!(move |(tuple, count)| (
        s_key_quot(&tuple),
        ZTuple { tuple, count }
    )));

    // ΔR x ΔS
    let dr_x_ds = dr_kstream.clone().join(ds_kstream.clone());
    //  R x ΔS
    let r_x_ds = r_kstream.join(ds_kstream);
    // ΔR x  S
    let dr_x_s = dr_kstream.join(s_kstream);

    let join_result = dr_x_ds
        .chain(r_x_ds)
        .chain(dr_x_s)
        .map(q!(move |(_key, (ztuple_r, ztuple_s))| {
            let merged = merge_quot(&ztuple_r.tuple, &ztuple_s.tuple);
            let count = ztuple_r.count * ztuple_s.count;
            (merged, count)
        }))
        .into_keyed()
        .fold_commutative(q!(|| 0i32), q!(|acc, count| *acc += count))
        .filter(q!(|count| *count != 0))
        .entries()
        .map(q!(|(tuple, count)| { ZTuple { tuple, count } }))
        .all_ticks();
    (join_result, ())
}

#[cfg(test)]
mod tests {
    use futures::{SinkExt, StreamExt};

    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
    struct RawTupleR {
        a: u32,
        b: u32,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
    struct RawTupleS {
        a: u32,
        c: u32,
        d: u32,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
    struct RawTupleT {
        a: u32,
        b: u32,
        c: u32,
        d: u32,
    }

    async fn test_join_basic<F>(join_fn: F)
    where
        F: for<'a> Fn(
            Stream<ZTuple<RawTupleR>, Process<'a>, Unbounded>,
            Stream<ZTuple<RawTupleS>, Process<'a>, Unbounded>,
            Tick<Process<'a>>,
        ) -> (
            Stream<ZTuple<RawTupleT>, Process<'a>, Unbounded, NoOrder>,
            (),
        ),
    {
        use hydro_deploy::Deployment;
        use hydro_lang::FlowBuilder;

        let mut deployment = Deployment::new();

        let flow = FlowBuilder::new();
        let process_node = flow.process::<()>();
        let external = flow.external::<()>();

        let (r_send, r_stream) = process_node.source_external_bincode(&external);
        let (s_send, s_stream) = process_node.source_external_bincode(&external);

        let tick = process_node.tick();

        // JOIN invocation
        let (responses, _errors) = join_fn(r_stream, s_stream, tick);

        let t_recv = responses.send_bincode_external(&external);

        let nodes = flow
            .with_process(&process_node, deployment.Localhost())
            .with_external(&external, deployment.Localhost())
            .deploy(&mut deployment);

        deployment.deploy().await.unwrap();

        let mut r_external_in = nodes.connect_sink_bincode(r_send).await;
        let mut s_external_in = nodes.connect_sink_bincode(s_send).await;
        let mut t_external_out = nodes.connect_source_bincode(t_recv).await;

        deployment.start().await.unwrap();

        //  R:           |  S:
        // ΔR: (1, 10)#1 | ΔS: (1, 20, 30)#1
        // ΔT: (1, 10, 20, 30)#1
        r_external_in
            .send(ZTuple {
                tuple: RawTupleR { a: 1, b: 10 },
                count: 1,
            })
            .await
            .unwrap();
        s_external_in
            .send(ZTuple {
                tuple: RawTupleS { a: 1, c: 20, d: 30 },
                count: 1,
            })
            .await
            .unwrap();
        let recv = t_external_out.by_ref().take(1).collect::<Vec<_>>().await;
        assert_eq!(
            recv[0],
            ZTuple {
                tuple: RawTupleT {
                    a: 1,
                    b: 10,
                    c: 20,
                    d: 30
                },
                count: 1
            }
        );

        //  R: (1, 10)#1 |  S: (1, 20, 30)#1
        // ΔR: (1, 10)#1 | ΔS:
        // ΔT: (1, 10, 20, 30)#1
        r_external_in
            .send(ZTuple {
                tuple: RawTupleR { a: 1, b: 10 },
                count: 1,
            })
            .await
            .unwrap();
        let recv = t_external_out.by_ref().take(1).collect::<Vec<_>>().await;
        assert_eq!(
            recv[0],
            ZTuple {
                tuple: RawTupleT {
                    a: 1,
                    b: 10,
                    c: 20,
                    d: 30
                },
                count: 1
            }
        );

        //  R: (1, 10)#2 |  S: (1, 20, 30)#1
        // ΔR:           | ΔS: (1, 20, 30)#-1
        // ΔT: (1, 10, 20, 30)#-2
        s_external_in
            .send(ZTuple {
                tuple: RawTupleS { a: 1, c: 20, d: 30 },
                count: -1,
            })
            .await
            .unwrap();
        let recv = t_external_out.by_ref().take(1).collect::<Vec<_>>().await;
        assert_eq!(
            recv[0],
            ZTuple {
                tuple: RawTupleT {
                    a: 1,
                    b: 10,
                    c: 20,
                    d: 30
                },
                count: -2
            }
        );

        //  R: (1, 10)#2 |  S:
        // ΔR: (1, 10)#1 | ΔS: (1, 40, 50)#2, (2, 20, 30)#1
        // ΔT: (1, 10, 40, 50)#6
        r_external_in
            .send(ZTuple {
                tuple: RawTupleR { a: 1, b: 10 },
                count: 1,
            })
            .await
            .unwrap();
        s_external_in
            .send(ZTuple {
                tuple: RawTupleS { a: 2, c: 20, d: 30 },
                count: 1,
            })
            .await
            .unwrap();
        s_external_in
            .send(ZTuple {
                tuple: RawTupleS { a: 1, c: 40, d: 50 },
                count: 2,
            })
            .await
            .unwrap();
        let recv = t_external_out.by_ref().take(1).collect::<Vec<_>>().await;
        assert_eq!(
            recv[0],
            ZTuple {
                tuple: RawTupleT {
                    a: 1,
                    b: 10,
                    c: 40,
                    d: 50
                },
                count: 6
            }
        );
    }

    #[tokio::test]
    async fn test_streaming_join_basic() {
        fn streaming_join_wrapper<'a>(
            r_stream: Stream<ZTuple<RawTupleR>, Process<'a>, Unbounded>,
            s_stream: Stream<ZTuple<RawTupleS>, Process<'a>, Unbounded>,
            tick: Tick<Process<'a>>,
        ) -> (
            Stream<ZTuple<RawTupleT>, Process<'a>, Unbounded, NoOrder>,
            (),
        ) {
            streaming_join(
                r_stream,
                s_stream,
                tick,
                q!(|r: &RawTupleR| r.a),
                q!(|s: &RawTupleS| s.a),
                q!(|r: &RawTupleR, s: &RawTupleS| {
                    if r.a != s.a {
                        panic!("{:?} != {:?}", r.a, s.a)
                    }
                    RawTupleT {
                        a: r.a,
                        b: r.b,
                        c: s.c,
                        d: s.d,
                    }
                }),
            )
        }
        test_join_basic(streaming_join_wrapper).await;
    }

    #[tokio::test]
    async fn test_dbsp_batch_join_basic() {
        fn dbsp_batch_join_wrapper<'a>(
            r_stream: Stream<ZTuple<RawTupleR>, Process<'a>, Unbounded>,
            s_stream: Stream<ZTuple<RawTupleS>, Process<'a>, Unbounded>,
            tick: Tick<Process<'a>>,
        ) -> (
            Stream<ZTuple<RawTupleT>, Process<'a>, Unbounded, NoOrder>,
            (),
        ) {
            dbsp_batch_join(
                r_stream,
                s_stream,
                tick,
                q!(|r: &RawTupleR| r.a),
                q!(|s: &RawTupleS| s.a),
                q!(|r: &RawTupleR, s: &RawTupleS| {
                    if r.a != s.a {
                        panic!("{:?} != {:?}", r.a, s.a)
                    }
                    RawTupleT {
                        a: r.a,
                        b: r.b,
                        c: s.c,
                        d: s.d,
                    }
                }),
            )
        }
        test_join_basic(dbsp_batch_join_wrapper).await;
    }
}
