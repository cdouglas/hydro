use std::fmt::Debug;
use std::hash::Hash;

use hydro_lang::location::tick::NoAtomic;
use hydro_lang::*;
use location::NoTick;
use serde::{Deserialize, Serialize};
use stageleft::QuotedWithContext;
use stageleft::IntoQuotedMut;
use hydro_lang::manual_expr::ManualExpr;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ZTuple<T> where T: Debug + Clone + Eq + Hash {
    pub tuple: T,
    pub count: i32,
}

pub fn dbsp_batch_join<'a, R, S, T, K, KR, KS, M, L, O>(
    r_stream: Stream<ZTuple<R>, Atomic<L>, Unbounded, O>,
    s_stream: Stream<ZTuple<S>, Atomic<L>, Unbounded, O>,
    r_key: impl IntoQuotedMut<'a, KR, Tick<L>> + Copy,
    s_key: impl IntoQuotedMut<'a, KS, Tick<L>> + Copy,
    merge: impl IntoQuotedMut<'a, M, Tick<L>> + Copy
) -> (
    Stream<ZTuple<T>, L, Unbounded, NoOrder>,
    () // Stream<String, L, Unbounded, NoOrder>, // err
)
    where R: Debug + Clone + Eq + Hash,
          S: Debug + Clone + Eq + Hash,
          T: Debug + Clone + Eq + Hash,
          K: Debug + Clone + PartialEq + Eq + Hash,
          KR: Fn(&R) -> K + 'a,
          KS: Fn(&S) -> K + 'a,
          M: Fn(&R, &S) -> T + 'a,
          L: Location<'a> + NoTick + NoAtomic
{
    let r_key_quot: ManualExpr<KR, _> = ManualExpr::new(move |ctx: &Tick<L>| r_key.splice_fn1_borrow_ctx(ctx));
    let s_key_quot: ManualExpr<KS, _> = ManualExpr::new(move |ctx: &Tick<L>| s_key.splice_fn1_borrow_ctx(ctx));
    let merge_quot: ManualExpr<M, _> = ManualExpr::new(move |ctx: &Tick<L>| merge.splice_fn2_borrow_ctx(ctx));

    let r = r_stream.clone()
        .map(q!(|ztuple| (ztuple.tuple, ztuple.count)))
        .into_keyed()
        .fold_commutative(q!(|| 0i32), q!(|acc, count| *acc += count))
        .filter(q!(|count| *count != 0))
        .snapshot(nondet!(/** rollup R state */))
        .entries()
        .map(q!(|(tuple, count)| ZTuple { tuple, count }))
        .defer_tick();
    let s = s_stream.clone()
        .map(q!(|ztuple| (ztuple.tuple, ztuple.count)))
        .into_keyed()
        .fold_commutative(q!(|| 0i32), q!(|acc, count| *acc += count))
        .filter(q!(|count| *count != 0))
        .snapshot(nondet!(/** rollup S state */))
        .entries()
        .map(q!(|(tuple, count)| ZTuple { tuple, count }))
        .defer_tick();

    let delta_r = r_stream.clone()
        .map(q!(|ztuple| (ztuple.tuple, ztuple.count)))
        .batch(nondet!(/** R tuples this tick */));
    let delta_s = s_stream.clone()
        .map(q!(|ztuple| (ztuple.tuple, ztuple.count)))
        .batch(nondet!(/** S tuples this tick */));

    let r_kstream = r.clone()
        .map(q!(move |ztuple| (r_key_quot(&ztuple.tuple), ztuple)));
    let s_kstream = s.clone()
        .map(q!(move |ztuple| (s_key_quot(&ztuple.tuple), ztuple)));
    let dr_kstream = delta_r
        .map(q!(move |(tuple, count)| (r_key_quot(&tuple), ZTuple { tuple, count })));
    let ds_kstream = delta_s
        .map(q!(move |(tuple, count)| (s_key_quot(&tuple), ZTuple { tuple, count })));

    // ΔR x ΔS
    let dr_x_ds = dr_kstream.clone().join(ds_kstream.clone());
    //  R x ΔS
    let r_x_ds = r_kstream.join(ds_kstream);
    // ΔR x  S
    let dr_x_s = dr_kstream.join(s_kstream);

    let join_result = dr_x_ds.chain(r_x_ds).chain(dr_x_s)
        .map(q!(move |(_key, (ztuple_r, ztuple_s))| {
            let merged = merge_quot(&ztuple_r.tuple, &ztuple_s.tuple);
            let count = ztuple_r.count * ztuple_s.count;
            (merged, count)
        }))
        .into_keyed()
        .fold_commutative(q!(|| 0i32), q!(|acc, count| *acc += count))
        .filter(q!(|count| *count != 0))
        .entries()
        .map(q!(|(tuple, count)| {
            ZTuple { tuple, count }
        }))
        .all_ticks();
    (join_result, ())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{SinkExt, StreamExt};

    #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
    struct RawTupleR { a: u32, b: u32, }

    #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
    struct RawTupleS { a: u32, c: u32, d: u32, }

    #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
    struct RawTupleT { a: u32, b: u32, c: u32, d: u32, }

    #[tokio::test]
    async fn test_batch_join_basic() {
        use hydro_deploy::Deployment;
        use hydro_lang::FlowBuilder;

        let mut deployment = Deployment::new();

        let flow = FlowBuilder::new();
        let process_node = flow.process::<()>();
        let external = flow.external::<()>();

        let (r_send, r_stream) = process_node.source_external_bincode(&external);
        let (s_send, s_stream) = process_node.source_external_bincode(&external);

        let tick = process_node.tick();
        let (responses, _errors) = dbsp_batch_join(
            r_stream.atomic(&tick),
            s_stream.atomic(&tick),
            q!(|r: &RawTupleR| r.a),
            q!(|s: &RawTupleS| s.a),
            q!(|r: &RawTupleR, s: &RawTupleS| {
                if r.a != s.a {
                    panic!("{:?} != {:?}", r.a, s.a)
                }
                RawTupleT { a: r.a, b: r.b, c: s.c, d: s.d }
            })
        );

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
        r_external_in.send(ZTuple { tuple: RawTupleR { a: 1, b: 10 }, count: 1 }).await.unwrap();
        s_external_in.send(ZTuple { tuple: RawTupleS { a: 1, c: 20, d: 30 }, count: 1 }).await.unwrap();
        let recv = t_external_out.by_ref().take(1).collect::<Vec<_>>().await;
        assert_eq!(recv[0], ZTuple { tuple: RawTupleT { a: 1, b: 10, c: 20, d: 30 }, count: 1 });

        //  R: (1, 10)#1 |  S: (1, 20, 30)#1
        // ΔR: (1, 10)#1 | ΔS:
        // ΔT: (1, 10, 20, 30)#1
        r_external_in.send(ZTuple { tuple: RawTupleR { a: 1, b: 10 }, count: 1 }).await.unwrap();
        let recv = t_external_out.by_ref().take(1).collect::<Vec<_>>().await;
        assert_eq!(recv[0], ZTuple { tuple: RawTupleT { a: 1, b: 10, c: 20, d: 30 }, count: 1 });

        //  R: (1, 10)#2 |  S: (1, 20, 30)#1
        // ΔR:           | ΔS: (1, 20, 30)#-1
        // ΔT: (1, 10, 20, 30)#-2
        s_external_in.send(ZTuple { tuple: RawTupleS { a: 1, c: 20, d: 30 }, count: -1 }).await.unwrap();
        let recv = t_external_out.by_ref().take(1).collect::<Vec<_>>().await;
        assert_eq!(recv[0], ZTuple { tuple: RawTupleT { a: 1, b: 10, c: 20, d: 30 }, count: -2 });

        //  R: (1, 10)#2 |  S:
        // ΔR: (1, 10)#1 | ΔS: (1, 40, 50)#2, (2, 20, 30)#1
        // ΔT: (1, 10, 40, 50)#6
        r_external_in.send(ZTuple { tuple: RawTupleR { a: 1, b: 10 }, count: 1 }).await.unwrap();
        s_external_in.send(ZTuple { tuple: RawTupleS { a: 2, c: 20, d: 30 }, count: 1 }).await.unwrap();
        s_external_in.send(ZTuple { tuple: RawTupleS { a: 1, c: 40, d: 50 }, count: 2 }).await.unwrap();
        let recv = t_external_out.by_ref().take(1).collect::<Vec<_>>().await;
        assert_eq!(recv[0], ZTuple { tuple: RawTupleT { a: 1, b: 10, c: 40, d: 50 }, count: 6 });
    }
}