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
    let r_key_quot: ManualExpr<KR, _> = ManualExpr::new(move |ctx: &Tick<L>| r_key.splice_fn1_ctx(ctx));
    let s_key_quot: ManualExpr<KS, _> = ManualExpr::new(move |ctx: &Tick<L>| s_key.splice_fn1_ctx(ctx));
    let merge_quot: ManualExpr<M, _> = ManualExpr::new(move |ctx: &Tick<L>| merge.splice_fn2_ctx(ctx));

    let r = r_stream.clone()
        .map(q!(|ztuple| (ztuple.tuple, ztuple.count)))
        .into_keyed()
        .fold_commutative(q!(|| 0i32), q!(|acc, count| *acc += count))
        .filter(q!(|count| *count != 0))
        .snapshot(nondet!(/** rollup R state */))
        .entries()
        .map(q!(|(tuple, count)| ZTuple { tuple, count })) // XXX this seems wildly inefficient
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

    // TODO: join on KeyedStream not complete, yet
    let r_kstream = r.clone()
        .map(q!(move |ztuple: ZTuple<R>| (r_key_quot(&ztuple.tuple), ztuple)))
        .into_keyed();
    let s_kstream = s.clone()
        .map(q!(move |ztuple: ZTuple<S>| (s_key_quot(&ztuple.tuple), ztuple)))
        .into_keyed();
    let dr_kstream = delta_r
        .map(q!(move |(tuple, count): (R, i32)| (r_key_quot(&tuple), ZTuple { tuple, count })))
        .into_keyed();
    let ds_kstream = delta_s
        .map(q!(move |(tuple, count): (S, i32)| (s_key_quot(&tuple), ZTuple { tuple, count })))
        .into_keyed();

    // ΔR x ΔS
    let dr_x_ds = dr_kstream.clone().entries().join(ds_kstream.clone().entries());
    //  R x ΔS
    let r_x_ds = r_kstream.entries().join(ds_kstream.entries());
    // ΔR x  S
    let dr_x_s = dr_kstream.entries().join(s_kstream.entries());

    let join_result = dr_x_ds.chain(r_x_ds).chain(dr_x_s)
        .map(q!(move |(_key, (ztuple_r, ztuple_s)): (K, (ZTuple<R>, ZTuple<S>))| {
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

    #[tokio::test]
    async fn test_batch_join_basic() {
        use hydro_deploy::Deployment;
        use hydro_lang::FlowBuilder;

        let mut deployment = Deployment::new();

        let flow = FlowBuilder::new();
        let process_node = flow.process::<()>();
        let external_r = flow.external::<()>();
        let external_s = flow.external::<()>();
        let external_out = flow.external::<()>();

        let (r_port, r_stream_in, _r_membership, _r_complete_sink) =
            process_node.bidi_external_many_bincode::<(), ZTuple<(u32, u32)>, ()>(&external_r);
        let (s_port, s_stream_in, _s_membership, _s_complete_sink) =
            process_node.bidi_external_many_bincode::<(), ZTuple<(u32, u32, u32)>, ()>(&external_s);
        let (out_port, _out_stream, _out_membership, out_complete_sink) =
            process_node.bidi_external_many_bincode::<(), (), ZTuple<(u32, u32, u32, u32)>>(&external_out);

        let nodes = flow
            .with_process(&process_node, deployment.Localhost())
            .with_external(&external_r, deployment.Localhost())
            .with_external(&external_s, deployment.Localhost())
            .with_external(&external_out, deployment.Localhost())
            .deploy(&mut deployment);


        deployment.deploy().await.unwrap();

        let (_r_external_out, mut r_external_in) = nodes.connect_bincode(r_port).await;
        let (_s_external_out, mut s_external_in) = nodes.connect_bincode(s_port).await;
        let (mut out_external_out, _out_external_in) = nodes.connect_bincode(out_port).await;

        deployment.start().await.unwrap();

        let tick = process_node.tick();
        let (responses, _errors) = dbsp_batch_join(
            r_stream_in.values().atomic(&tick),
            s_stream_in.values().atomic(&tick),
            q!(|(a, _b): &(u32, u32)| *a),
            q!(|(a, _c, _d): &(u32, u32, u32)| *a),
            q!(|&(a_r, b), &(a_s, c, d)| {
                if a_r != a_s {
                    panic!("{:?} != {:?}", a_r, a_s)
                } else {
                    (a_r, b, c, d)
                }
            })
        );

        out_complete_sink.complete(responses.map(q!(|z| (0, z))).into_keyed());

        // Send test data
        r_external_in.send(ZTuple { tuple: (1, 10), count: 1 }).await.unwrap();
        s_external_in.send(ZTuple { tuple: (1, 20, 30), count: 1 }).await.unwrap();

        // Receive results
        let recv = out_external_out.by_ref().take(1).collect::<Vec<_>>().await;
        dbg!(&recv);
    }
}