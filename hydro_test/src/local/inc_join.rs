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
pub struct ZTuple<T> where T: Debug + Clone + PartialEq + Eq + Hash {
    pub tuple: T,
    pub count: i32,
}

pub fn dbsp_batch_join<'a, R, S, T, K, KR, KS, L, O>(
    r_stream: Stream<ZTuple<R>, Atomic<L>, Unbounded, NoOrder>,
    s_stream: Stream<ZTuple<S>, Atomic<L>, Unbounded, NoOrder>,
    r_key: impl IntoQuotedMut<'a, KR, Tick<L>> + Copy,
    s_key: impl IntoQuotedMut<'a, KS, Tick<L>> + Copy
) -> ((), ()
    // Stream<T, L, Unbounded, NoOrder>,
    // Stream<String, L, Unbounded, NoOrder>, // err
)
    where R: Debug + Clone + PartialEq + Eq + Hash,
          S: Debug + Clone + PartialEq + Eq + Hash,
          T: Debug + Clone + PartialEq + Eq + Hash,
          K: Debug + Clone + PartialEq + Eq + Hash,
          KR: Fn(&R) -> K + 'a,
          KS: Fn(&S) -> K + 'a,
          L: Location<'a> + NoTick + NoAtomic
{
    let r_key_quot: ManualExpr<KR, _> = ManualExpr::new(move |ctx: &Tick<L>| r_key.splice_fn1_ctx(ctx));
    let s_key_quot: ManualExpr<KS, _> = ManualExpr::new(move |ctx: &Tick<L>| s_key.splice_fn1_ctx(ctx));

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
    let dr_x_ds = dr_kstream.join(ds_kstream);
    //  R x ΔS
    let r_x_ds = r_kstream.join(ds_kstream);
    // ΔR x  S
    let s_x_dr = s_kstream.join(dr_kstream);

    ((), ())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_batch_join_basic() {
    }
}