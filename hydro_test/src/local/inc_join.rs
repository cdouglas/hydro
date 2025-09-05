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
) -> Stream<ZTuple<T>, L, Unbounded, NoOrder>
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
    // quote function ptr
    let r_key_quot: ManualExpr<KR, _> =
        ManualExpr::new(move |ctx: &L| r_key.splice_fn1_borrow_ctx(ctx));
    let s_key_quot: ManualExpr<KS, _> =
        ManualExpr::new(move |ctx: &L| s_key.splice_fn1_borrow_ctx(ctx));
    let merge_quot: ManualExpr<M, _> =
        ManualExpr::new(move |ctx: &L| merge.splice_fn2_borrow_ctx(ctx));

    // extract key from R, S
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

    // join R, S
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
    join_result
}

pub fn dbsp_batch_join<'a, R, S, T, K, KR, KS, M, L, O>(
    r_stream: Stream<ZTuple<R>, L, Unbounded, O>,
    s_stream: Stream<ZTuple<S>, L, Unbounded, O>,
    tick: Tick<L>,
    r_key: impl IntoQuotedMut<'a, KR, Tick<L>> + Copy,
    s_key: impl IntoQuotedMut<'a, KS, Tick<L>> + Copy,
    merge: impl IntoQuotedMut<'a, M, Tick<L>> + Copy,
) -> Stream<ZTuple<T>, L, Unbounded, NoOrder>
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
    join_result
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

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
        ) -> Stream<ZTuple<RawTupleT>, Process<'a>, Unbounded, NoOrder>,
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
        let responses = join_fn(r_stream, s_stream, tick);

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
        ) -> Stream<ZTuple<RawTupleT>, Process<'a>, Unbounded, NoOrder> {
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
        ) -> Stream<ZTuple<RawTupleT>, Process<'a>, Unbounded, NoOrder> {
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

    /// LOGGING TESTS

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
    struct LogEntry<T>
    where
        T: Debug + Clone + Eq + Hash,
    {
        value: ZTuple<T>,
        xid: u64,
    }

    #[tokio::test]
    async fn test_log_write() {
        use hydro_deploy::Deployment;
        use hydro_lang::FlowBuilder;

        let mut deployment = Deployment::new();

        let flow = FlowBuilder::new();
        let process_node = flow.process::<()>();
        let external = flow.external::<()>();

        let (r_send, r_stream) = process_node.source_external_bincode(&external);

        let tick = process_node.tick();

        let log = r_stream
            .batch(&tick, nondet!(/** snapshot of log state */))
            .persist()
            .all_ticks();

        let r_log = log.send_bincode_external(&external);

        let nodes = flow
            .with_process(&process_node, deployment.Localhost())
            .with_external(&external, deployment.Localhost())
            .deploy(&mut deployment);

        deployment.deploy().await.unwrap();

        let mut r_external_in = nodes.connect_sink_bincode(r_send).await;
        let mut listener_stream = nodes.connect_source_bincode(r_log).await;

        deployment.start().await.unwrap();

        r_external_in.send(log_entry(42u32, 1, 1)).await.unwrap();
        r_external_in.send(log_entry(42u32, 1, 2)).await.unwrap();
        let tup = listener_stream
            .by_ref()
            .take(2)
            .collect::<Vec<LogEntry<u32>>>()
            .await;

        assert_eq!(log_content(vec![(42u32, 1, 1), (42u32, 1, 2)]), tup);
    }

    #[tokio::test]
    async fn test_log_write_read() {
        use hydro_deploy::Deployment;
        use hydro_lang::FlowBuilder;

        let mut deployment = Deployment::new();

        let flow = FlowBuilder::new();
        let process_node = flow.process::<()>();
        let external = flow.external::<()>();

        let (r_send, r_stream) = process_node.source_external_bincode(&external);

        let tick = process_node.tick();

        // a stream of ZTuple insertions that came in a batch at a time
        let log = r_stream
            .batch(&tick, nondet!(/**  */))
            .persist()
            .all_ticks();

        // a KeyedSingleton: i.e. a map of tuples to multiplicities
        let log_replayed = log
            .clone()
            .map(q!(|entry: LogEntry<_>| (
                entry.value.tuple,
                entry.value.count
            )))
            .into_keyed()
            .fold_commutative(q!(|| 0i32), q!(|acc, count| *acc += count));

        // a snapshot of log_replayed at the end of this tick
        let snapshot = log_replayed.snapshot(&tick, nondet!(/** **/)).entries();
        // .defer_tick(); XXX do consumers of this flow need us to defer emitting this til end of tick explicitly?

        let r_log = log.send_bincode_external(&external);
        let r_snapshot = snapshot.all_ticks().send_bincode_external(&external);

        let nodes = flow
            .with_process(&process_node, deployment.Localhost())
            .with_external(&external, deployment.Localhost())
            .deploy(&mut deployment);

        deployment.deploy().await.unwrap();

        let mut external_in = nodes.connect_sink_bincode(r_send).await;
        let mut log_stream = nodes.connect_source_bincode(r_log).await;
        let mut snapshot_stream = nodes.connect_source_bincode(r_snapshot).await;

        deployment.start().await.unwrap();

        external_in.send(log_entry(42u32, 1, 1)).await.unwrap();
        external_in.send(log_entry(42u32, 1, 2)).await.unwrap();
        external_in.send(log_entry(0u32, -1, 3)).await.unwrap();
        external_in.send(log_entry(0u32, 1, 4)).await.unwrap();

        let tup = log_stream
            .by_ref()
            .take(4)
            .collect::<Vec<LogEntry<u32>>>()
            .await;

        assert_eq!(
            log_content(vec![
                (42u32, 1, 1),
                (42u32, 1, 2),
                (0u32, -1, 3),
                (0u32, 1, 4)
            ]),
            tup
        );
        let sn = snapshot_stream
            .by_ref()
            .take(2)
            .collect::<Vec<(u32, i32)>>()
            .await;
        chk(Some(vec![(42u32, 2), (0u32, 0)]), sn);
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
    enum Action<K, T>
    where
        T: Debug + Clone + Eq + Hash,
    {
        BEGIN { xid: u64 }, // currently ignored
        READ { xid: u64, seq: u8, key: K },
        WRITE { xid: u64, seq: u8, tuple: ZTuple<T> },
        COMMIT { xid: u64, seq: u8 },
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
    enum LogAction<T>
    where
        T: Debug + Clone + Eq + Hash,
    {
        WRITE { xid: u64, seq: u8, tuple: ZTuple<T> },
        COMMIT { xid: u64, seq: u8 },
    }

    #[tokio::test]
    async fn test_batched_rw() {
        use hydro_deploy::Deployment;
        use hydro_lang::FlowBuilder;

        let mut deployment = Deployment::new();

        let flow = FlowBuilder::new();
        let process_node = flow.process::<()>();
        let external = flow.external::<()>();

        let (incoming, in_stream) = process_node.source_external_bincode(&external);

        let tick = process_node.tick();

        // a stream of ZTuple insertions that came in a batch at a time
        let log = in_stream
            .batch(&tick, nondet!(/**  */))
            .filter_map(q!(|a| match a {
                Action::WRITE { xid, seq, tuple } => Some(LogAction::WRITE { xid, seq, tuple }),
                Action::COMMIT { xid, seq } =>
                    if seq < 32 {
                        // hack for unordered stream; use a bitset to track if begin..end received
                        Some(LogAction::COMMIT { xid, seq })
                    } else {
                        None
                    },
                _ => None,
            }))
            .persist()
            .all_ticks();

        // TODO BEGIN doesn't need seqno 0, assign to first action
        // filter out incomplete transactions i.e., need begin/end/unbroken sequence
        let log_commit = log
            .clone()
            .filter_map(q!(|entry: LogAction<_>| match entry {
                LogAction::COMMIT { xid, seq } => Some((xid, (1 << seq) - 1 as u32)),
                _ => None,
            }))
            .into_keyed()
            .reduce_commutative(q!(|acc, mask| {
                if *acc != mask {
                    // not kosher; error?
                    *acc |= mask;
                }
            }))
            .snapshot(&tick, nondet!(/** */));

        let writes = log
            .clone()
            .filter_map(q!(|entry: LogAction<_>| match entry {
                LogAction::WRITE { xid, seq, tuple } => Some((xid, (seq, tuple))),
                _ => None,
            }));

        let write_mask = writes
            .into_keyed()
            .fold_commutative(
                q!(|| (0u32, Vec::new())),
                q!(|(acc, tuples), (seq, tuple)| {
                    let mask = (1 << seq) as u32;
                    if (*acc & mask) == 0 {
                        // ignore duplicates
                        *acc |= mask;
                        tuples.push(tuple);
                    }
                }),
            )
            .snapshot(&tick, nondet!(/** */));

        let visible = log_commit
            .entries()
            .join(write_mask.entries())
            .filter_map(q!(|(_xid, (bitmask, (mask, tuples)))| {
                if bitmask ^ mask == 0 {
                    Some(tuples)
                } else {
                    None
                }
            }))
            .flatten_unordered();

        // a KeyedSingleton: i.e. a map of tuples to multiplicities
        let log_replayed = visible
            .clone()
            .map(q!(|ztup: ZTuple<_>| (ztup.tuple, ztup.count)))
            .into_keyed()
            .fold_commutative(q!(|| 0i32), q!(|acc, count| *acc += count));

        // a snapshot of log_replayed at the end of this tick
        let snapshot = log_replayed.entries().all_ticks();

        let r_log = log.send_bincode_external(&external);
        let r_snapshot = snapshot.send_bincode_external(&external);

        let nodes = flow
            .with_process(&process_node, deployment.Localhost())
            .with_external(&external, deployment.Localhost())
            .deploy(&mut deployment);

        deployment.deploy().await.unwrap();

        let mut external_in = nodes.connect_sink_bincode(incoming).await;
        let mut log_stream = nodes.connect_source_bincode(r_log).await;
        let mut snapshot_stream = nodes.connect_source_bincode(r_snapshot).await;

        deployment.start().await.unwrap();

        let mut a = Txn::new(1);
        let mut b = Txn::new(2);

        external_in.send(a.begin()).await.unwrap();
        external_in.send(a.write(42u32, 1)).await.unwrap();
        external_in.send(a.write(42u32, 1)).await.unwrap();
        external_in.send(a.commit()).await.unwrap();

        external_in.send(b.begin()).await.unwrap();
        external_in.send(b.read(42u32)).await.unwrap();
        external_in.send(b.commit()).await.unwrap();

        // let tup = log_stream
        //     .by_ref()
        //     .take(4)
        //     .collect::<Vec<LogAction<u32>>>()
        //     .await;

        // assert_eq!(
        //     alog_content(vec![
        //         (42u32, 1, 1),
        //         (42u32, 1, 2),
        //         (0u32, -1, 3),
        //         (0u32, 1, 4)
        //     ]),
        //     tup
        // );
        let sn = snapshot_stream
            .by_ref()
            .take(1)
            .collect::<Vec<(u32, i32)>>()
            .await;
        chk(Some(vec![(42u32, 2)]), sn);
    }

    fn log_entry<T>(tuple: T, count: i32, xid: u64) -> LogEntry<T>
    where
        T: Debug + Clone + Eq + Hash,
    {
        LogEntry {
            value: ZTuple { tuple, count },
            xid,
        }
    }

    struct Txn {
        xid: u64,
        seq: i8,
    }

    impl Txn {
        fn new(xid: u64) -> Self {
            Self { xid, seq: 0 }
        }
        fn begin<K, T>(&mut self) -> Action<K, T>
        where
            K: Debug + Clone + Eq + Hash,
            T: Debug + Clone + Eq + Hash,
        {
            assert!(self.seq == 0i8);
            Action::BEGIN { xid: self.xid }
        }
        fn read<K, T>(&mut self, key: K) -> Action<K, T>
        where
            K: Debug + Clone + Eq + Hash,
            T: Debug + Clone + Eq + Hash,
        {
            assert!(self.seq >= 0i8);
            let seq = self.seq as u8;
            self.seq += 1;
            Action::READ {
                xid: self.xid,
                seq,
                key,
            }
        }
        fn write<K, T>(&mut self, tuple: T, count: i32) -> Action<K, T>
        where
            K: Debug + Clone + Eq + Hash,
            T: Debug + Clone + Eq + Hash,
        {
            assert!(self.seq >= 0i8);
            let seq = self.seq as u8;
            self.seq += 1;
            Action::WRITE {
                xid: self.xid,
                seq,
                tuple: ZTuple { tuple, count },
            }
        }
        fn commit<K, T>(&mut self) -> Action<K, T>
        where
            K: Debug + Clone + Eq + Hash,
            T: Debug + Clone + Eq + Hash,
        {
            assert!(self.seq >= 0i8);
            let seq = self.seq as u8;
            self.seq = -1;
            Action::COMMIT { xid: self.xid, seq }
        }
    }

    fn write_action<T>(tuple: T, count: i32, xid: u64) -> LogAction<T>
    where
        T: Debug + Clone + Eq + Hash,
    {
        LogAction::WRITE {
            xid,
            seq: 0,
            tuple: ZTuple { tuple, count },
        }
    }

    // TODO unfinished, just here for type check
    fn alog_content<T>(values: Vec<(T, i32, u64)>) -> Vec<LogAction<T>>
    where
        T: Debug + Clone + Eq + Hash,
    {
        values
            .into_iter()
            .map(|(tuple, count, xid)| LogAction::WRITE {
                xid,
                seq: 0,
                tuple: ZTuple { tuple, count },
            })
            .collect()
    }

    fn log_content<T>(values: Vec<(T, i32, u64)>) -> Vec<LogEntry<T>>
    where
        T: Debug + Clone + Eq + Hash,
    {
        values
            .into_iter()
            .map(|(tuple, count, xid)| LogEntry {
                value: ZTuple { tuple, count },
                xid,
            })
            .collect()
    }

    fn chk<T>(expected: Option<Vec<T>>, actual: Vec<T>)
    where
        T: Debug + Clone + Eq + Hash,
    {
        if let Some(expected) = expected {
            assert_eq!(expected.len(), actual.len());
            assert_eq!(
                expected.into_iter().collect::<HashSet<_>>(),
                actual.into_iter().collect::<HashSet<_>>()
            );
        } else {
            assert!(actual.is_empty());
        }
    }
}
