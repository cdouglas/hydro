- [x] **JOIN Investigation**: DBSP-style explicit bilinear incremental join in Hydro
    - [x] ensure that Hydro's (symmetric hash) join produces the same results
- [ ] **Agg Investigation**: ensure that Hydro's (hash) groupby produces the same results as DBSP's lifted GROUPBY/AGG
- [x] **Logging/Recovery (blind writes)**: write a small version of a transaction log flow that is differentiable
    - [x] **Log Schema**: decide on the UPDATE log record structure `(K, ZTuple?, xid)`
    - [x] **Materialize Log**: write a flow that "materializes" the log in Hydro internal state
        ```rust
             source_stream()
            .map(/* group by K */.into_keyed()
            .fold((|| []), (|z, acc| *acc.push(z)) // THIS IS A LOG
        ```
        - [x] **workload generator**: we need to generate log traffic
- [ ] **Play the Log**: extend that flow to "play the log" and produce a KVS snapshot as of transaction T. "just works" via folding the log?
    ```rust
    /* log flow above */
    .flatten()
    .fold_commutative((|| identity), |z, acc| *acc += z)
    .batch()`? // THIS IS A DB SNAPSHOT (INTEGRATE)
    .persist() // THIS IS A STORED SNAPSHOT .. ATTACH A "TIMESTAMP" VIA MAP
    ```
- [ ] **Enhance log with commit records**
    - [ ] Extend log schema (wrap w/ enum)
    - [ ] Capture committed transactions with associated timestamps
    - [ ] Extend DB snapshot generation logic to "skip" uncommitted transactions
- [ ] **Support Timestamped Reads**: figure out how reads work on committed data and attach appropriate timestamps
    - [ ] Selectively replay log up to a timestamp
- [ ] **Identifying Conflicts**: write a inter-transaction conflict detection flow
- [ ] **The Secret Sauce**: reordering conflicts rather than aborting on conflict. maybe this "just works" thanks to ZTuples/Abelian Groups??
- [ ] **GC/Efficiency**:
    - [ ] Worry about garbage collection. Lattices/Tombstones?
    - [ ] Flush log head to persistent store
    - [ ] Minimize log replay overheads