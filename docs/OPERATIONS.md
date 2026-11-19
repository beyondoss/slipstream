# Operations and sizing

How many watchers, writes and bytes one NATS JetStream KV bucket can carry, how to recover from each failure, and what to alert on.

Measured with the published `beyond-slipstream` 0.8.6, on self-hosted NATS 2.15.3 on AWS spot instances. The behavior these measurements describe is unchanged in 0.9.0. Limits depend on hardware and workload. Re-measure before you rely on these numbers for another NATS release or another write mix.

## Terms

| Term            | Meaning                                                                                              |
| --------------- | ---------------------------------------------------------------------------------------------------- |
| Watcher         | A `watch_applied` consumer that follows one bucket and keeps a local fold.                           |
| Fold            | The local copy of the bucket on one node. The fold is the replica of record.                         |
| Cursor          | A stream sequence number. The fold has applied everything up to it.                                  |
| Retention       | How long NATS keeps data. Set by `max_bytes`, `max_age` and `discard`. Older entries are evicted.    |
| Artifact        | A fold exported to object storage (S3), with its cursor.                                             |
| Canary          | A watcher with a full fold on its own host. Its latency and digest show whether the fleet is correct. |

Slipstream does not replay evicted data from NATS. A cursor older than the retention window expires. Recovery then needs an artifact that is newer than the local fold and still inside the window.

## What to size

| Input            | Unit          | How to get it                                                                 |
| ---------------- | ------------- | ----------------------------------------------------------------------------- |
| Watchers `N`     | consumers     | Count of `watch_applied` consumers on one stream.                             |
| Writes `w`       | writes/s      | Puts, drains and deletes into the bucket. Measure the mean over an hour.      |
| Window `W`       | s             | `(last_seq - first_seq) / w`, read from the stream state.                     |
| Export interval `E` | s          | Exporter cadence. Must be well below `W`.                                     |
| Live record size | bytes/record  | Stream bytes / messages for live values. Measured: 524 B (439 B value, 43 B key) in a formula test; 222 B per route in the Beyond-shaped run. Measure your own. |
| Marker size      | bytes/record  | Measured: 120 B per delete marker.                                            |

Retention in bytes, with `c` creates/s, value life `L` and window `W`:

```
bytes = c × (L × live_size + (W − L) × marker_size)   if W > L
bytes = c × W × live_size                             otherwise
```

Given `max_bytes`, solve for `W`. Or read `W` from the sequence span, as above. Measured at 200 writes/s: 64k sequences span about 320 s.

Formula accuracy, in harness ticks:

- Short life (L = 30 ticks): measured window 590. Formula 564 with 750 B and 115 B constants (4.4 % under). Formula 598 with the measured 524 B and 120 B (1.4 % under).
- Long life (L = 300 ticks): formula 112 to 160. Measured 129 (oldest log age) to 429 (creation window). The formula errs low. For export sizing that is the safe direction: it makes you export sooner, not later.

Disk per fold: a RocksDB fold preallocates about 286 MB at open. Keep that in mind for many folds on one disk. Three hundred empty folds used 84 GB. Five hundred filled a 96 GB disk.

## Limits

Each row is one bucket on one R1 stream, with 3-node NATS clusters of 8 vCPU per node on EBS gp3 storage. Latency is scheduled write to fold applied, measured on a canary that holds a full fold. Load watchers held no fold. Each row is one search step of 60 s warm-up and 150 s window, n=1, unless noted.

| Watchers | Writes/s | Canary p99 | NATS egress msg/s | Verdict                                  |
| -------: | -------: | ---------: | ----------------: | ---------------------------------------- |
|       30 |    3,200 |     13.9 ms |              173k | Good. 0 write failures.                  |
|      100 |      800 |     14.7 ms |              136k | Good.                                    |
|      100 |    1,600 |      768 ms |              219k | Marginal. p999 1,228 ms.                 |
|      100 |    3,200 |    2,783 ms |              536k | Broken.                                  |
|      300 |      400 |      627 ms |              218k | Good.                                    |
|      300 |      800 |    5,132 ms |              417k | Broken. 1.8 % write failures.            |
|      500 |      200 |      643 ms |              202k | Search result, n=1. Marginal. One canary was reclaimed. |
|      500 |      200 | 875 ms, 1.34 s, 1.42 s | n/a         | 3 x 10 min checks on 8 vCPU NATS hosts, R1. 2 of 3 failed. Raw results not retained. |
|      500 |      200 |    1,336 ms | n/a               | 4 vCPU NATS hosts (m6a.xlarge). Broken. NATS 3.0 of 4 cores. |
|      700 |      200 |    1,331 ms |              248k | Broken.                                  |
|    1,000 |      200 |  1.3 to 6.7 s |         300k to 402k | Broken in all five runs.           |

Other limits:

| Limit                          | Value                                              | Notes                                                                           |
| ------------------------------ | -------------------------------------------------- | ------------------------------------------------------------------------------- |
| Watchers per stream (NATS default) | 1,000                                          | Above this, new watchers fail with JetStream error 10026. Raise `jetstream { limits { default_max_consumers } }`. |
| Break point, one R1 stream     | About 220k to 250k NATS egress msg/s               | Good rows measured 136k to 219k. Broken rows measured 248k to 536k. Not a formula. |
| Single-replica apply           | 340,236 updates/s (RocksDB), 173,616 (fjall)       | Store apply only, 10M keys, batch 100. Not the limit in the fan-out tests.       |
| Window at 200 writes/s         | About 320 s                                        | 10 MiB origins bucket, history 1, `discard: old`.                               |
| Window at 1,000 writes/s       | About 65 s                                         | Same bucket. Window is inversely proportional to write rate.                    |
| Restore time, 1M-key fold      | 7.8 to 10.4 s                                      | From a 60 s export. 4 concurrent restores.                                      |
| Fresh-node import, 1M routes   | 127 ms to 1.2 s local. 12 to 15 s with 25 concurrent on AWS | Artifact 20.6 MB.                                                       |

The limit is one stream leader's file store. Its lock serializes the publisher and every caught-up consumer. CPU, network and disk were not saturated at the break. Adding watchers adds NATS egress until the leader stops keeping up.

## Choosing R1, R3 or split buckets

| Option        | Use when                                                                 | Cost                                                                                   | Measured effect at 1,000 watchers, 200 writes/s |
| ------------- | ------------------------------------------------------------------------ | -------------------------------------------------------------------------------------- | ----------------------------------------------- |
| R1 (default)  | Watchers and writes within the good rows of the limits table.             | One copy. The leader is a single point for the stream.                                 | p99 1.6 s and 4.4 s in two A/B runs.            |
| R3            | Watchers near 1,000, or any stream that must survive a leader loss.       | 3x storage and replication. 2.1 NATS cores per node, against 1.9 on the R1 leader (one run). Set at stream creation or by stream edit. | p99 238 ms, 541 ms and 703 ms in three runs. |
| Split (N buckets) | Each watcher needs only one shard of the keys.                        | Client routing. No global fold across buckets. N streams.                              | p99 15 ms with N=4.                             |

Split does not reduce fan-out for consumers that must see every key. Each shard obeys the same limit. Use it only when a watcher can follow one shard.

## Export interval and retention

Keep `E` well below `W`. A restore needs an artifact that is ahead of the local fold and whose cursor is still inside the window.

Measured with a 900 s export at 200 writes/s and a 320 s window:

- 25 of 25 expired watchers restored, because their artifact happened to be 140 s old.
- Restores were refused when the artifact was not ahead of the fold. Four folds stayed stale for at least 7 minutes, until a newer artifact landed.

Measured with a 60 s export: 3 of 3 expired-cursor repetitions restored, 0 refused.

At 1,000 writes/s the window is about 65 s. A 60 s export is already at the edge.

## Recovery

| Condition                                            | What happens                                                                                  | Measured                                  | Operator action                                                                                                       |
| ---------------------------------------------------- | --------------------------------------------------------------------------------------------- | ----------------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| Watcher stopped inside the window                    | Resumes from its own cursor. No restore.                                                      | Tail replay of about 15k messages.        | None.                                                                                                                 |
| Cursor expired, artifact newer than fold and inside window | Restores from the artifact, then resumes from its cursor.                             | 7.8 to 10.4 s for 1M keys.                | None.                                                                                                                 |
| Cursor expired, artifact not ahead of fold           | Restore refused. The watch fails and retries with backoff. The fold stays stale.             | Stale until a newer artifact lands.       | Make the exporter produce a fresh artifact: restart or relocate the lease holder, or shorten `E`. Or delete the stale fold to bootstrap from the newest artifact. |
| Artifact missing                                     | Same path as "not ahead of fold": no artifact, no restore.                                    | Not measured with the artifact deleted. The refusal path was measured. | Re-export, and confirm a new artifact exists. Then follow the row above.                                 |
| Node join (fresh node)                               | Imports the newest artifact. Never re-lists NATS to rebuild a bounded fold.                   | 127 ms to 1.2 s local. 12 to 15 s on AWS with 25 concurrent. | Start the node after an artifact exists. Stagger starts for large fleets.                                  |
| Restart of 300 watchers at once                      | Write acks slow, then recover.                                                                | 0 failures of 46,218 writes. Ack max 684 ms for about 30 s. Back to 16 ms within 40 s. | None.                                                                                                       |
| 1,000 consumers created at once (join storm)         | Writes fail until the consumers settle.                                                       | 2,248 failures of 48,000 writes over 20 s, up to 17,339 over 85 s. | Stagger consumer creation.                                                                              |
| R3 stream leader killed                              | Writes fail until a new leader is elected. Folds catch up.                                    | 530 to 1,277 failed writes per kill, over 4 to 5 s. Folds caught up 7 to 9 s after the kill. 0 bad keys. | None. Alert on write failures.                                                             |
| NATS process restart, R1 leader loss, rolling restart, single node join | Not measured.                                                              | n/a                                       | Treat as unproven. Test in staging before you rely on it.                                                             |
| Fold lost on a node                                  | Rebuild from the newest artifact. Do not re-list NATS.                                        | See "Node join".                          | Delete the local fold. Restart the node.                                                                              |

## Recreating a bucket

Until the recreated-stream fix is released, do this before you recreate a bucket:

1. Stop every watcher on the bucket.
2. Delete the bucket's artifact prefix in S3.
3. Delete every local fold of the bucket, on every node.
4. Recreate the bucket.
5. Start the exporter. Start the watchers after the first artifact exists.

Cursors and artifacts from the old stream mean nothing in the new stream.

After the recreated-stream fix is released, slipstream refuses a cursor or artifact from another stream incarnation. Keep the steps above until the release is running on every node.

## Alerts

| Signal                                  | Why                                                                                              | Alert when                                                                          |
| --------------------------------------- | ------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------- |
| Writer ack latency (p99)                | Acks slow before folds lag. At the break, ack p99 reached 4.3 s with 1,744 writes in flight.     | It rises above your write timeout budget.                                           |
| Watchers × writes/s per stream          | This is the load the leader carries. Compare it with the limits table.                           | It reaches the last good row for your hardware.                                     |
| Artifact age vs window                  | A restore needs an artifact inside the window. At 200 writes/s with a 900 s interval it is not.  | Artifact age reaches the window `W`.                                                |
| Apply in flight                         | A store apply has no timeout. A hung apply stops the watch with no error. `ApplyProbe::in_flight()` reports its age. | Age passes your limit. Then restart the process.                       |
| Fold lag (canary write-to-applied)      | The fleet's correctness signal. A watcher without a fold cannot show divergence.                 | Canary p99 rises above your latency budget for the stream.                          |
| Cursor expired, error log               | A watch that cannot restore fails at `error`.                                                     | Any occurrence on a node that should be current.                                    |
| Write failures per stream               | Leader loss and fan-out saturation both show here.                                               | Any sustained failures.                                                             |

A health snapshot API for these signals is being added. It is not in this release.
