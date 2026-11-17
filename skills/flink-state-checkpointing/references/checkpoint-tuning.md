<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Checkpoint tuning

Docs: `docs/content/docs/ops/state/checkpoints.md`,
`docs/content/docs/ops/monitoring/checkpoint_monitoring.md`,
`docs/content/docs/ops/state/large_state_tuning.md`.

## Reading a checkpoint

Key metrics/durations per checkpoint:

1. **Alignment time** — barriers reaching all subtasks. Long alignment = backpressure or an
   unbalanced/slow source. Fix throughput first; only then consider
   `execution.checkpointing.mode: at_least_once` (trades correctness) or unaligned checkpoints.
2. **Async upload/write time** — state size and storage bandwidth. Fixes: incremental RocksDB
   checkpoints, faster storage, changelog backend, smaller state (TTL, compaction of history).
3. **Failure** — check logs for the subtask that failed; common causes: storage timeout
   (`execution.checkpointing.timeout`), disk full, exceeded `tolerable-failed-checkpoints`.

## Tuning knobs (in order of preference)

1. Remove the bottleneck (backpressure, oversized state) — never mask it.
2. `execution.checkpointing.incremental: true` with RocksDB/ForSt for large state.
3. Changelog backend when alignment/write can't fit in the interval.
4. Raise `execution.checkpointing.interval` only as a last resort; it directly increases
   post-failure replay time and recovery duration.
5. `execution.checkpointing.max-concurrent-checkpoints: 1` (default) — only raise for very fast
   checkpoints; `min-pause` prevents checkpoint flooding.

## Backpressure interaction

Under sustained backpressure, checkpoints stretch and eventually time out. Sequence:

1. Diagnose the bottleneck (operator, source lag, sink) — `flink-job-ops-tuning`.
2. If the job must checkpoint during known backlog, `execution.checkpointing.interval-during-backlog`
   lets checkpoints pause/resume with the workload.

## Storage guidance

- `execution.checkpointing.dir` should be a fault-tolerant, high-throughput filesystem (S3/HDFS/
  GCS with throughput ≥ aggregate state-change rate). Local disk is not HA-safe.
- `execution.checkpointing.file-merging.*` options reduce small-file pressure on object stores
  for many parallel subtasks.
- Retention: `execution.checkpointing.num-retained` + externalized retention policy; keep at
  least one completed checkpoint if the job should be restorable after cancel.

## Validation loop

1. Baseline: record checkpoint durations/alignment from the UI or metrics before changes.
2. Apply one change at a time; re-run a workload with realistic state size (not an empty state).
3. Kill a TaskManager deliberately in staging and measure full recovery time end-to-end —
   checkpoint duration alone is not the recovery cost.
