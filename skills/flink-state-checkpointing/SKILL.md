---
name: flink-state-checkpointing
description: "Handle Apache Flink state and fault tolerance: keyed state (ValueState, ListState, MapState, BroadcastState), state backend selection (heap, RocksDB, ForSt, changelog), checkpoint and savepoint configuration and tuning, exactly-once semantics, state TTL, state schema migration after code changes, and diagnosing failed or slow checkpoints. Use when the user asks about state size, checkpoint duration or failures, savepoints, restoring state after a serializer/schema change, or which state backend to pick. Do NOT trigger for: writing query or window logic (flink-sql-table-api, flink-datastream-programming); connector delivery guarantees (flink-connectors-formats); cluster metrics, backpressure, or job tuning in general (flink-job-ops-tuning)."
metadata:
  author: "yusuf"
  version: "1.0.0"
  last_updated: "2026-11-17"
compatibility: "Grounded in the Apache Flink 2.5-SNAPSHOT source tree; config keys verified against docs/content/docs/ops/state."
---
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

# Flink State & Checkpointing

Use managed state correctly, pick a backend, configure checkpoints, and migrate state safely.

## Scope boundary (check first)

- *Accessing* state inside a function (descriptors, timers) — see
  `flink-datastream-programming/references/transformations.md`; this skill covers backend choice,
  durability, tuning, and migration.
- Sink-side transactional guarantees — `flink-connectors-formats`.
- Diagnosing backpressure/metrics on a running cluster — `flink-job-ops-tuning` (this skill covers
  checkpoint-specific diagnosis).

## Choose a state backend

| State size / access pattern | Backend | Module |
|---|---|---|
| Small state, fits in TaskManager heap | `state.backend.type: hashmap` (default heap) | `flink-core` runtime |
| Large state, incremental checkpoints | RocksDB (`EmbeddedRocksDBStateBackend`) | `flink-state-backends/flink-statebackend-rocksdb/` |
| RocksDB alternative (experimental) | ForSt | `flink-state-backends/flink-statebackend-forst/` |
| Spillable heap state | heap-spillable | `flink-state-backends/flink-statebackend-heap-spillable/` |
| Very frequent checkpoints / slow full snapshots | changelog wrapper over any backend | `flink-state-backends/flink-statebackend-changelog/` + `flink-dstl/flink-dstl-dfs/` (storage via `state.changelog.storage`) |

Rules of thumb:

- Heap backend + small state = simplest; RocksDB when state exceeds heap budget or checkpoints are
  too large/slow. RocksDB settings: `state.backend.rocksdb.memory.managed` (keep `true` unless
  tuning manually), `state.backend.rocksdb.predefined-options`, `state.backend.rocksdb.options-factory`.
- Changelog backend decouples checkpoint duration from state size (logs deltas to
  `flink-dstl-dfs`, materializes in background via
  `state.changelog.periodic-materialize.interval`). Enable when checkpoints exceed target
  interval under backpressure — see
  [checkpointing under backpressure](../../docs/content/docs/ops/state/checkpointing_under_backpressure.md).
- Timer service on RocksDB defaults to RocksDB-backed timers; `state.backend.rocksdb.timer-service.factory`
  can be set to `heap` for small timer counts.

Full comparison: [references/state-backends.md](references/state-backends.md).

## Configure checkpoints

Core keys (verified against `flink-core/src/main/java/org/apache/flink/configuration/CheckpointingOptions.java`
and the ops docs):

| Key | Meaning |
|---|---|
| `execution.checkpointing.interval` | Checkpoint period; pair with `execution.checkpointing.min-pause` |
| `execution.checkpointing.mode` | `exactly_once` (default) or `at_least_once` |
| `execution.checkpointing.timeout` | Fail the checkpoint after this long |
| `execution.checkpointing.dir` / `execution.checkpointing.savepoint-dir` | Storage locations |
| `execution.checkpointing.incremental` | Incremental (RocksDB/ForSt only — huge win for large state) |
| `execution.checkpointing.max-concurrent-checkpoints`, `.tolerable-failed-checkpoints` | Scheduling/failure tolerance |
| `execution.checkpointing.externalized-checkpoint-retention` | Keep checkpoints after cancel (needed for restore) |

Exactly-once = consistent snapshot **plus** sources that can replay and sinks that commit on
checkpoint completion (two-phase commit). A sink with `at_least_once` breaks end-to-end
exactly-once regardless of checkpoint config — see `flink-connectors-formats`.

## Diagnosis loop (slow/failed checkpoints)

1. Open the checkpoint tab / `docs/content/docs/ops/monitoring/checkpoint_monitoring.md`:
   look at alignment duration vs. write duration.
2. **Alignment slow** → backpressure or barrier congestion: fix backpressure first
   (flink-job-ops-tuning), consider changelog backend.
3. **Write slow / large increments** → state too big for full snapshots: enable
   `execution.checkpointing.incremental`, tune RocksDB memory, or enable changelog.
4. **Failed checkpoints** → check `execution.checkpointing.tolerable-failed-checkpoints`,
   disk/NFS on `execution.checkpointing.dir`, and task-level failure causes in the logs.
5. Never fix by blindly raising `execution.checkpointing.interval` — that widens the replay
   window after failure.

Details: [references/checkpoint-tuning.md](references/checkpoint-tuning.md).

## State schema migration

Changing a state type/serializer can make checkpoints unrestorable:

1. State serializers are versioned — a `TypeSerializerSnapshot` must implement compatibility
   resolution for old versions (`getCurrentVersion()` / `readSnapshot`). Never change a
   serializer's format without bumping its snapshot version.
2. Prefer additive evolution: new optional field with default, not renamed/reordered mandatory ones.
3. For incompatible changes, use a savepoint + the state migration path described in
   `docs/content/docs/dev/datastream/fault-tolerance/state_migration.md`; test the migration with a
   savepoint from the **old** jar restoring under the **new** jar before touching production.
4. Operator UIDs must be stable — state restores by UID; a missing/changed `uid()` silently
   starts empty state.

Details: [references/savepoints-migration.md](references/savepoints-migration.md).

## State TTL

Programmatic via `StateTtlConfig` (`flink-core/src/main/java/org/apache/flink/api/common/state/StateTtlConfig.java`)
on the state descriptor. TTL applies to reads, writes, and full-state snapshots; expired entries
are cleaned lazily — TTL is not a hard memory bound. Document TTL semantics (update-on-read vs
creation-time) in the job's README if downstream correctness depends on it.

## Guardrails

- Savepoint/cancel/restore operations on a running cluster: present the exact commands and wait
  for confirmation before executing.
- Never delete a savepoint or checkpoint directory without explicit user confirmation.
- No Java serialization for state types — use Flink's type system or Kryo only as a last resort
  (Kryo states are not schema-evolvable).
