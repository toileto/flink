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

# State backends in depth

Docs: `docs/content/docs/ops/state/state_backends.md`,
`docs/content/docs/dev/datastream/fault-tolerance/state_backends.md`.
Source: `flink-state-backends/`.

## Heap (hashmap) backend

- State lives in TaskManager JVM heap — fastest access, bounded by memory.
- Checkpoints serialize all state every time (no incremental).
- Choose when: state comfortably fits (rough rule: < 50% of TM managed memory) and checkpoint
  latency targets are modest.

## RocksDB backend

- On-disk, column-family-per-state-backend; supports **incremental checkpoints**
  (`execution.checkpointing.incremental: true`) — only SST files newer than the last checkpoint
  are uploaded.
- Memory: `state.backend.rocksdb.memory.managed: true` (default) shares managed memory with
  Flink; only disable when hand-tuning `ColumnFamilyOptions` via
  `state.backend.rocksdb.options-factory`.
- Related keys: `state.backend.rocksdb.predefined-options`,
  `state.backend.rocksdb.memory.write-buffer-ratio`,
  `state.backend.rocksdb.memory.high-prio-pool-ratio`,
  `state.backend.rocksdb.timer-service.factory`.
- Cost: per-record (de)serialization and slower access than heap — for hot small state mixed
  with large cold state, consider TTL or splitting the job.

## ForSt backend (experimental)

- Fork of RocksDB with decoupled compute/storage (`flink-statebackend-forst/`).
- Keys: `state.backend.forst.executor.read-io-parallelism`,
  `state.backend.forst.executor.write-io-parallelism`, `state.backend.forst.executor.inline-write`,
  `state.backend.forst.executor.inline-coordinator`.
- Evaluate against RocksDB on your workload before adopting; API-stability annotation is
  experimental.

## Changelog state backend

- Wrapper (`flink-statebackend-changelog/`) that appends state changes to a changelog
  (uploaded by `flink-dstl/flink-dstl-dfs/`, storage configured by `state.changelog.storage`)
  while materializing full snapshots in the background
  (`state.changelog.periodic-materialize.interval`).
- Effect: checkpoint time becomes ~constant regardless of state size; restore replays changelog
  on top of the last materialized snapshot.
- Trade-off: extra upload traffic and more complex restore; enable when checkpoints can't keep up
  with `execution.checkpointing.interval`, especially under backpressure
  (`docs/content/docs/ops/state/checkpointing_under_backpressure.md`).

## Disaggregated state

For very large state with cloud object storage, see
`docs/content/docs/ops/state/disaggregated_state.md` — state stored remotely rather than on
local disk.

## Decision flow

1. State fits heap and jobs checkpoint fine → heap backend.
2. State exceeds heap or full snapshots too big → RocksDB + incremental.
3. Checkpoints still exceed interval (barrier alignment/backpressure) → changelog backend.
4. Cloud-native / remote state requirements → evaluate ForSt or disaggregated state docs.

## Verification

- Unit: backend-specific tests under each `flink-state-backends/*/src/test/`.
- Integration: checkpoint/restore ITCases in `flink-tests/` — always test a savepoint restore
  across the change, not just a fresh start.
