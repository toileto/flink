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

# Building a Source (FLIP-27)

Interfaces: `flink-core/src/main/java/org/apache/flink/api/connector/source/`.
Base implementations: `flink-connectors/flink-connector-base/.../source/reader/`.
Docs: `docs/content/docs/connectors/datastream/overview.md`, `sources.md`.

## The four roles

1. **`Source`** — serializable factory: splits, enumerator, reader; declares `Boundedness`
   (`CONTINUOUS_UNBOUNDED`, `BOUNDED`, `FINITELY_BOUNDED`).
2. **`SplitEnumerator`** — runs on the JobManager: discovers splits, assigns them to readers,
   handles reader registration/failure. Keeps assignment state in its snapshot.
3. **`SourceSplit`** — unit of work (file, partition+offset range). Must be serializable with a
   versioned `SimpleVersionedSerializer`.
4. **`SourceReader`** — runs on TaskManagers: fetches records for assigned splits, emits via
   `ReaderOutput`, reports `SourceStatus` (no-more-splits, idle).

## Prefer the base classes

- Extend `SourceReaderBase` (or `SingleThreadMultiplexSourceReaderBase` for one split at a time)
  instead of implementing `SourceReader` from scratch — you provide a `SplitReader`
  (`flink-connector-base/.../source/reader/splitreader/`) and a `RecordEmitter`; the base handles
  the fetch loop, buffering, and checkpointing (`RecordsWithSplitIds`).
- `FutureCompletingBlockingQueue` + `SingleThreadFetcherManager` manage I/O threads; only move to
  multi-threaded fetchers if profiling shows the bottleneck.
- `SplitReader` returns `RecordsWithSplitIds` with timestamps; the emitter assigns them to
  `ReaderOutput` — watermark generation still belongs to the `WatermarkStrategy` at the
  DataStream level, not inside the reader.

## Design checklist

- [ ] Split granularity balances parallelism vs overhead (one split per file/partition, not per record).
- [ ] Reader snapshot = unsent/unfinished splits only; committed offsets live in enumerator state
      or the external system (idempotent restart).
- [ ] Boundedness declared honestly — batch execution and job termination depend on it
      (`SupportsBatchSnapshot` for sources that can snapshot for batch mode).
- [ ] Dynamic parallelism: implement/return `DynamicParallelismInference` info if the source can
      suggest parallelism (files → min(files, max)).
- [ ] Idleness: readers with no data must report idle so watermarks advance
      (or use `withIdleness` at strategy level).
- [ ] Backpressure: fetch loop must pause (return no records) rather than buffer unboundedly.

## Hybrid sources

`HybridSource` (`flink-connector-base/.../source/hybrid/`) chains sources — e.g. read a finite
backlog file source then switch to a continuous queue source — with an explicit
`SwitchSourceEvent` and split-serializer handoff. Use for "backfill then tail" patterns instead
of custom enumeration.

## Testing

- Use `flink-connector-datagen` for downstream tests.
- Source-level tests: enumerator tests (assignment logic) + reader tests (fetch/checkpoint) as
  plain JUnit with mocked `SourceReaderContext`; then one MiniCluster ITCase for the whole
  lifecycle including failure restore (see `flink-datastream-programming/references/testing.md`).
