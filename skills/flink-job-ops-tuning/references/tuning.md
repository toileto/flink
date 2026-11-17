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

# Tuning recipes

Full config reference: `docs/content/docs/deployment/config.md` (generated from the
`ConfigOption` definitions listed below).

## Parallelism

- `parallelism.default` (source: `flink-core/src/main/java/org/apache/flink/configuration/CoreOptions.java`)
  — job-wide default; override per job/program for isolation.
- Rules: parallelism ≤ total slots; watch key-group cardinality (default max-key-group count caps
  effective parallelism of keyed streams); more parallelism = more checkpoint files and more
  state instances.
- Scale-out: `docs/content/docs/deployment/elastic_scaling.md`, YARN/K8s autoscalers under
  `deployment/resource-providers/`.

## Restart strategy

Key: `restart-strategy` with values `fixed-delay`, `failure-rate`, `exponential-delay`, `none`
(subkeys `restart-strategy.<strategy>.*`; definitions in
`flink-core/src/main/java/org/apache/flink/configuration/RestartStrategyOptions.java`).

| Job profile | Strategy | Rationale |
|---|---|---|
| Deterministic code, transient infra faults | `fixed-delay` with modest attempts | Simple, predictable |
| Poison-pill / repeatable failures | `failure-rate` | Stops crash loops, surfaces the bug |
| Variable-duration outages (cloud) | `exponential-delay` | Backs off without giving up |
| Batch or must-fail-fast | `none` | Fail immediately for inspection |

Never use unlimited retries to paper over a deterministic bug — the failure loop hides the root
cause and burns cluster resources.

## Memory

- Read `docs/content/docs/deployment/memory/_index.md` before touching `-Xmx`.
- Managed memory (RocksDB/ForSt, batch operators) vs JVM heap vs network buffers — mis-sizing
  shows up as `OutOfMemoryError: direct buffer memory` or ContainerMetasOOM.
- RocksDB with `state.backend.rocksdb.memory.managed: true` (default) participates in managed
  memory — don't double-budget.

## Checkpoint interaction

Checkpoint tuning lives in `flink-state-checkpointing`; from the ops side, only ensure
`execution.checkpointing.dir` storage bandwidth and `tolerable-failed-checkpoints` match the
environment's flakiness.

## Table/SQL execution

`table.exec.*` options (`flink-table/flink-table-api-java/.../config/ExecutionConfigOptions.java`):

- `table.exec.resource.default-parallelism` — SQL parallelism override.
- State clean-up: `table.exec.state.ttl` (seconds) for aggregations — enables RocksDB compaction
  of expired keys; set deliberately, it silently drops old state.
- `table.exec.mini-batch.*` — aggregate mini-batching trades latency for throughput on
  high-update streams.

## One-knob-at-a-time protocol

1. Write down the metric you're improving (records/s, p99 latency, checkpoint ms).
2. Change one knob; rerun the same load (datagen with fixed seed where possible).
3. Keep the change only if the metric improves and correctness checks still pass.
4. Record old/new values in the job's README or PR description.
