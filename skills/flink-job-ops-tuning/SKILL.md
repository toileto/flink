---
name: flink-job-ops-tuning
description: "Run, debug, tune, and monitor Apache Flink jobs: the local development loop (MiniCluster, sql-client, flink run), configuration keys (parallelism.default, restart-strategy, memory, execution.checkpointing.*), backpressure and checkpoint monitoring, flame graphs and profiling, metrics and log-based debugging, event-time debugging, and production-readiness checks. Use when the user asks to run a job locally, improve throughput or latency, diagnose slow checkpoints or backpressure, or prepare a job for production. Do NOT trigger for: state backend internals or savepoint migration (flink-state-checkpointing); query or transform logic (flink-sql-table-api, flink-datastream-programming); connector guarantees (flink-connectors-formats); Confluent Cloud operations (confluent-cloud-flink-sql)."
metadata:
  author: "yusuf"
  version: "1.0.0"
  last_updated: "2026-11-17"
compatibility: "Grounded in the Apache Flink 2.5-SNAPSHOT source tree; config keys verified against docs/content/docs."
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

# Flink Job Operations & Tuning

Run Flink jobs locally, diagnose them on a cluster, and tune with measured changes.

## Scope boundary (check first)

- Checkpoint/savepoint mechanics and state backends — `flink-state-checkpointing`.
- Query/transform correctness — `flink-sql-table-api` / `flink-datastream-programming`.
- Confluent Cloud compute pools and `confluent flink` CLI — `confluent-cloud-flink-sql`.

## Local development loop (cheapest first)

| Goal | Command / entry | Grounding |
|---|---|---|
| Try SQL interactively | `./bin/sql-client.sh` with a session YAML | `flink-table/flink-sql-client/`; [sqlClient.md](../../docs/content/docs/dev/table/sqlClient.md) |
| Run a DataStream/SQL jar | `./bin/flink run -c <mainClass> job.jar` | `flink-clients/src/main/java/org/apache/flink/client/`; [cli.md](../../docs/content/docs/deployment/cli.md) |
| Embedded test run | `StreamExecutionEnvironment.getExecutionEnvironment()` in JUnit with MiniCluster | `flink-test-utils-parent/`; [testing.md](../../docs/content/docs/dev/datastream/testing.md) |
| Inspect a plan | `EXPLAIN` in sql-client, or `flink print`/dry-run patterns | `flink-sql-table-api/references/verification.md` |

Build the tree first if needed:

```bash
./mvnw clean install -DskipTests -Dfast -Pskip-webui-build -T1C     # fast dev build
./mvnw -pl <module> -Dtest=SomeTest test                            # single test
./mvnw spotless:apply                                               # format after editing
```

## Configuration map (where things live)

| Concern | Key(s) | Source of truth |
|---|---|---|
| Parallelism | `parallelism.default` (and per-job `setParallelism`) | `flink-core/.../CoreOptions.java` |
| Restart behavior | `restart-strategy` (`fixed-delay`, `failure-rate`, `exponential-delay`, `none`) with `restart-strategy.<strategy>.*` subkeys | `flink-core/.../RestartStrategyOptions.java` |
| Checkpointing | `execution.checkpointing.*` | `flink-core/.../CheckpointingOptions.java` |
| Memory | TaskManager/jobmanager memory sections | [deployment/memory](../../docs/content/docs/deployment/memory/_index.md) |
| Table execution | `table.exec.*` | `flink-table/flink-table-api-java/.../config/ExecutionConfigOptions.java` |
| Full reference | — | [deployment/config.md](../../docs/content/docs/deployment/config.md), `flink-docs` generates the option docs |

**Rule:** find the `ConfigOption` in source before answering "what's the default for X" — the
generated docs are built from these annotations.

## Diagnosis playbook

1. **Job won't start** — check classpath/CLI errors first ([debugging_classloading.md](../../docs/content/docs/ops/debugging/debugging_classloading.md));
   for SQL, `EXPLAIN` to surface unbound tables/options.
2. **Slow / failing checkpoints** — hand off mechanics to `flink-state-checkpointing`; here check
   whether backpressure is the cause ([checkpoint_monitoring.md](../../docs/content/docs/ops/monitoring/checkpoint_monitoring.md)).
3. **Backpressure** — use the backpressure monitor ([back_pressure.md](../../docs/content/docs/ops/monitoring/back_pressure.md));
   the *saturated* operator (not the one that looks busy) is the bottleneck. Flame graphs
   ([flame_graphs.md](../../docs/content/docs/ops/debugging/flame_graphs.md)) and the
   async-profiler integration ([profiler.md](../../docs/content/docs/ops/debugging/profiler.md))
   pinpoint CPU-bound hotspots.
4. **Wrong results with time semantics** — [debugging_event_time.md](../../docs/content/docs/ops/debugging/debugging_event_time.md);
   verify watermarks are advancing (idle sources!) before blaming logic.
5. **Recovery loops** — [task_failure_recovery.md](../../docs/content/docs/ops/state/task_failure_recovery.md):
   read the *first* exception in the TaskManager log, not the cascade.
6. **OOM / memory errors** — [deployment/memory](../../docs/content/docs/deployment/memory/_index.md);
   RocksDB off-heap counts against managed memory, not JVM heap.

## Tuning sequence (throughput/latency)

1. Measure: metrics (`docs/content/docs/ops/metrics.md`), checkpoint durations, source lag.
2. Fix correctness/structure first: reduce shuffles (fewer `keyBy`), avoid `rebalance()` in hot
   paths, batch where possible.
3. Parallelism: raise `parallelism.default` until an external system or network saturates —
   more parallelism is not free (more checkpoints, more state instances).
4. State/backpressure: only then state-backend and checkpoint tuning (flink-state-checkpointing).
5. JVM: GC and memory tuning after the above — [deployment/memory](../../docs/content/docs/deployment/memory/_index.md).

## Production readiness checklist

- [ ] Operator `uid()`s set on stateful operators; savepoint taken pre-migration
      ([upgrading.md](../../docs/content/docs/ops/upgrading.md))
- [ ] Restart strategy chosen deliberately (default may be `none`-ish in some setups — verify)
- [ ] HA configured if a JM restart must not lose the job ([deployment/ha](../../docs/content/docs/deployment/ha/_index.md))
- [ ] Externalized checkpoint retention set if restore-after-cancel is required
- [ ] Metrics/reporters wired ([metrics.md](../../docs/content/docs/ops/metrics.md));
      alert on checkpoint duration, source lag, backpressure ratio
- [ ] Log levels sane ([logging_context.md](../../docs/content/docs/ops/logging_context.md));
      MDC context carried into logs for per-key debugging
- [ ] Load tested at peak with realistic state — see [production_ready.md](../../docs/content/docs/ops/production_ready.md)

## References

- [references/local-dev-loop.md](references/local-dev-loop.md) — sql-client, flink run, MiniCluster
  tests, common startup errors
- [references/tuning.md](references/tuning.md) — config keys with defaults, parallelism/memory/
  restart recipes, what to change first
- [references/debugging-monitoring.md](references/debugging-monitoring.md) — backpressure, flame
  graphs, event-time debugging, metrics that matter, failure triage

## Guardrails

- Present exact commands (cancel, savepoint, config changes on a shared cluster) and wait for
  confirmation before executing anything that affects a running job or shared resources.
- Change one tuning knob at a time; record before/after measurements.
