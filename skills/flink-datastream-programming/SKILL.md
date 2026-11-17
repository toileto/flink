---
name: flink-datastream-programming
description: "Build Apache Flink DataStream API programs in Java: transforms (map, filter, keyBy, connect), event time and watermarks, tumbling/sliding/session windows, ProcessFunction and KeyedProcessFunction with timers and side outputs, async I/O, operator UIDs and chaining, and choosing between the classic DataStream API and the newer DataStream v2 API. Use when the user asks to write or debug a streaming job in the DataStream API, handle late data, or implement custom window or process logic. Do NOT trigger for: Flink SQL or Table API queries (flink-sql-table-api); state backend or checkpoint tuning (flink-state-checkpointing); implementing or configuring connectors (flink-connectors-formats); Confluent Cloud specifics (confluent-cloud-flink-sql)."
metadata:
  author: "yusuf"
  version: "1.0.0"
  last_updated: "2026-11-17"
compatibility: "Grounded in the Apache Flink 2.5-SNAPSHOT source tree; classic API classes live in flink-runtime and flink-streaming-java, v2 API in flink-datastream-api/flink-datastream."
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

# Flink DataStream Programming

Build streaming jobs with the DataStream API. Paths below reflect **this** source tree — in the
2.5-SNAPSHOT layout the classic API classes moved into `flink-runtime`, not `flink-streaming-java`.

## Scope boundary (check first)

- Raw streaming logic (transforms, event time, custom windows/process functions) — this skill.
- Set-based queries, aggregations written as SQL — `flink-sql-table-api`.
- Choosing/configuring state backends, checkpoint intervals — `flink-state-checkpointing`
  (this skill only covers *using* state in functions).
- Writing/configuring sources, sinks, formats — `flink-connectors-formats`.

## Pick the API

| | Classic DataStream API | DataStream v2 API |
|---|---|---|
| Package | `org.apache.flink.streaming.api.*` | `org.apache.flink.datastream.api.*` |
| Source of truth | `flink-runtime/src/main/java/org/apache/flink/streaming/api/` (e.g. [`DataStream.java`](../../flink-runtime/src/main/java/org/apache/flink/streaming/api/datastream/DataStream.java), [`KeyedStream.java`](../../flink-runtime/src/main/java/org/apache/flink/streaming/api/datastream/KeyedStream.java)) | `flink-datastream-api/src/main/java/org/apache/flink/datastream/api/` |
| Environment | `StreamExecutionEnvironment` (`flink-runtime/.../streaming/api/environment/`) | `ExecutionEnvironment` (`flink-datastream-api/.../api/ExecutionEnvironment.java`) |
| Process functions | `ProcessFunction`, `KeyedProcessFunction` (`flink-runtime/.../streaming/api/functions/`) | `ProcessFunction`, `OneInputStreamProcessFunction`, ... (`flink-datastream-api/.../api/function/`) |
| Windows | `WindowedStream` + `windowing/` assigners/triggers/evictors (`flink-streaming-java`) | `WindowStrategy` extensions (`flink-datastream-api/.../api/extension/window/`) |
| Docs | [`docs/content/docs/dev/datastream/`](../../docs/content/docs/dev/datastream/overview.md) | [`docs/content/docs/dev/datastream-v2/`](../../docs/content/docs/dev/datastream-v2/_index.md) |

**Default to the classic API** for new jobs (it is what docs, examples, and most connectors target);
choose v2 when the user explicitly wants its unified `ProcessFunction` model or partitioned-context
state access. Do not mix the two in one job.

## Workflow

1. **Set up the environment**: `StreamExecutionEnvironment.getExecutionEnvironment()`, set
   parallelism and checkpointing interval explicitly (don't rely on defaults for production).
2. **Ingest**: sources via the Source API — see `flink-connectors-formats` for choosing one.
3. **Transform**: see [references/transformations.md](references/transformations.md).
4. **Declare time semantics** before any time-based operator: watermark strategy
   ([references/event-time-windows.md](references/event-time-windows.md)).
5. **Attach stateful logic** in `KeyedProcessFunction` only *after* `keyBy` — keyed state and
   timers require a key.
6. **Emit** to a sink (`flink-connectors-formats`), then `env.execute("job-name")`.
7. **Set operator UIDs** (`uid("...")`) on every stateful operator before running against any
   cluster you intend to upgrade or restore — restore matches state by UID.

## Semantics checklist

- **Keying**: `keyBy` must be deterministic on the object used for state; prefer stable IDs
  (String/Long) over POJO `hashCode()` for stateful keys.
- **Timers**: event-time and processing-time timers only fire on keyed streams; register with
  `ctx.timerService()`, handle in `onTimer`.
- **Late data**: define `withIdleness` for idle sources, set
  `withTimestampAssigner(...).withBoundedOutOfOrderness(...)`, and route late events explicitly
  via side output — otherwise they are silently dropped.
- **Side outputs**: declare an `OutputTag<T>` (typed, serializable) and use
  `ctx.output(tag, value)`; read with `getSideOutput(tag)`.
- **Async I/O**: use `AsyncDataStream.unorderedWait` with `AsyncFunction`; configure retry
  (`AsyncRetryStrategy` in `flink-streaming-java/.../functions/`) instead of catching exceptions
  inside the function; beware unbounded buffering — set capacity.
- **Chaining and fairness**: consecutive one-to-one operators chain into a task; `disableChaining`
  or `startNewChain` only for profiling, not as a default fix for backpressure.
- **Type extraction**: lambdas need type hints (`returns(...)`) — POJO/TypeInformation issues are
  the #1 cause of `The return type of function ... could not be determined` errors
  ([java_lambdas.md](../../docs/content/docs/dev/datastream/java_lambdas.md)).

## Verification loop

1. `env.execute()` in a local run (MiniCluster) with `flink-connector-datagen` as source.
2. Job-level tests: `flink-tests/` patterns and `docs/content/docs/dev/datastream/testing.md` —
   use `MiniClusterWithClientResource` / test harnesses rather than asserting on live clusters.
3. For operator logic, prefer the low-level harness tests (`OneInputStreamOperatorTestHarness`)
   in `flink-streaming-java` — deterministic timer control, no wall-clock sleeps.

## References (read only what the task needs)

- [references/transformations.md](references/transformations.md) — operator matrix, keyBy/connect
  rules, chaining, UID and side-output patterns
- [references/event-time-windows.md](references/event-time-windows.md) — watermark strategies,
  window types, triggers/evictors, late-data handling
- [references/testing.md](references/testing.md) — MiniCluster, operator harnesses, deterministic
  timer testing, common failure signatures

## Guardrails

- Before running `cancel`, `stop-with-savepoint`, or anything that alters a running cluster,
  present the exact commands and wait for confirmation.
- Never use wall-clock `Thread.sleep` in tests; drive time with harness timers or a manual clock.
