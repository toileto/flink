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

# Debugging and monitoring

Docs: `docs/content/docs/ops/monitoring/`, `docs/content/docs/ops/debugging/`,
`docs/content/docs/ops/metrics.md`.

## Backpressure

`docs/content/docs/ops/monitoring/back_pressure.md`:

- The **saturated** (100% busy reading) operator is downstream; the *stalled* one is the victim.
  Fix the saturated operator — throwing more parallelism at the victim does nothing.
- Sources backpressured = the whole pipeline is capped; find the first non-backpressured operator
  downstream of the source and work forward.
- Async I/O with too-small capacity, or a slow sink, is the classic root cause.

## CPU hotspots

1. `docs/content/docs/ops/debugging/flame_graphs.md` — Web UI flame graphs for the suspect task.
2. `docs/content/docs/ops/debugging/profiler.md` + `application_profiling.md` — async-profiler
   integration for wall/CPU/allocation profiles.
3. Common culprits found this way: per-record serialization (Kryo/POJO), string concatenation in
   hot paths, custom `hashCode` on large keys, JSON parsing per record.

## Event-time debugging

`docs/content/docs/ops/debugging/debugging_event_time.md`:

- Check watermark metrics first — if the watermark isn't advancing, time-based operators are
  stuck regardless of input throughput.
- Idle sources need `withIdleness`; sparse partitions stall joins otherwise.
- Late-event side output counts tell you whether `boundedOutOfOrderness` is sized correctly.

## Failure triage order

1. **First** exception in TaskManager logs (later ones are cascades).
2. Job status history / restart count — is it crash-looping?
3. `docs/content/docs/ops/state/task_failure_recovery.md` for failover region behavior.
4. Classloader issues → `docs/content/docs/ops/debugging/debugging_classloading.md`
   (usually connector jar version mismatch).
5. Memory → `docs/content/docs/deployment/memory/_index.md`.

## Metrics that matter (alert on these)

| Metric area | What | Why |
|---|---|---|
| Checkpoints | duration, failure count, alignment time | Early warning of state/backpressure growth |
| Sources | pending/committed records (lag) | End-to-end freshness |
| Backpressure | per-operator ratio | Throughput ceiling |
| Watermarks | min watermark lag | Stalled time progress |
| Restarts | restart count | Crash loops |
| Heap/GC | GC pause frequency | JVM tuning need |

Metric groups and reporters: `docs/content/docs/ops/metrics.md`; event/reporter options under
`docs/content/docs/deployment/event_reporters.md` and `metric_reporters.md`.

## Log hygiene

- Use parameterized logging; attach job/checkpoint context via MDC
  (`docs/content/docs/ops/logging_context.md`) instead of concatenating.
- In production, INFO for lifecycle (job start, checkpoint failure transitions), WARN for
  recoverable anomalies, ERROR only with an actionable message.
