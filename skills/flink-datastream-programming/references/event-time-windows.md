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

# Event time and windows

Docs: `docs/content/docs/dev/datastream/event-time/` (`built_in.md`, `generating_watermarks.md`),
`docs/content/docs/dev/datastream/operators/windows.md`.

## Watermark strategies

```java
WatermarkStrategy<Order> strategy = WatermarkStrategy
    .<Order>forBoundedOutOfOrderness(Duration.ofSeconds(5))
    .withTimestampAssigner((order, ts) -> order.eventTime().toEpochMilli())
    .withIdleness(Duration.ofMinutes(1));   // required for interval joins on sparse inputs

DataStream<Order> orders = env.fromSource(source, strategy, "orders");
```

Rules:

- `forMonotonousTimestamps()` when timestamps never go backwards — cheapest.
- `forBoundedOutOfOrderness(d)` must **cover** observed lateness; events later than `d` are
  dropped from windows (or routed to a side output if you handle them).
- `withIdleness` prevents watermark stalls when one parallel input goes quiet — without it,
  joins/merges wait forever.
- Built-in extractors: `AscendingTimestampExtractor`,
  `BoundedOutOfOrdernessTimestampExtractor` in `flink-streaming-java/.../functions/`.

Late-data policy — decide explicitly:

1. Drop (default) — fine for metrics.
2. Side output + recompute — collect `OutputTag` and patch results.
3. Allow lateness — windows stay open for `allowedLateness`, output updates as `+U`.

## Window types

| Window | API | Use |
|---|---|---|
| Tumbling | `window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))` | Fixed buckets |
| Sliding | `window(SlidingEventTimeWindows.of(size, slide))` | Overlapping buckets |
| Session | `window(EventTimeSessionWindows.withGap(Duration.ofMinutes(5)))` | Activity bursts |
| Global + trigger | `window(GlobalWindows.create()).trigger(...)` | Custom logic |
| Count windows | `countWindow(size)` / `countWindow(size, slide)` | Processing-time count; no time semantics |

Window internals (assigners/triggers/evictors): `flink-streaming-java/src/main/java/org/apache/flink/streaming/api/windowing/`
and `flink-streaming-java/.../runtime/operators/windowing/`.

## Triggers and evictors

- Default event-time trigger fires at watermark pass — deterministic in tests.
- `ContinuousEventTimeTrigger` for early results; combine with side outputs for speculative emits.
- `Evictors` (e.g. `TimeEvictor`) discard elements before/after firing — use sparingly; they
  interact subtly with checkpointed window state.

## ProcessFunction time handling

- `KeyedProcessFunction.onTimer` — register via `ctx.timerService().registerEventTimeTimer(ts)`
  or `registerProcessingTimeTimer(ts)`.
- Event-time timers fire when the watermark passes `ts`; processing-time timers use wall clock —
  in tests, drive them with the operator harness (`advanceProcessingTime`), never `sleep`.

## Common failure signatures

| Symptom | Cause |
|---|---|
| Windows never fire in streaming | No watermark (missing strategy) or source idling without `withIdleness` |
| Results missing events | `boundedOutOfOrderness` too small; events late beyond allowance |
| Watermark stuck at MIN_VALUE | One parallel subtask has no events and no idleness configured |
| Wrong timestamps after join | Timestamp assigned before `connect`, lost through a non-timestamped operator |
