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

# DataStream transformations

Classic API classes: `flink-runtime/src/main/java/org/apache/flink/streaming/api/datastream/`.
Function interfaces: `flink-streaming-java/src/main/java/org/apache/flink/streaming/api/functions/`
and `flink-core-api/.../api/common/functions/` (`MapFunction`, `FlatMapFunction`, `FilterFunction`,
`KeyedProcessFunction` lives with the streaming functions).

## Operator matrix

| Need | Operator | Notes |
|---|---|---|
| 1:1 elementwise | `map` | Pure; no state |
| 1:N | `flatMap` | Collector-based |
| Filter | `filter` | |
| Route to key | `keyBy(keySelector)` | Enables keyed state + timers |
| Union of same-type streams | `union` | No keying |
| Two different streams | `connect` + `CoProcessFunction`/`CoMapFunction` | Shared operator, not keyed; keyed variant: `KeyedCoProcessFunction` after `keyBy` on a shared key |
| Group both sides | `coGroup` / `join` / `iterate` | `flink-runtime/.../datastream/CoGroupedStreams.java`, `JoinedStreams.java` |
| Split by predicate | `process` with side outputs | Prefer over two `filter`s when both outputs needed |
| Async enrichment | `AsyncDataStream.unorderedWait/orderedWait` | `flink-streaming-java/.../functions/AsyncFunction.java` |
| Windowed aggregation | `window(...).aggregate(...)` | See event-time-windows reference |
| Periodic snapshot | `addSink` / sink v2 | See flink-connectors-formats |

## keyBy rules

- `keyBy` accepts a `KeySelector`; the key's `hashCode`/`equals` define the partitioning.
- Use stable business IDs (String/Long). POJO keys with generated `hashCode` break across
  versions and languages.
- After `keyBy` you get a `KeyedStream` — only there do `valueState`, `mapState`, and timers exist.
- Re-keying (`keyBy` twice) creates a network shuffle; each keyBy is a repartition boundary.

## UIDs, chaining, and parallelism

```java
source
  .uid("source-orders")            // stable UID — required for savepoint restore
  .map(new Enrich())
  .uid("enrich-orders")
  .keyBy(o -> o.customerId)
  .process(new RunningTotal())
  .uid("running-total")
  .addSink(sink)
  .uid("sink-orders");
```

- UIDs default to a hash of the operator's chain when not set — changing downstream code then
  orphans state. Set `uid()` on **every stateful** operator; set `setParallelism` explicitly.
- `startNewChain()` / `disableChaining()` are diagnostics, not fixes.
- `setBufferTimeout` rarely helps modern backpressure; diagnose with the backpressure monitor
  instead (see flink-job-ops-tuning).

## Stateful function skeleton

```java
public class RunningTotal extends KeyedProcessFunction<String, Order, OrderTotal> {
    private ValueState<BigDecimal> total;

    @Override
    public void open(FunctionConfiguration ctx) {
        total = getRuntimeContext().getState(
            StateDeclarations.value("running-total", Types.BIG_DECIMAL));
    }

    @Override
    public void processElement(Order o, Context ctx, Collector<OrderTotal> out) throws Exception {
        BigDecimal cur = total.value();
        BigDecimal next = cur == null ? o.amount() : cur.add(o.amount());
        total.update(next);
        ctx.timerService().registerProcessingTimeTimer(
            ctx.timerService().currentProcessingTime() + 60_000);
        out.collect(new OrderTotal(o.customerId(), next));
    }

    @Override
    public void onTimer(long ts, OnTimerContext ctx, Collector<OrderTotal> out) throws Exception {
        // flush/expire logic — timers fire only on keyed streams
    }
}
```

State descriptors: `flink-core-api/src/main/java/org/apache/flink/api/common/state/`
(`StateDeclarations`, `ValueState`, `ListState`, `MapState`, `BroadcastState`). Serialization of
state values goes through Flink's type system — no Java serialization for new state types.

## v2 API differences (when chosen)

- Single `ProcessFunction` model with `PartitionedContext`/`NonPartitionedContext` instead of the
  map/flatMap/process split (`flink-datastream-api/.../api/function/`).
- `BuiltinFuncs` for common transforms (`flink-datastream-api/.../api/builtin/BuiltinFuncs.java`).
- Windowing via `WindowStrategy` extensions rather than `WindowedStream`.
- Docs: `docs/content/docs/dev/datastream-v2/{overview,building_blocks,context_and_state_processing}.md`.
