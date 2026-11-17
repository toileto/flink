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

# Testing DataStream jobs

Docs: `docs/content/docs/dev/datastream/testing.md`. Harnesses live in
`flink-streaming-java/src/test/java/org/apache/flink/streaming/util/`
(`OneInputStreamOperatorTestHarness`, `KeyedOneInputStreamOperatorTestHarness`,
`ProcessFunctionTestHarnesses`).

## Test pyramid (cheapest first)

1. **Pure function tests** — instantiate `MapFunction`/`KeyedProcessFunction` directly with
   mocked `RuntimeContext` where needed. No Flink runtime. Most logic lives here.
2. **Operator harness tests** — run the operator in a `TestHarness`:
   ```java
   try (KeyedOneInputStreamOperatorTestHarness<String, Event, String> harness =
            new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new RunningTotal()), keySelector, keyType)) {
       harness.open();
       harness.processElement(event1, 0L);
       harness.processWatermark(1000L);          // drives event-time timers deterministically
       harness.processElement(event2, 1000L);
       assertEquals(expected, harness.extractOutputValues());
   }
   ```
   Advance time with `processWatermark` / `advanceProcessingTime` — **never `Thread.sleep`**.
3. **Job-level integration test** — `MiniClusterWithClientResource` (see `flink-tests/`),
   typically with `flink-connector-datagen` as source and a collection/print sink.
   Use `executeAndCollect()` on DataStreams for result assertions.

## Determinism rules

- Fix the clock: harness time APIs, not wall clock.
- Fix parallelism (`setParallelism(1)`) for output-order assertions, or assert on sets when the
  order is legitimately nondeterministic.
- Use fixed epoch timestamps in test data; never `System.currentTimeMillis()` in fixtures.
- For watermark/idle-source behavior, feed explicit watermarks rather than real timers.

## Common failure signatures

| Test failure | Likely cause |
|---|---|
| `The return type of function '...' could not be determined` | Lambda type erasure — add `.returns(...)` or use a named class/POJO |
| `ClassNotFoundException` / serialization error in test | Non-serializable field captured by lambda — mark `transient`, inject in `open()` |
| Timers never fire in harness | Forgot `processWatermark` / registered processing-time timer but advanced event time |
| State empty after "restore" | Missing `uid()` on stateful operator, or harness not re-`open()`ed with snapshot |
| Test passes locally, fails in CI | Wall-clock dependency or parallelism-dependent ordering |
