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

# Verification loop for Flink SQL

Escalate from cheapest to most expensive. Never claim a query works on the basis of syntax alone.

## 1. EXPLAIN (no execution)

In sql-client or via `TableEnvironment.executeSql("EXPLAIN <stmt>")`:

```sql
EXPLAIN SELECT page, COUNT(*) FROM events GROUP BY page;
```

Catches: unbound objects, type mismatches, illegal window specifications, unsupported constructs.
Reference: `docs/content/docs/sql/reference/utility/explain.md`.

What to look for in the plan:

- `WatermarkGenerator` present for every time-based operator — a missing watermark means
  incorrect (processing-time-ish) results.
- `LookupJoin` vs `TemporalJoin` vs `IntervalJoin` — confirm the join type matches intent.
- Sink operator `ChangelogMode` matches the sink's capability (append vs upsert vs retraction).

## 2. Compiled plan (production statements)

For jobs that must survive upgrades, store a compiled plan and validate against it on change.
If the plan diff after a Flink bump touches more than cosmetic node IDs, investigate before
deploying. See `docs/content/docs/dev/table/` plan-related pages and `CompiledPlan` in
`flink-table/flink-table-api-java/.../CompiledPlan.java`.

## 3. Unit tests (planner level)

- Type strategies and validation messages: unit tests in `flink-table-common`
  (`InputTypeStrategiesTestBase`, `TypeStrategiesTestBase` subclasses).
- Helper parsers (JSON path, etc.): plain JUnit in the owning module.

Command shape:

```bash
./mvnw -pl flink-table/flink-table-common -Dtest=MyStrategyTest test
```

## 4. ITCase (mini cluster)

Runtime behavior — results, NULL inputs, runtime errors — goes in the category ITCase in
`flink-table/flink-table-planner`, built on `flink-table/flink-table-test-utils`.

```bash
./mvnw -pl flink-table/flink-table-planner -Dtest=MyFunctionsITCase test
```

Rules of thumb:

- One ITCase per function/operator category, not one per function.
- Use `testResult` for expected values and assert the exact runtime error message for failures.
- Call the Table API method (not `call("NAME")`) so the API surface is exercised too.

## Common planner/runtime errors

| Symptom | Likely cause | Fix |
|---|---|---|
| `Object 'x' not found` | Table not in catalog / wrong catalog in use | Check `USE CATALOG`/`USE`, catalog registration |
| `Column 'x' not found in table` | Column alias/qualification mismatch after join | Qualify columns with table aliases |
| Query never finishes in batch mode | Unbounded input or missing watermark for time ops | Declare watermark; use streaming mode for unbounded sources |
| Sink fails with changelog mode error | Append sink fed by retracting operator | Use upsert sink, or add a window/aggregation boundary |
| `Unsupported SQL kind` / parse error | Non-standard dialect syntax, or CC-only syntax on OSS | Standard Flink SQL only; CC constructs belong to `confluent-cloud-flink-sql` |
| Wrong results only for late data | Missing/incorrect watermark strategy | Fix `WATERMARK FOR ... AS ... - INTERVAL`, check `docs/content/docs/dev/datastream/event-time/` |

## Grounding

- Planner tests: `flink-table/flink-table-planner/src/test/`
- Test utilities: `flink-table/flink-table-test-utils/`
- Mini cluster: `flink-tests/` and `flink-test-utils-parent/`
