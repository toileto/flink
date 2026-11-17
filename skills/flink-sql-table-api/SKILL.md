---
name: flink-sql-table-api
description: "Write, debug, and optimize Apache Flink SQL and Table API queries for OSS Flink 2.x: DDL, DML, joins (interval, temporal, lookup), window aggregations, deduplication, TOP-N, catalogs, data types, changelog/upsert semantics, and verification with EXPLAIN or compiled plans. Use when the user asks to write or fix a Flink SQL statement or Table API call, reason about retract streams, or run queries through sql-client or the SQL Gateway and the context establishes Apache Flink OSS (this source repo, an OSS cluster, or OSS docs); if the runtime is unknown, ask first. Do NOT trigger for: Confluent Cloud Flink SQL or the `confluent flink` CLI (use confluent-cloud-flink-sql); authoring Java UDF/UDTF/PTF functions (use flink-udf); CDC pipelines into Iceberg or Delta (use confluent-cloud-cdc-tableflow); DataStream API programs (use flink-datastream-programming)."
metadata:
  author: "yusuf"
  version: "1.0.0"
  last_updated: "2026-11-17"
compatibility: "Grounded in the Apache Flink 2.5-SNAPSHOT source tree; verify behavior against the docs/ pages cited below."
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

# Flink SQL & Table API (OSS)

Write and verify Flink SQL and Table API queries against **Apache Flink OSS**. This skill is
grounded in the checked-out source tree; prefer paths here over memorized APIs from older Flink
versions.

## Scope boundary (check first)

- Runtime is **Apache Flink OSS** (local, self-managed, or a Flink cluster). If the target is a
  Confluent Cloud compute pool or the `confluent flink` CLI, stop and use `confluent-cloud-flink-sql`
  — CC Flink rejects or silently mishandles many valid OSS constructs.
- Custom Java functions (scalar UDF, UDTF, PTF) belong to the `flink-udf` skill; this skill covers
  built-in functions and query structure only.
- CDC ingestion pipelines into a lakehouse belong to `confluent-cloud-cdc-tableflow`.

## Choose the execution entry point

| Situation | Entry point | Grounding |
|---|---|---|
| Interactive exploration, one-off job | `bin/sql-client.sh` | `flink-table/flink-sql-client/`; [sql-client docs](../../docs/content/docs/sql/interfaces/sql-client.md) |
| Embedded in a Java application | `TableEnvironment.create(EnvironmentSettings...)` | [`TableEnvironment.java`](../../flink-table/flink-table-api-java/src/main/java/org/apache/flink/table/api/TableEnvironment.java); [table environment docs](../../docs/content/docs/dev/table/table_environment.md) |
| Remote / programmatic access | SQL Gateway + JDBC driver | `flink-table/flink-sql-gateway/`, `flink-table/flink-sql-jdbc-driver/`; [SQL Gateway docs](../../docs/content/docs/dev/table/sql-gateway) |
| DataStream <-> Table conversion | `StreamTableEnvironment` | `flink-table/flink-table-api-java-bridge/`; [DataStream integration](../../docs/content/docs/dev/table/data_stream_api.md) |

## Workflow

1. **Confirm the runtime** is OSS Flink (see scope boundary above).
2. **Model the schema first.** Pick types from [data types](../../docs/content/docs/sql/reference/data-types.md);
   be explicit about `TIMESTAMP(3)` vs `TIMESTAMP_LTZ(3)` — event-time attributes and temporal joins
   depend on it ([timezone docs](../../docs/content/docs/sql/timezone.md)).
3. **Write DDL, then DML.** Reference pages:
   [DDL](../../docs/content/docs/sql/reference/ddl), [queries](../../docs/content/docs/sql/reference/queries/overview.md),
   [built-in functions](../../docs/content/docs/sql/built-in-functions/_index.md).
4. **Verify before running** — see the verification loop below.
5. **Check changelog compatibility** of the sink: aggregations and dedup emit retract
   (`-D`) and update (`+U`) rows; an append-only sink will fail or corrupt data.
   See [changelog.md](../../docs/content/docs/sql/reference/queries/changelog.md).

## Verification loop (cheapest first)

1. Static check: `EXPLAIN <statement>;` in sql-client
   ([explain docs](../../docs/content/docs/sql/reference/utility/explain.md)) — catches type,
   unbound-object, and plan errors without executing.
2. Compiled plan for production statements: store a compiled plan and validate upgrades against it
   so a Flink version bump that changes the plan fails fast.
3. Runtime behavior: run an ITCase on the mini cluster — follow the test pattern in
   `flink-table/flink-table-planner/src/test/java/.../ITCase` classes using
   `flink-table/flink-table-test-utils`. Never re-test pure type-strategy cases in an ITCase if a
   planner unit test already covers them.

Full details: [references/verification.md](references/verification.md).

## Semantics checklist (the usual traps)

- **Event time**: windows, interval joins, and `MATCH_RECOGNIZE` need a rowtime attribute or
  watermark declaration; otherwise results are wrong or the query never becomes bounded.
- **Windows**: prefer window TVFs (`TUMBLE`, `HOP`, `SESSION`, `CUME_DIST`, `NTILE`) +
  `GROUP BY` — see [window-tvf.md](../../docs/content/docs/sql/reference/queries/window-tvf.md);
  legacy `TUMBLE(...)` group-window syntax is still accepted but the TVF form is the modern path.
- **TOP-N**: `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)` + `WHERE rn <= N`
  ([topn.md](../../docs/content/docs/sql/reference/queries/topn.md)); plain
  `ORDER BY ... LIMIT` on an updating stream is not equivalent.
- **Temporal joins**: require a versioned (upsert) dimension source; lookup joins use hints
  (`LookupJoinHintOptions` in
  [`flink-table-api-java` config](../../flink-table/flink-table-api-java/src/main/java/org/apache/flink/table/api/config/LookupJoinHintOptions.java)).
- **Interval joins** are only bounded when both sides have bounded time attributes
  ([joins.md](../../docs/content/docs/sql/reference/queries/joins.md)).
- **NULLs and casts**: a SQL NULL argument yields NULL for built-ins; cast behavior is defined by
  planner cast rules in `flink-table/flink-table-planner/.../functions/casting/`.
- **Dialect**: write standard Flink SQL (`SqlDialect.DEFAULT` in
  [`SqlDialect.java`](../../flink-table/flink-table-api-java/src/main/java/org/apache/flink/table/api/SqlDialect.java));
  do not modify `Parser.jj` (Calcite-generated).

## Source and doc grounding

- API surface: `flink-table/flink-table-api-java/` (`Table`, `TableEnvironment`, `Expressions`,
  `StatementSet`, `TableConfig`)
- Planning/optimization: `flink-table/flink-table-planner/` — plan rules, codegen, cast rules
- Runtime operators: `flink-table/flink-table-runtime/`
- Built-in function definitions: `flink-table/flink-table-common/.../BuiltInFunctionDefinitions.java`
- Docs root for this topic: [`docs/content/docs/sql/`](../../docs/content/docs/sql/_index.md) and
  [`docs/content/docs/dev/table/`](../../docs/content/docs/dev/table/_index.md)

## References (read only what the task needs)

- [references/query-patterns.md](references/query-patterns.md) — copy-ready patterns: window
  aggregation, dedup, TOP-N, temporal/lookup joins, incremental aggregation
- [references/table-api.md](references/table-api.md) — fluent Table API equivalents of SQL,
  `Expressions`, window builders, `StatementSet`
- [references/verification.md](references/verification.md) — EXPLAIN, compiled plans, ITCase
  test loop, common planner errors and their fixes

## Guardrails

- Present a plan before executing statements that change cluster state (`DROP`, `ALTER`,
  `TRUNCATE`, overwriting sinks) and wait for confirmation.
- Never commit generated plans or credentials into the repo; keep sample data synthetic.
