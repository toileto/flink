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

# Table API (fluent equivalent of SQL)

When the user is writing Java (not raw SQL), use the Table API in
`flink-table/flink-table-api-java/`. Entry point: `TableEnvironment` or `StreamTableEnvironment`
(for DataStream interop).

## Core shapes

```java
TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

env.executeSql("CREATE TABLE events (...) WITH ('connector' = 'datagen', ...)");

Table events = env.from("events");

// filter + project
Table filtered = events
    .filter($("page").isNotNull())
    .select($("user_id"), $("event_time"), $("page"));

// group-by aggregate
Table totals = events
    .groupBy($("user_id"))
    .select($("user_id"), $("amount").sum().as("total"),
                           $("amount").avg().as("avg_amount"));
```

`$` is `org.apache.flink.table.api.Expressions.$`; the full expression DSL lives in
[`Expressions.java`](../../../flink-table/flink-table-api-java/src/main/java/org/apache/flink/table/api/Expressions.java)
and `BaseExpressions` (`org.apache.flink.table.api.internal.BaseExpressions`).

## Window builders

Table API window classes are in `flink-table/flink-table-api-java/.../table/api/`:
`Tumble`, `Slide`, `Session`, `GroupWindowedTable`, `WindowGroupedTable`.

```java
Table windowed = events
    .window(Tumble.over(lit(10).seconds()).on($("event_time")).as("w"))
    .groupBy($("w"), $("page"))
    .select($("page"), $("event_time").count().as("views"));
```

Prefer the SQL window TVFs (via `env.executeSql`) for new code — the TVF form is what the planner
normalizes to; the Table API window builders translate to the same physical plan.

## Over windows

```java
Table ranked = events
    .window(Over.partitionBy($("user_id")).orderBy($("event_time"))
              .preceding(lit(10).minutes()).as("w"))
    .select($("user_id"), $("event_time"),
            $("amount").sum().over("w").as("running_total"));
```

## StatementSet — multiple inserts in one job

```java
StatementSet set = env.createStatementSet();
set.addInsert("sink_a", tableA);
set.addInsert("sink_b", tableB);
set.execute();   // single job, single checkpoint barrier flow
```

See `flink-table/flink-table-api-java/.../StatementSet.java` and
`docs/content/docs/dev/table/table_environment.md`.

## When to reach for SQL instead

- Window TVFs, `MATCH_RECOGNIZE`, `TOP-N` via `ROW_NUMBER`, and lookup-join hints are less
  verbose in SQL; mixing is fine — `table.executeSql(...)` and fluent calls operate on the same
  catalog objects.
- Anything needing a literal plan artifact (compiled plan) is SQL-first.

## Interop with DataStream

`StreamTableEnvironment.fromDataStream(...)` / `.toDataStream(...)` — see
`flink-table/flink-table-api-java-bridge/` and `docs/content/docs/dev/table/data_stream_api.md`.
Column naming and time attributes are the usual source of surprises here: rowtime/proctime
attributes must be declared at conversion time.
