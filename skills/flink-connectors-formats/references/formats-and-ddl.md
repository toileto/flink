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

# Formats and connector DDL

Docs: `docs/content/docs/connectors/table/formats/`, `docs/content/docs/connectors/table/`,
`docs/content/docs/dev/table/sourcesSinks.md`.

## SQL DDL patterns

```sql
-- file + JSON
CREATE TABLE events (
  user_id STRING, page STRING, event_time TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'filesystem',
  'path' = '/data/events',
  'format' = 'json'
);

-- append-only print sink (debugging)
CREATE TABLE debug_sink (...) WITH ('connector' = 'print');

-- Avro with Confluent Schema Registry (in-tree module flink-sql-avro-confluent-registry)
CREATE TABLE sr_table (...) WITH (
  'connector' = 'kafka',            -- connector itself from the external kafka connector jar
  'format' = 'avro-confluent',
  'avro-confluent.url' = 'http://schema-registry:8081'
);
```

Format option prefixes follow `'<format>.<option>'`. Unknown options fail fast at `EXPLAIN` —
run it before submitting.

## Format cheat sheet

| Need | Format module(s) | Notes |
|---|---|---|
| General JSON | `flink-json`, `flink-sql-json` | `'json.fail-on-missing-field'`, `'json.ignore-parse-errors'` are the usual knobs |
| Schema Registry serdes | `flink-avro-confluent-registry`, `flink-sql-avro-confluent-registry` | GUID/schema-id in payload; compatible with Confluent ecosystem |
| Columnar files | `flink-parquet`, `flink-orc` (+ `flink-sql-*`) | Predicate/projection pushdown into file readers |
| CSV | `flink-csv`, `flink-sql-csv` | Set `'csv.allow-comments'` etc.; keep schemas explicit |
| Protobuf | `flink-protobuf`, `flink-sql-protobuf` | Descriptor file required |
| Debezium/canal/ogg changelog formats | under `docs/content/docs/connectors/table/formats/` | Decode CDC into Flink's changelog — feeds upsert/retract semantics (flink-sql-table-api) |

## Schema evolution rules

- File formats: schema read is per-file metadata; changing the table schema changes how old files
  are interpreted — validate with a read of pre-change files.
- JSON: adding fields is safe with defaults; renaming/removing requires the consumer to tolerate
  missing keys (`'json.ignore-parse-errors'` masks real errors — don't use it as a fix).
- Avro/Protobuf with Schema Registry: evolution follows registry compatibility rules
  (BACKWARD/FORWARD/BIDIRECTIONAL) — check the registered subject before deploying.

## External connector lookup

Before writing any connector code, check whether it already exists:

1. `docs/content/docs/connectors/other-projects/overview.md` (externalized connectors list)
2. `docs/content/docs/connectors/table/downloads.md` (version-matched connector jars)
3. github.com/apache/flink-connector-{kafka,jdbc,elasticsearch,...}

Only then consider implementing a new one — start from `flink-connector-base` (see
[sources.md](sources.md) and the sink interfaces in [sink-guarantees.md](sink-guarantees.md)).

## Testing DDL

- `EXPLAIN` to validate options/factories without running.
- `flink-connector-datagen` for source-side DDL tests; `filesystem` + `print` for end-to-end
  smoke tests with no external dependencies.
