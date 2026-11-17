---
name: flink-connectors-formats
description: "Work with Apache Flink connectors and formats: the Source API (FLIP-27) and Sink API (sink2), FileSource/FileSink, async sinks and their rate-limiting strategies, DeliveryGuarantee semantics (none, at-least-once, exactly-once with two-phase commit), bundled formats (JSON, Avro, Parquet, ORC, CSV, Protobuf) and their SQL connector options. Use when the user configures a source or sink in SQL DDL or DataStream, asks about exactly-once sink behavior, wants to add a format, or plans to build a custom connector. Do NOT trigger for: general SQL query authoring (flink-sql-table-api); writing stream processing logic (flink-datastream-programming); Confluent CDC-to-lake pipelines (confluent-cloud-cdc-tableflow); state backend/checkpoint internals (flink-state-checkpointing)."
metadata:
  author: "yusuf"
  version: "1.0.0"
  last_updated: "2026-11-17"
compatibility: "Grounded in the Apache Flink 2.5-SNAPSHOT source tree; the Kafka connector and most connectors live in external apache/flink-connector-* repos."
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

# Flink Connectors & Formats

Configure or build sources, sinks, and formats — and reason about delivery guarantees.

## Scope boundary (check first)

- Choosing/writing query logic around a table — `flink-sql-table-api`.
- Checkpoint mechanics and state backends — `flink-state-checkpointing` (this skill covers only
  the sink-side commit protocol).
- Kafka Streams topologies — `kafka-streams-programming`.

**Where connectors live:** in-tree we have only `flink-connectors/`
(`flink-connector-base`, `flink-connector-files`, `flink-connector-datagen`,
`flink-hadoop-compatibility`). Kafka, JDBC, Elasticsearch, Pulsar, etc. are in **external**
`apache/flink-connector-*` repositories — see
[`docs/content/docs/connectors/other-projects/`](../../docs/content/docs/connectors/other-projects/overview.md)
and [`docs/content/docs/connectors/` _index](../../docs/content/docs/connectors/_index.md).
Never implement Kafka connectivity from scratch; depend on the external connector.

## The two API layers

### 1. DataStream level

| Piece | Interface | Location |
|---|---|---|
| Source (FLIP-27) | `Source`, `SourceReader`, `SplitEnumerator`, `SourceSplit` | `flink-core/src/main/java/org/apache/flink/api/connector/source/` |
| Source base impls | `SourceReaderBase`, `SingleThreadMultiplexSourceReaderBase`, `SplitFetcher` | `flink-connectors/flink-connector-base/.../source/reader/` |
| Hybrid source (chain sources) | `HybridSource` | `flink-connectors/flink-connector-base/.../source/hybrid/` |
| Sink | `Sink`, `SinkWriter` | `flink-core/src/main/java/org/apache/flink/api/connector/sink2/` |
| Commit protocol | `CommittingSinkWriter`, `SupportsCommitter`, `Committer` | same package |
| Writer state/restore | `SupportsWriterState`, `StatefulSinkWriter` | same package |
| Async sink base | `AsyncSinkBase`, `AsyncSinkWriter`, rate-limit strategies | `flink-connectors/flink-connector-base/.../sink/` |
| File source/sink | `FileSource`, `FileSink` (+ compaction) | `flink-connectors/flink-connector-files/` |
| DataStream v2 variants | `org.apache.flink.api.connector.dsv2.{Source,Sink}` | `flink-core-api/.../api/connector/dsv2/` |

### 2. Table/SQL level

Dynamic tables wrap the DataStream connectors: factories in
`flink-connectors/flink-connector-base/.../table/` (e.g. `AsyncDynamicTableSinkFactory`)
plus each connector's `*DynamicTableFactory`. Configure via DDL:

```sql
CREATE TABLE t (...) WITH (
  'connector' = 'filesystem',
  'path' = 's3://bucket/path',
  'format' = 'json'
);
```

Docs: [`docs/content/docs/dev/table/sourcesSinks.md`](../../docs/content/docs/dev/table/sourcesSinks.md),
[`connectors/table/`](../../docs/content/docs/connectors/table/_index.md).

## Delivery guarantees

`DeliveryGuarantee` enum: `flink-connectors/flink-connector-base/.../DeliveryGuarantee.java` —
`NONE`, `AT_LEAST_ONCE`, `EXACTLY_ONCE`.

End-to-end exactly-once requires **all three**:

1. Checkpointing enabled (`flink-state-checkpointing`).
2. Source that can replay from a committed offset/split state (FLIP-27 sources do).
3. Sink with a checkpoint-aligned commit: writer buffers, `flush(false)` before snapshot,
   commits via `SupportsCommitter`/`CommittingSinkWriter` after the checkpoint completes;
   abort on failure.

- `AT_LEAST_ONCE` sinks commit without barrier alignment → duplicates on failure.
- Async sinks (`AsyncSinkBase`) batch requests for throughput — guarantee still comes from the
  commit protocol, not the batching; batching only affects latency/dupe window size.
- For files, `FileSink`'s two-phase commit + rolling/compaction
  (`flink-connectors/flink-connector-files/.../sink/compactor/`) yields exactly-once object-store
  output; for Kafka-like sinks, check the external connector's guarantee table.

## Formats

In-tree format modules under `flink-formats/`:

| Format | Modules | Notes |
|---|---|---|
| JSON | `flink-json`, `flink-sql-json` | Schema evolution via Schema Registry-compatible options; default choice for new pipelines |
| Avro | `flink-avro`, `flink-avro-confluent-registry`, `flink-sql-avro*` | Confluent Schema Registry serdes available in-tree |
| Parquet / ORC | `flink-parquet`, `flink-orc`, `flink-sql-parquet`, `flink-sql-orc` | Columnar, for file lakehouses |
| CSV | `flink-csv`, `flink-sql-csv` | Simplest; weak typing |
| Protobuf | `flink-protobuf`, `flink-sql-protobuf` | Descriptor-driven |
| Compression/sequence | `flink-compress`, `flink-sequence-file`, `flink-hadoop-bulk` | Hadoop-era helpers |

SQL usage: `'format' = 'json'` + format options; docs under
[`docs/content/docs/connectors/table/formats/`](../../docs/content/docs/connectors/table/formats/_index.md)
and DataStream formats under
[`docs/content/docs/connectors/datastream/formats/`](../../docs/content/docs/connectors/datastream/formats/_index.md).

## Workflow

1. Identify level: SQL DDL config vs DataStream API construction.
2. Pick connector (in-tree vs external repo) and format; verify its documented guarantee.
3. For SQL: write DDL, `EXPLAIN` to confirm the right source/sink operators and options resolve.
4. For DataStream: build via `Source`/`Sink` factories (`fromSource`, `SinkV2` builders), set
   timestamps/watermarks on the source (see flink-datastream-programming).
5. Verify guarantees under failure: kill a TaskManager in staging and count duplicates/losses —
   config tables are claims, tests are proof.

## References

- [references/sink-guarantees.md](references/sink-guarantees.md) — commit protocol, async sink
  tuning, guarantee matrix, exactly-once verification recipe
- [references/sources.md](references/sources.md) — building a FLIP-27 source: splits, enumerator,
  reader loop, boundedness, dynamic parallelism
- [references/formats-and-ddl.md](references/formats-and-ddl.md) — format options cheat sheet,
  DDL patterns, schema evolution, external connector lookup

## Guardrails

- Present a plan (tables/topics/files to create, commands in order) before any operation that
  creates or overwrites external resources; wait for confirmation.
- Keep credentials out of DDL samples — use environment/secret references, never literal keys.
- Sample data must be synthetic (`example.com`, `555-01xx` numbers).
