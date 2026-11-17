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

# Flink Agent Skills

Agent Skills (see [agentskills.io/specification](https://agentskills.io/specification)) grounded in
this Apache Flink source tree (2.5-SNAPSHOT). Each skill is a `skills/<name>/SKILL.md` with lazy
`references/` files and `evals/evals.json`.

| Skill | Scope |
|---|---|
| [flink-sql-table-api](flink-sql-table-api/SKILL.md) | Write/debug Flink SQL and Table API queries (joins, windows, changelog semantics) |
| [flink-datastream-programming](flink-datastream-programming/SKILL.md) | DataStream API programs: transforms, event time, windows, process functions |
| [flink-state-checkpointing](flink-state-checkpointing/SKILL.md) | State usage, state backends, checkpoints, savepoints, state migration |
| [flink-connectors-formats](flink-connectors-formats/SKILL.md) | Source/Sink APIs, delivery guarantees, bundled formats, connector DDL |
| [flink-job-ops-tuning](flink-job-ops-tuning/SKILL.md) | Run, debug, tune, and monitor Flink jobs |

Scope boundaries: these skills cover **Apache Flink OSS only**. Confluent Cloud Flink SQL lives in
the `confluent-cloud-flink-sql` skill, UDF authoring/deployment in `flink-udf`, and CDC-to-lake
pipelines in `confluent-cloud-cdc-tableflow`.
