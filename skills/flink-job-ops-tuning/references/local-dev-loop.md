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

# Local development loop

## Prerequisites

- Java 11/17/21 (17 default), Maven via `./mvnw` (see repo `AGENTS.md`).
- Fast build: `./mvnw clean install -DskipTests -Dfast -Pskip-webui-build -T1C`.

## SQL iteration

1. Write DDL + query in a session file or directly in `./bin/sql-client.sh`
   (`flink-table/flink-sql-client/`).
2. `EXPLAIN` the statement — cheapest full validation.
3. Run with `'connector' = 'datagen'` source and `'connector' = 'print'` sink for zero-dependency
   smoke tests.
4. Promote to `filesystem`/real connectors only after the plan looks right.

Docs: `docs/content/docs/dev/table/sqlClient.md`, `docs/content/docs/dev/table/tuning.md`.

## Jar iteration

```bash
./mvnw clean package -DskipTests -pl <your-module> -am
./bin/flink run -c com.example.MyJob target/<your-module>-*.jar
./bin/flink list                    # job status
./bin/flink cancel <jobId>          # stop (confirm first on shared clusters)
```

CLI reference: `docs/content/docs/deployment/cli.md`; client code:
`flink-clients/src/main/java/org/apache/flink/client/cli/`.

## Embedded tests (recommended default)

```java
// in src/test — runs in MiniCluster, no external cluster needed
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(1);
env.addSource(new TestSource(...)).print();
env.execute("test-job");
```

Better: assert outputs with `executeAndCollect()` or operator harnesses — see
`flink-datastream-programming/references/testing.md`.

## Common startup errors

| Error | Fix |
|---|---|
| `Could not find or load main class` | Wrong `-c` main class or shaded jar missing `Main-Class`/user classes |
| `No entry table ... found` | sql-client session file not loaded; check `-e`/`--environment` flag |
| `ClassNotFoundException: org.apache.flink.table...` | Missing `flink-table-planner-loader` on classpath (use dist jars, not partial classpath) |
| `InsufficientTaskManager memory` | Slot count × TM memory too small — see deployment/memory docs |
| Job starts then immediately `FAILED` | Read the **first** TaskManager exception; later ones are cascades |
| Web UI port conflict | Change `rest.port` in `conf/flink-conf.yaml` |

## Version consistency

The dist, connectors, and table planner jars must all be the same Flink version — mixing a
connector built for an older minor version causes `NoSuchMethodError` at submit time, not compile
time. Check `docs/content/docs/connectors/table/downloads.md` for the matched matrix.
