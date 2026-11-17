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

# Sink guarantees and tuning

Docs: `docs/content/docs/connectors/datastream/guarantees.md`,
`docs/content/docs/connectors/table/overview.md`.

## The commit protocol (sink2)

Interfaces: `flink-core/src/main/java/org/apache/flink/api/connector/sink2/`.

```
processElement  ->  writer buffer
      |
checkpoint barrier arrives
      |  flush(true)  -> pending writes drained to the external system
      |  snapshotState -> writer state (offsets/pending files) captured
      |  (checkpoint completes on JM)
      |  CommittingSinkWriter.commit() / SupportsCommitter.commit -> make writes visible
      v
failure before commit -> restore writer state, re-emit/abort (no visibility)
```

- `SupportsWriterState`/`StatefulSinkWriter` — restore path after failure.
- `CommittingSinkWriter`/`SupportsCommitter`/`Committer` — the "make visible" step that gives
  exactly-once visibility. A sink without it can only offer at-least-once or none.

## Guarantee matrix

| Guarantee | What you get on failure | Typical sink type |
|---|---|---|
| `NONE` | Loss or dupes possible | fire-and-forget, async without commit |
| `AT_LEAST_ONCE` | Dupes possible, no loss | commit-on-barrier without alignment |
| `EXACTLY_ONCE` | No loss, no dupes (single-writer visibility) | two-phase commit (FileSink, transactional external sinks) |

Caveats to state explicitly to users:

- Exactly-once **visibility** depends on the external system supporting transactions or
  idempotent writes; a non-transactional endpoint caps you at at-least-once even with Flink
  checkpoints.
- Source must be replayable; non-replayable sources (random generator, live socket) are at best
  effectively-once for their own lifetime.

## Async sink (`AsyncSinkBase`)

`flink-connectors/flink-connector-base/.../sink/`:

- `AsyncSinkWriter` buffers entries and dispatches batches; `Batch`/`BatchCreator` define a batch
  (count and size limits are connector-specific options).
- Rate limiting: `strategy/` — `AIMDScalingStrategy` (additive-increase/multiplicative-decrease,
  default for network endpoints), `NoOpScalingStrategy`. Tune batch sizes upward until the
  endpoint's throttling kicks in, then let AIMD settle.
- `AsyncSinkConnectorOptions` (table level) expose buffer/batch settings — change them through
  documented options, not by subclassing.
- Fatal vs retryable failures are classified by `FatalExceptionClassifier` /
  `AsyncSinkFatalExceptionClassifiers` — misclassified retryables cause silent data loss;
  when a connector misbehaves, check its classifier first.

## Exactly-once verification recipe (staging)

1. Produce N known records with synthetic ids.
2. Enable checkpointing; let at least one checkpoint complete.
3. `kill -9` a TaskManager (or `flink cancel`) mid-stream.
4. Restart from the latest savepoint/checkpoint.
5. Assert: received set == expected set exactly (no missing, no duplicates) for
   `EXACTLY_ONCE` sinks; count dupes for `AT_LEAST_ONCE` and document them.
6. Record the result in the job's README — guarantees are per-connector and per-version.
