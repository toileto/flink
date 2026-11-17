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

# Savepoints and state migration

Docs: `docs/content/docs/ops/state/savepoints.md`,
`docs/content/docs/ops/state/checkpoints_vs_savepoints.md`,
`docs/content/docs/dev/datastream/fault-tolerance/state_migration.md`,
`docs/content/docs/ops/upgrading.md`.

## Checkpoints vs savepoints

| | Checkpoint | Savepoint |
|---|---|---|
| Purpose | Failure recovery | Planned migration/upgrade |
| Trigger | JobManager periodic | User (`flink savepoint ...` or REST) |
| Ownership | Flink (retention configured) | User (keep until done) |
| Format/portability | May be job-internal | Stable, operator-UID-addressed |
| After cancel | Only if externalized retention enabled | Always yours |

Savepoint restore settings live in
`flink-runtime/src/main/java/org/apache/flink/runtime/jobgraph/SavepointRestoreSettings.java`.

## Safe upgrade procedure

1. Take a savepoint while the old jar is running:
   `bin/flink savepoint -t <checkpoint-dir-or-target> <jobId> <savepoint-dir>`
   (or `--stop-with-savepoint` for a clean stop).
2. Build and stage the new jar; run the restore in staging first:
   `bin/flink run -s <savepoint-path> -d new-job.jar`
3. Verify: state restored (operator list shows state sizes), output correct, no
   `No state found for operator ...` warnings.
4. Only then cut over production.

## State schema change rules

- **Compatible (no action)**: adding a field with a default, if the serializer snapshot
  implements `resolveSchemaCompatibility` for the old version.
- **Migratable**: serializer snapshots are versioned — implement `getCurrentVersion()` and read
  old versions in `readSnapshot`. Test by restoring a savepoint written by the previous version.
- **Incompatible**: new state name/UID, or type change with no migration path → plan a
  re-keying/replay migration (reprocess from the source with retained offsets) instead of
  pretending a restore will work.
- **Never** rely on Java serialization for state; Kryo-only types have no evolution path.

## UID discipline

- `uid("stable-name")` on every stateful operator before first production run.
- Changing a UID orphans its state: restore logs `No state found for operator ...` and the
  operator starts empty. If you must rename, do it via the state migration path (map old UID →
  new) or accept full reprocessing.

## Guardrails

- Always present exact savepoint/cancel commands and wait for confirmation.
- Keep savepoints until the new version has run correctly for at least one full checkpoint cycle
  plus a business-meaningful validation window; never delete on success of job *start* alone.
