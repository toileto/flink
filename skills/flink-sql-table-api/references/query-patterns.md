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

# Flink SQL query patterns

Copy-ready patterns for OSS Flink SQL. All examples assume a source table `events`
(`user_id STRING, page STRING, event_time TIMESTAMP(3), WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND`)
unless stated otherwise.

## Window aggregation (window TVF form — preferred)

```sql
SELECT
  window_start,
  window_end,
  page,
  COUNT(*) AS views
FROM TABLE(TUMBLE(TABLE events, DESCRIPTOR(event_time), INTERVAL '10' SECOND))
GROUP BY window_start, window_end, page;
```

Sliding window: replace `TUMBLE` with `HOP(TABLE events, DESCRIPTOR(event_time),
INTERVAL '10' SECOND, INTERVAL '5' SECOND)`. Session windows use `SESSION(TABLE events,
DESCRIPTOR(event_time), INTERVAL '30' SECOND)`.

Reference: `docs/content/docs/sql/reference/queries/window-tvf.md`.

## Deduplication (latest event per key)

```sql
SELECT user_id, page, event_time
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_time DESC) AS rn
  FROM events
)
WHERE rn = 1;
```

Reference: `docs/content/docs/sql/reference/queries/deduplication.md`.

## TOP-N (per-group ranked rows)

```sql
SELECT user_id, page, cnt
FROM (
  SELECT user_id, page, cnt,
         ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY cnt DESC) AS rn
  FROM (
    SELECT user_id, page, COUNT(*) AS cnt
    FROM events
    GROUP BY user_id, page
  )
)
WHERE rn <= 3;
```

Reference: `docs/content/docs/sql/reference/queries/topn.md`.

## Interval join (bounded only)

```sql
SELECT o.order_id, p.price
FROM orders AS o, payments AS p
WHERE o.order_id = p.order_id
  AND p.pay_time BETWEEN o.order_time AND o.order_time + INTERVAL '30' MINUTE;
```

Both sides need time attributes; the result is only correct/bounded when both inputs have bounded
event time. Reference: `docs/content/docs/sql/reference/queries/joins.md`.

## Temporal join (versioned dimension, event time)

```sql
SELECT o.order_id, o.amount, d.currency_rate
FROM orders AS o
JOIN currency_rates
  FOR SYSTEM_TIME AS OF o.order_time AS d
ON o.currency = d.currency;
```

Requires the dimension table to be upsert/versioned so the planner can read history.

## Lookup join (dimension queried on demand, processing time)

```sql
SELECT o.order_id, d.customer_tier
FROM orders AS o
JOIN customer_lookup
  FOR SYSTEM_TIME AS OF o.proc_time AS d
ON o.customer_id = d.id;
```

Use `LookupJoinHintOptions` (see
`flink-table/flink-table-api-java/src/main/java/org/apache/flink/table/api/config/LookupJoinHintOptions.java`)
to tune async/caching behavior. Lookup joins are the right tool when the dimension is not a
changelog stream.

## Incremental aggregation (aggregate without a window)

```sql
SELECT user_id, COUNT(*) AS events, SUM(amount) AS total
FROM purchases
GROUP BY user_id;
```

Emits `-D`/`+U` updates; the sink must accept a changelog
(`docs/content/docs/sql/reference/queries/changelog.md`). Do not pair with an append-only sink —
use an upsert sink or add a window.

## MATCH_RECOGNIZE (event pattern detection)

```sql
SELECT *
FROM clicks
MATCH_RECOGNIZE (
  PARTITION BY user_id
  ORDER BY event_time
  MEASURES
    FIRST(a.page) AS start_page,
    LAST(b.page) AS bounce_page
  PATTERN (a b+)
  DEFINE
    b.page <> a.page AS is_bounce
);
```

Reference: `docs/content/docs/sql/reference/queries/match_recognize.md`.

## Anti-patterns to avoid

- `ORDER BY ts DESC LIMIT 1` on an unbounded/upserting stream — use the dedup ROW_NUMBER pattern.
- Window functions directly on `GROUP BY` output without a nested subquery — wrap in a subquery.
- Declaring watermarks only on one side of a temporal join — both sides need time attributes.
- `SELECT *` with `INSERT INTO` into a schema with different column order — list columns explicitly.
