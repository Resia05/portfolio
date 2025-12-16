-- ===============================================================
-- File: user_session_analysis.sql
-- Purpose: Full workflow from initial naive solution to final daily aggregation
-- ===============================================================

-- ====================
-- Step 1: Initial Naive Solution
-- ====================
WITH base AS (
SELECT
id_user,
action,
TIMESTAMP(timestamp_action) AS ts
FROM `analytics.user_sessions_events`
),

max_ts AS (
SELECT MAX(ts) AS max_time FROM base
),

paired AS (
SELECT
b.id_user,
b.ts AS open_time,
LEAD(b.ts) OVER (PARTITION BY b.id_user ORDER BY b.ts) AS next_time,
LEAD(b.action) OVER (PARTITION BY b.id_user ORDER BY b.ts) AS next_action
FROM base b
WHERE b.action = 'open'
),

sessions_naive AS (
SELECT
p.id_user,
p.open_time,
CASE
WHEN p.next_action = 'close' THEN p.next_time
ELSE m.max_time
END AS close_time
FROM paired p
CROSS JOIN max_ts m
),

-- ====================
-- Step 2: EDA / Validation
-- ====================
eda_check AS (
SELECT
id_user,
open_time,
close_time,
TIMESTAMP_DIFF(close_time, open_time, SECOND) / 3600.0 AS hours
FROM sessions_naive
WHERE TIMESTAMP_DIFF(close_time, open_time, HOUR) > 24
),

-- ====================
-- Step 3: Data Cleaning and Flagging
-- ====================
cleaned AS (
SELECT DISTINCT
id_user,
action,
TIMESTAMP(timestamp_action) AS ts
FROM `analytics.user_sessions_events`
),

with_prev AS (
SELECT
id_user,
action,
ts,
LAG(action) OVER (PARTITION BY id_user ORDER BY ts) AS prev_action,
LAG(ts)     OVER (PARTITION BY id_user ORDER BY ts) AS prev_ts
FROM cleaned
),

marked AS (
SELECT
id_user,
action,
ts AS event_ts,
prev_action,
prev_ts,
CASE
WHEN prev_action IS NULL AND action = 'close' THEN 1
WHEN prev_action = 'open'  AND action = 'open'  THEN 1
WHEN prev_action = 'close' AND action = 'close' THEN 1
ELSE 0
END AS is_invalid_chain
FROM with_prev
),

paired_final AS (
SELECT
id_user,
action,
event_ts,
is_invalid_chain,
LEAD(action) OVER (PARTITION BY id_user ORDER BY event_ts) AS next_action,
LEAD(event_ts) OVER (PARTITION BY id_user ORDER BY event_ts) AS next_ts
FROM marked
),

-- ====================
-- Step 4: Session Construction and Split Across Days
-- ====================
sessions AS (
SELECT
id_user,
event_ts AS session_start,
COALESCE(next_ts, (SELECT MAX(ts) FROM cleaned)) AS session_end,
is_invalid_chain,
CASE WHEN action = 'open' AND next_action = 'close' THEN 0 ELSE 1 END AS is_unpaired_open,
CASE WHEN next_ts IS NULL THEN 1 ELSE 0 END AS missing_close,
CASE
WHEN next_ts IS NOT NULL AND TIMESTAMP_DIFF(next_ts, event_ts, HOUR) > 24 THEN 1 ELSE 0
END AS too_long,
TIMESTAMP_DIFF(COALESCE(next_ts, (SELECT MAX(ts) FROM cleaned)), event_ts, SECOND) AS duration_seconds
FROM paired_final
WHERE action = 'open'
),

expanded AS (
SELECT
id_user,
is_invalid_chain,
is_unpaired_open,
missing_close,
too_long,
day,
CASE
WHEN DATE(session_start) = DATE(session_end)
THEN TIMESTAMP_DIFF(session_end, session_start, SECOND)
WHEN day = DATE(session_start)
THEN TIMESTAMP_DIFF(TIMESTAMP_TRUNC(session_start, DAY) + INTERVAL 1 DAY, session_start, SECOND)
WHEN day = DATE(session_end)
THEN TIMESTAMP_DIFF(session_end, TIMESTAMP_TRUNC(session_end, DAY), SECOND)
ELSE 24 * 3600
END AS seconds_in_day
FROM sessions,
UNNEST(GENERATE_DATE_ARRAY(DATE(session_start), DATE(session_end))) AS day
),

-- ====================
-- Step 5: Final Daily Aggregation
-- ====================
max_dates AS (
SELECT MAX(DATE(ts)) AS max_date FROM cleaned
),

date_window AS (
SELECT
DATE_SUB(max_date, INTERVAL 9 DAY) AS start_day,
max_date AS end_day
FROM max_dates
)

SELECT
e.id_user,
e.day,
SUM(e.seconds_in_day) / 3600.0 AS hours_spent_non_rounded,
MAX(CAST(e.is_invalid_chain AS INT64)) AS has_invalid_chain,
MAX(CAST(e.is_unpaired_open AS INT64)) AS has_unpaired_open,
MAX(CAST(e.missing_close AS INT64))    AS missing_close,
MAX(CAST(e.too_long AS INT64))         AS has_too_long_session,
CASE
WHEN MAX(CAST(e.is_invalid_chain AS INT64)) = 0
AND MAX(CAST(e.is_unpaired_open AS INT64)) = 0
AND MAX(CAST(e.missing_close AS INT64))    = 0
AND MAX(CAST(e.too_long AS INT64))         = 0
THEN 'OK'
ELSE 'CHECK'
END AS included_in_report
FROM expanded e
JOIN date_window w
ON e.day BETWEEN w.start_day AND w.end_day
GROUP BY e.id_user, e.day
ORDER BY e.id_user, e.day;
