-- Backfill ~120k synthetic events across the last 8 days
-- Users pool 400, movies pool 50 -> users reappear across days -> retention works
INSERT INTO cinema.movie_events
    (event_id, user_id, movie_id, event_type, event_ts, event_date,
     device_type, session_id, progress_seconds)
SELECT
    toString(generateUUIDv4())                                                       AS event_id,
    concat('user_', leftPad(toString(rand() % 400), 4, '0'))                         AS user_id,
    concat('movie_', leftPad(toString(rand() % 50), 3, '0'))                         AS movie_id,
    ['VIEW_STARTED','VIEW_PAUSED','VIEW_RESUMED','VIEW_FINISHED','LIKED','SEARCHED']
        [(rand() % 6) + 1]                                                           AS event_type,
    ts                                                                               AS event_ts,
    toDate(ts)                                                                       AS event_date,
    ['TV','MOBILE','TABLET','DESKTOP'][(rand() % 4) + 1]                             AS device_type,
    toString(generateUUIDv4())                                                       AS session_id,
    toInt32(rand() % 7200)                                                           AS progress_seconds
FROM (
    SELECT now() - toIntervalSecond(rand() % (8 * 24 * 3600)) AS ts
    FROM numbers(120000)
);
