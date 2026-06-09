"""
Seed realistic demo data into cinema.movie_events.

Generates ~150k events over the last 8 days with:
  - growing DAU (new cohorts every day)
  - J-curve retention (core / regular / casual user tiers)
  - Zipf-distributed movie popularity (top 5 dominate)
  - mobile-heavy device mix
  - progress_seconds only for VIEW_FINISHED / VIEW_PAUSED

Run inside the `aggregation` container:
    docker cp seed_demo_data.py aggregation:/tmp/seed.py
    docker exec -it aggregation python /tmp/seed.py
"""
from __future__ import annotations

import os
import random
import uuid
from datetime import date, datetime, timedelta, timezone

from clickhouse_driver import Client

# ---- config ----------------------------------------------------------------

DAYS               = 8            # how far back we go
TOTAL_USERS        = 2500
MOVIES             = 50
SEED               = 42

CH_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CH_PORT = int(os.getenv("CLICKHOUSE_PORT", "9000"))
CH_DB   = os.getenv("CLICKHOUSE_DB", "cinema")
CH_USER = os.getenv("CLICKHOUSE_USER", "cinema_user")
CH_PASS = os.getenv("CLICKHOUSE_PASSWORD", "cinema_pass")

EVENT_TYPES   = ["VIEW_STARTED", "VIEW_FINISHED", "VIEW_PAUSED", "VIEW_RESUMED", "LIKED", "SEARCHED"]
EVENT_WEIGHTS = [0.40,           0.28,            0.12,          0.10,           0.06,    0.04]

DEVICE_TYPES   = ["MOBILE", "DESKTOP", "TV",  "TABLET"]
DEVICE_WEIGHTS = [0.50,     0.25,      0.13,  0.12]

# ---- user tiers -----------------------------------------------------------
# each user gets a retention profile:
#   core    (10%): super sticky, ~85% chance to show up on any day after first seen
#   regular (30%): moderate retention, decays
#   casual  (60%): drops off fast
TIER_SHARES = {"core": 0.10, "regular": 0.30, "casual": 0.60}


def activity_prob(tier: str, days_since_first: int) -> float:
    if days_since_first == 0:
        return 1.0
    if tier == "core":
        return 0.85
    if tier == "regular":
        return 0.60 * (0.88 ** days_since_first) + 0.08
    # casual
    return 0.55 * (0.65 ** days_since_first) + 0.04


# ---- main ------------------------------------------------------------------

def main() -> None:
    random.seed(SEED)
    today = date.today()

    # movies: Zipf-ish popularity
    movie_ids = [f"movie_{i+1:03d}" for i in range(MOVIES)]
    movie_weights = [1.0 / ((i + 1) ** 1.3) for i in range(MOVIES)]
    total_w = sum(movie_weights)
    movie_weights = [w / total_w for w in movie_weights]

    # user profiles
    users = []
    for i in range(TOTAL_USERS):
        user_id = f"user_{i:05d}"
        # first_day_offset: 0..DAYS-1 days ago, weighted toward recent
        # beta(2, 4) mean ~0.33 → more fresh users than old
        first_day_offset = int(random.betavariate(2, 4) * DAYS)
        first_day_offset = min(first_day_offset, DAYS - 1)
        # tier
        r = random.random()
        if r < TIER_SHARES["core"]:
            tier = "core"
        elif r < TIER_SHARES["core"] + TIER_SHARES["regular"]:
            tier = "regular"
        else:
            tier = "casual"
        users.append((user_id, first_day_offset, tier))

    # generate events
    rows = []
    # iterate over each user, then for each day from first_seen to today
    for user_id, first_offset, tier in users:
        first_date = today - timedelta(days=first_offset)
        for delta in range(first_offset + 1):
            day_date = first_date + timedelta(days=delta)
            days_since_first = delta
            p = activity_prob(tier, days_since_first)
            if random.random() > p:
                continue
            # number of events this day
            if tier == "core":
                n_events = random.randint(6, 18)
            elif tier == "regular":
                n_events = random.randint(3, 10)
            else:
                n_events = random.randint(1, 5)

            for _ in range(n_events):
                etype    = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]
                movie_id = random.choices(movie_ids,   weights=movie_weights, k=1)[0]
                device   = random.choices(DEVICE_TYPES, weights=DEVICE_WEIGHTS, k=1)[0]

                # random timestamp within the day
                secs = random.randint(0, 86399)
                event_ts = datetime.combine(day_date, datetime.min.time(), tzinfo=timezone.utc) \
                           + timedelta(seconds=secs)

                # progress_seconds: only for finished/paused
                progress = None
                if etype == "VIEW_FINISHED":
                    progress = random.randint(900, 7200)   # 15 min — 2 h
                elif etype == "VIEW_PAUSED":
                    progress = random.randint(30, 4000)

                rows.append((
                    str(uuid.uuid4()),
                    user_id,
                    movie_id,
                    etype,
                    event_ts.replace(tzinfo=None),         # ClickHouse DateTime('UTC') accepts naive
                    day_date,
                    device,
                    str(uuid.uuid4()),
                    progress,
                ))

    print(f"[seed] generated {len(rows):,} events across {DAYS} days, {TOTAL_USERS} users")

    # upload in batches
    client = Client(host=CH_HOST, port=CH_PORT, database=CH_DB, user=CH_USER, password=CH_PASS)

    columns = ("event_id", "user_id", "movie_id", "event_type", "event_ts",
               "event_date", "device_type", "session_id", "progress_seconds")
    batch = 20000
    for start in range(0, len(rows), batch):
        chunk = rows[start:start + batch]
        client.execute(
            f"INSERT INTO cinema.movie_events ({','.join(columns)}) VALUES",
            chunk,
        )
        print(f"[seed] inserted {min(start + batch, len(rows)):,} / {len(rows):,}")

    # sanity summary
    by_day = client.execute(
        "SELECT event_date, count(), uniq(user_id) "
        "FROM cinema.movie_events GROUP BY event_date ORDER BY event_date"
    )
    print("\n[seed] events / DAU per day:")
    for d, n, u in by_day:
        print(f"  {d}  events={n:>6}  DAU={u:>5}")


if __name__ == "__main__":
    main()
