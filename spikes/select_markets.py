# /// script
# requires-python = ">=3.12"
# dependencies = ["polymarket-client==0.12.0"]
# ///
"""List candidate markets for the source-behavior investigation.

Writes one JSON object per market to stdout, tagged with the group it was
chosen for:

  long     open markets ending at least --min-days from now, by 24-hour volume
  short    open markets whose slug starts with --short-prefix and that end
           within --short-window minutes, soonest first
  settled  closed markets whose slug starts with --short-prefix and that
           ended within the last --settled-window minutes, latest first

Market lookup goes through the official SDK, which this project pins for it.
"""

import argparse
import asyncio
import json
from datetime import UTC, datetime, timedelta

from polymarket import AsyncPublicClient


def record(group: str, m) -> dict:
    return {
        "group": group,
        "slug": m.slug,
        "question": m.question,
        "condition_id": m.condition_id,
        "token_ids": [m.outcomes.yes.token_id, m.outcomes.no.token_id],
        "outcomes": [m.outcomes.yes.label, m.outcomes.no.label],
        "end_date": m.state.end_date.isoformat() if m.state.end_date else None,
        "closed": m.state.closed,
        "accepting_orders": m.state.accepting_orders,
        "uma_resolution_status": m.resolution.uma_resolution_status,
        "volume_24hr": str(m.metrics.volume_24hr),
    }


async def take(paginator, limit, keep=lambda m: True):
    out = []
    async for m in paginator.iter_items():
        if keep(m):
            out.append(m)
            if len(out) >= limit:
                break
    return out


async def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--long", type=int, default=8, help="long-dated candidates to list")
    p.add_argument("--min-days", type=int, default=30)
    p.add_argument("--long-match", default="election,nomination",
                   help="comma-separated slug substrings for long-dated candidates")
    p.add_argument("--short", type=int, default=6)
    p.add_argument("--short-prefix", default="btc-updown-")
    p.add_argument("--short-window", type=int, default=60, help="minutes")
    p.add_argument("--settled", type=int, default=3)
    p.add_argument("--settled-window", type=int, default=120, help="minutes")
    args = p.parse_args()

    now = datetime.now(UTC)
    words = [w for w in args.long_match.split(",") if w]
    async with AsyncPublicClient() as client:
        long_ = await take(
            client.list_markets(closed=False, end_date_min=now + timedelta(days=args.min_days),
                                order="volume24hr", ascending=False, page_size=100),
            args.long,
            lambda m: m.state.accepting_orders and any(w in (m.slug or "") for w in words),
        )
        short = await take(
            client.list_markets(closed=False, end_date_min=now,
                                end_date_max=now + timedelta(minutes=args.short_window),
                                order="endDate", ascending=True, page_size=100),
            args.short,
            lambda m: (m.slug or "").startswith(args.short_prefix),
        )
        settled = await take(
            client.list_markets(closed=True,
                                end_date_min=now - timedelta(minutes=args.settled_window),
                                end_date_max=now, order="endDate", ascending=False,
                                page_size=100),
            args.settled,
            lambda m: (m.slug or "").startswith(args.short_prefix),
        )
    for group, markets in (("long", long_), ("short", short), ("settled", settled)):
        for m in markets:
            print(json.dumps(record(group, m)))


if __name__ == "__main__":
    asyncio.run(main())
