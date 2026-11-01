"""The quickstart: the shortest useful program. It resolves the market whose
slug it is given, subscribes to it, prints each record, and ends when the
client reports ``idle``, which follows the market's settlement
(spec/client.md, Public interface and Connection states;
docs/implementation-plan.md, 11. Add the research example and check the
built wheel).

It needs the sdk extra, which provides the market lookup::

    python examples/quickstart.py <slug>

The README holds a copy of the program below this docstring, and a test
checks that the two match.
"""

import asyncio
import sys

from polymarket_market_data import (
    BookEvent,
    CaptureGap,
    ClientConfig,
    ConnectionState,
    ConnectionStateChange,
    MarketDataClient,
    MarketLookup,
    PriceChangeEvent,
    TokenState,
    TokenStateChange,
)


async def main(
    slug: str, config: ClientConfig | None = None, lookup: MarketLookup | None = None
) -> None:
    config = ClientConfig() if config is None else config
    async with MarketDataClient(config, lookup=lookup) as client:
        client.subscribe(await client.resolve(slug))
        async for record in client.records():
            match record:
                case TokenStateChange(state=TokenState.READY):
                    print(f"token {record.token_id} ready: its book can be used")
                case TokenStateChange(state=TokenState.SETTLED):
                    print(
                        f"token {record.token_id} settled, "
                        f"winner {record.winning_asset_id}"
                    )
                case TokenStateChange():
                    print(f"token {record.token_id} {record.state}: {record.reason}")
                case BookEvent():
                    print(
                        f"book {record.asset_id}: {len(record.bids)} bid "
                        f"and {len(record.asks)} ask levels"
                    )
                case PriceChangeEvent():
                    entries = (f"{c.side} {c.price} x {c.size}" for c in record.changes)
                    print("price change:", ", ".join(entries))
                case CaptureGap():
                    print(
                        f"gap {record.token_id}: events since "
                        f"{record.last_confirmed_at} may be missing"
                    )
                case ConnectionStateChange(state=ConnectionState.IDLE):
                    print("idle: no unsettled market is left")
                    return
                case ConnectionStateChange():
                    print(f"connection {record.state}")
                case _:
                    print(type(record).__name__)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("usage: python examples/quickstart.py <slug>")
    asyncio.run(main(sys.argv[1]))
