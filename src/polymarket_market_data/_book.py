"""A token's order book (spec/client.md, Recovery contract).

Pure: no I/O and no timers. A book holds a size for each price on each
side. It starts from a ``book`` event, applies ``price_change`` entries in
arrival order, and compares itself, level for level, with a new book.
"""

from collections.abc import Iterable
from decimal import Decimal

from ._records import BookEvent, Level, PriceChange, Side


class Book:
    """A size for each price on each side, as the source sent them.

    Prices compare by value, so ``0.48`` and ``0.480`` are one level, and
    each level keeps the ``Decimal`` it was last set with, whose ``str()``
    is the text the source sent.
    """

    __slots__ = ("_asks", "_bids", "timestamp")

    def __init__(
        self,
        bids: Iterable[Level] = (),
        asks: Iterable[Level] = (),
        *,
        timestamp: int,
    ) -> None:
        self._bids = {level.price: level for level in bids}
        self._asks = {level.price: level for level in asks}
        self.timestamp = timestamp
        """The source timestamp of the book's last change, in milliseconds."""

    @classmethod
    def from_event(cls, event: BookEvent) -> "Book":
        return cls(event.bids, event.asks, timestamp=event.source_timestamp_ms)

    @property
    def bids(self) -> tuple[Level, ...]:
        """The bids in ascending price order, as the hash input writes them."""
        return tuple(self._bids[price] for price in sorted(self._bids))

    @property
    def asks(self) -> tuple[Level, ...]:
        """The asks in descending price order, as the hash input writes them."""
        return tuple(self._asks[price] for price in sorted(self._asks, reverse=True))

    def apply(self, change: PriceChange, timestamp: int) -> None:
        """Set the size at the entry's price on its side, ``BUY`` on the
        bids and ``SELL`` on the asks; a size of 0 removes the level."""
        side = self._bids if change.side is Side.BUY else self._asks
        if change.size == 0:
            side.pop(change.price, None)
        else:
            side[change.price] = Level(change.price, change.size)
        self.timestamp = timestamp

    def matches(self, other: "Book") -> bool:
        """Whether every price and size on both sides is equal."""
        mine = (_sizes(self._bids), _sizes(self._asks))
        return mine == (_sizes(other._bids), _sizes(other._asks))

    def copy(self) -> "Book":
        return Book(self._bids.values(), self._asks.values(), timestamp=self.timestamp)

    def __repr__(self) -> str:
        return (
            f"Book(bids={self.bids!r}, asks={self.asks!r}, timestamp={self.timestamp})"
        )


def _sizes(side: dict[Decimal, Level]) -> dict[Decimal, Decimal]:
    return {price: level.size for price, level in side.items()}
