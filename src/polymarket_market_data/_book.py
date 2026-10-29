"""A token's order book (spec/client.md, Recovery contract).

Pure: no I/O and no timers. A book holds a size for each price on each
side. It starts from a ``book`` event, applies ``price_change`` entries in
arrival order, and compares itself, level for level, with a new book.
"""

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from decimal import Decimal

from ._records import BookEvent, Level, PriceChange, Side


@dataclass(frozen=True, slots=True)
class LevelText:
    """The text the source sent for a level's price and size, which the
    hash writes (spec/client.md, Order-book hash)."""

    price: str
    size: str


_Held = tuple[Level, LevelText]


class Book:
    """A size for each price on each side, as the source sent them.

    Prices compare by value, so ``0.48`` and ``0.480`` are one level. Each
    level keeps the ``Decimal`` it was last set with, and the text the
    source sent for its price and size, which the hash writes: ``str()`` of
    the ``Decimal`` can spell it otherwise, as ``1E-7`` for ``0.0000001``.
    """

    __slots__ = ("_asks", "_bids", "timestamp")

    def __init__(
        self,
        bids: Iterable[_Held] = (),
        asks: Iterable[_Held] = (),
        *,
        timestamp: int,
    ) -> None:
        self._bids = {level.price: (level, text) for level, text in bids}
        self._asks = {level.price: (level, text) for level, text in asks}
        self.timestamp = timestamp
        """The source timestamp of the book's last change, in milliseconds."""

    @classmethod
    def from_event(
        cls, event: BookEvent, bids: Sequence[LevelText], asks: Sequence[LevelText]
    ) -> "Book":
        """The book a ``book`` event brings, with the text the source sent
        for each of its bids and asks, in the event's order."""
        return cls(
            zip(event.bids, bids, strict=True),
            zip(event.asks, asks, strict=True),
            timestamp=event.source_timestamp_ms,
        )

    @property
    def bids(self) -> tuple[Level, ...]:
        """The bids in ascending price order, as the hash input writes them."""
        return tuple(level for level, _ in _ascending(self._bids))

    @property
    def asks(self) -> tuple[Level, ...]:
        """The asks in descending price order, as the hash input writes them."""
        return tuple(level for level, _ in _descending(self._asks))

    @property
    def bid_texts(self) -> tuple[LevelText, ...]:
        """The text the source sent for each bid, in the order of ``bids``."""
        return tuple(text for _, text in _ascending(self._bids))

    @property
    def ask_texts(self) -> tuple[LevelText, ...]:
        """The text the source sent for each ask, in the order of ``asks``."""
        return tuple(text for _, text in _descending(self._asks))

    def apply(self, change: PriceChange, text: LevelText, timestamp: int) -> None:
        """Set the size at the entry's price on its side, ``BUY`` on the
        bids and ``SELL`` on the asks, with ``text``, the text the source
        sent for the entry's price and size; a size of 0 removes the
        level."""
        side = self._bids if change.side is Side.BUY else self._asks
        if change.size == 0:
            side.pop(change.price, None)
        else:
            side[change.price] = (Level(change.price, change.size), text)
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


def _ascending(side: dict[Decimal, _Held]) -> Iterable[_Held]:
    return (side[price] for price in sorted(side))


def _descending(side: dict[Decimal, _Held]) -> Iterable[_Held]:
    return (side[price] for price in sorted(side, reverse=True))


def _sizes(side: dict[Decimal, _Held]) -> dict[Decimal, Decimal]:
    return {price: level.size for price, (level, _) in side.items()}
