"""최신 호가 상태 저장소 (인메모리 핫패스)."""
from __future__ import annotations

from ..models import Book, now_ms


class BookStore:
    def __init__(self) -> None:
        self._books: dict[tuple[str, str, str], Book] = {}

    def update(self, book: Book) -> None:
        self._books[book.key] = book

    def get(self, exchange: str, base: str, quote: str) -> Book | None:
        return self._books.get((exchange, base, quote))

    def fresh(self, exchange: str, base: str, quote: str, max_age_ms: int) -> Book | None:
        """max_age_ms 이내에 수신된 호가만 반환. 스테일이면 None (PLAN §8 — stale 시세로 판단 금지)."""
        b = self._books.get((exchange, base, quote))
        if b is None or now_ms() - b.ts_local > max_age_ms:
            return None
        return b
