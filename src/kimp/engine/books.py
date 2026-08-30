"""최신 호가 상태 저장소 (인메모리 핫패스)."""
from __future__ import annotations

from ..models import Book, now_ms


class BookStore:
    def __init__(self) -> None:
        self._books: dict[tuple[str, str, str], Book] = {}

    def update(self, book: Book) -> None:
        # 순서 역전 가드 (인계서 §2.3 흡수): 재연결·REST 혼용 시 옛 스냅샷이 새 호가를 덮지 못하게.
        # 거래소 타임스탬프 우선, 없으면 로컬 수신 시각으로 비교
        old = self._books.get(book.key)
        if old is not None:
            if book.ts_exchange is not None and old.ts_exchange is not None:
                if book.ts_exchange < old.ts_exchange:
                    return
            elif book.ts_local < old.ts_local:
                return
        self._books[book.key] = book

    def get(self, exchange: str, base: str, quote: str) -> Book | None:
        return self._books.get((exchange, base, quote))

    def fresh(self, exchange: str, base: str, quote: str, max_age_ms: int) -> Book | None:
        """max_age_ms 이내에 수신된 호가만 반환. 스테일이면 None (PLAN §8 — stale 시세로 판단 금지)."""
        b = self._books.get((exchange, base, quote))
        if b is None or now_ms() - b.ts_local > max_age_ms:
            return None
        return b
