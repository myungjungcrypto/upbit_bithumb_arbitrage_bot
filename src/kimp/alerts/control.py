"""텔레그램 관제탑 — 양방향 (PLAN §1.3).

수신 기능:
- /status : 시스템 상태 요약
- /stop   : 수동 L1 (신규 진입 차단 — 진행 중 사이클은 완주)
- /resume : L1 해제
- 인라인 버튼 콜백: 출금 승인/거부 (§4.1 방어선 5 감독 모드)

보안: chat_id 화이트리스트 — 다른 채팅의 메시지·콜백은 무시 (§1.3).
텔레그램에는 정지·승인 권한만 있고 출금을 발의할 수는 없다 (게이트웨이가 발의, 여긴 승인만).
"""
from __future__ import annotations

import asyncio
import json
import logging
import uuid

import aiohttp

log = logging.getLogger("control.telegram")


class TelegramControl:
    def __init__(self, token: str, chat_id: str) -> None:
        self.token = token
        self.chat_id = str(chat_id)
        self.commands: dict[str, callable] = {}      # "/status" -> () -> str
        self._pending: dict[str, asyncio.Future] = {}  # 승인 대기 (req_id -> Future[bool])
        self._offset = 0

    @property
    def live(self) -> bool:
        return bool(self.token and self.chat_id)

    def on_command(self, cmd: str, fn) -> None:
        self.commands[cmd] = fn

    # ---------- 승인 요청 (게이트웨이가 호출) ----------

    async def request_approval(self, text: str, timeout_sec: float) -> bool:
        """인라인 버튼으로 승인을 요청하고 응답을 기다린다. 타임아웃/미가동 = 거부 (보수)."""
        if not self.live:
            log.warning("텔레그램 미가동 — 승인 요청 자동 거부: %s", text)
            return False
        req_id = uuid.uuid4().hex[:10]
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        self._pending[req_id] = fut
        try:
            await self._api("sendMessage", {
                "chat_id": self.chat_id,
                "text": f"🔐 승인 요청\n{text}\n(제한시간 {timeout_sec/60:.0f}분 — 무응답 = 거부)",
                "reply_markup": json.dumps({"inline_keyboard": [[
                    {"text": "✅ 승인", "callback_data": f"ap:{req_id}:1"},
                    {"text": "❌ 거부", "callback_data": f"ap:{req_id}:0"},
                ]]}),
            })
            return await asyncio.wait_for(fut, timeout=timeout_sec)
        except asyncio.TimeoutError:
            return False
        except Exception:
            log.exception("approval request failed")
            return False
        finally:
            self._pending.pop(req_id, None)

    # ---------- 수신 루프 ----------

    def handle_update(self, u: dict) -> tuple[str, str] | None:
        """업데이트 1건 처리 → (응답 chat_id, 응답 텍스트) 또는 None. 순수 로직 (테스트 대상)."""
        msg = u.get("message")
        if msg:
            if str(msg.get("chat", {}).get("id")) != self.chat_id:
                return None  # 화이트리스트 외 — 무시
            text = (msg.get("text") or "").strip().split()[0] if msg.get("text") else ""
            fn = self.commands.get(text)
            if fn:
                try:
                    return (self.chat_id, str(fn()))
                except Exception as e:
                    return (self.chat_id, f"명령 실패: {e!r}")
            if text.startswith("/"):
                return (self.chat_id, f"모르는 명령. 사용 가능: {', '.join(sorted(self.commands))}")
            return None
        cb = u.get("callback_query")
        if cb:
            if str(cb.get("message", {}).get("chat", {}).get("id")) != self.chat_id:
                return None
            data = cb.get("data") or ""
            if data.startswith("ap:"):
                _, req_id, val = data.split(":")
                fut = self._pending.get(req_id)
                if fut and not fut.done():
                    fut.set_result(val == "1")
                    return (self.chat_id, "✅ 승인됨" if val == "1" else "❌ 거부됨")
                return (self.chat_id, "이미 처리됐거나 만료된 요청")
        return None

    async def run(self, stop: asyncio.Event) -> None:
        if not self.live:
            await stop.wait()
            return
        async with aiohttp.ClientSession(trust_env=True) as sess:
            self._sess = sess
            while not stop.is_set():
                try:
                    updates = await self._api("getUpdates", {
                        "offset": self._offset, "timeout": 25,
                        "allowed_updates": '["message","callback_query"]',
                    }, timeout=35)
                    for u in updates.get("result", []):
                        self._offset = max(self._offset, u["update_id"] + 1)
                        cb = u.get("callback_query")
                        if cb:  # 버튼 응답 표시 (로딩 스피너 해제)
                            try:
                                await self._api("answerCallbackQuery", {"callback_query_id": cb["id"]})
                            except Exception:
                                pass
                        reply = self.handle_update(u)
                        if reply:
                            await self._api("sendMessage", {"chat_id": reply[0], "text": reply[1]})
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    log.warning("control poll error: %r", e)
                    try:
                        await asyncio.wait_for(stop.wait(), timeout=5)
                    except asyncio.TimeoutError:
                        pass

    async def _api(self, method: str, params: dict, timeout: float = 15):
        async with self._sess.post(
            f"https://api.telegram.org/bot{self.token}/{method}",
            data=params, timeout=aiohttp.ClientTimeout(total=timeout),
        ) as resp:
            body = await resp.json(content_type=None)
            if not body.get("ok"):
                raise RuntimeError(f"telegram {method} failed: {body}")
            return body
