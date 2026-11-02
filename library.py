"""传话插件的纯逻辑：找人、选通道、转告文案、限频、触发词（不依赖 AstrBot，可单测）。"""

from __future__ import annotations

import re
import time
from collections import deque


def _norm(text: str) -> str:
    return re.sub(r"\s+", "", str(text or "")).lower()


def match_person(target: str, members: list[dict]) -> tuple[dict | None, list[dict]]:
    """在成员列表里找人。members: [{"qq": "123", "name": "名片/昵称"}]。

    评分：QQ 完全相等 1.0；名字完全相等 1.0；名字互相包含 0.7。
    返回 (最佳成员|None, 并列最高分的候选列表)；无达到 0.7 的返回 (None, [])。
    """
    key = _norm(target)
    if not key:
        return None, []
    scored: list[tuple[float, dict]] = []
    for member in members:
        qq = _norm(member.get("qq"))
        name = _norm(member.get("name"))
        score = 0.0
        if qq and qq == key:
            score = 1.0
        elif name and name == key:
            score = 1.0
        elif name and (key in name or name in key):
            score = 0.7
        if score > 0:
            scored.append((score, member))
    if not scored:
        return None, []
    top = max(score for score, _ in scored)
    if top < 0.7:
        return None, []
    best = next(member for score, member in scored if score == top)
    tied = [member for score, member in scored if score == top]
    return best, tied


def decide_channel(
    in_group: bool,
    target_in_group: bool,
    is_friend: bool,
    requested: str = "auto",
) -> tuple[str, str]:
    """决定联系方式。返回 (channel, 失败原因)；channel 为空串表示无法联系。

    规则：对方在当前群 → 群里@；其他情况只私聊；显式 private/group 覆盖 auto。
    """
    requested = (requested or "auto").strip().lower()
    if requested == "group":
        if target_in_group:
            return "group", ""
        return "", "对方不在当前群聊里，没法在群里找TA"
    if requested == "private":
        if is_friend:
            return "private", ""
        return "", "只能私聊TA，但TA还不是好友"
    # auto
    if in_group and target_in_group:
        return "group", ""
    if is_friend:
        return "private", ""
    if in_group:
        return "", "TA不在当前群聊里，也还不是好友，私聊发不出去"
    return "", "TA还不是好友，私聊发不出去"


def relay_text(name: str, text: str, limit: int = 200) -> str:
    """把对方的私聊回复转告给发起人。"""
    body = re.sub(r"\s+", " ", str(text or "")).strip()
    if len(body) > limit:
        body = body[:limit] + "…"
    return f"（传话回复）{name or '对方'} 回你了：{body}"


def platform_id_from_umo(umo: str, fallback: str = "") -> str:
    """从 unified_msg_origin 取平台 ID（如 napcat），不是平台类型（如 aiocqhttp）。

    AstrBot 的 umo 形如 `napcat:FriendMessage:123456`，第一段是平台实例 ID。
    `event.get_platform_name()` 返回的是**类型**，用它拼 umo 会导致
    「cannot find platform for session aiocqhttp:FriendMessage:xxx」发不出去。
    """
    text = str(umo or "")
    if ":" in text:
        pid = text.split(":", 1)[0].strip()
        if pid:
            return pid
    return str(fallback or "")


class RateLimiter:
    """滑动窗口限频：window 秒内最多 max_count 次（0 = 不限）。"""

    def __init__(self, window_seconds: int = 3600, max_count: int = 10) -> None:
        self.window = max(1, int(window_seconds))
        self.max_count = max(0, int(max_count))
        self._hits: deque[float] = deque()

    def allow(self, now: float | None = None) -> bool:
        if self.max_count <= 0:
            return True
        now = time.time() if now is None else now
        while self._hits and now - self._hits[0] > self.window:
            self._hits.popleft()
        if len(self._hits) >= self.max_count:
            return False
        self._hits.append(now)
        return True

    def used(self, now: float | None = None) -> int:
        now = time.time() if now is None else now
        while self._hits and now - self._hits[0] > self.window:
            self._hits.popleft()
        return len(self._hits)


_CONTACT_RE = re.compile(r"(去问|帮我问|问问|告诉|转告|捎话|带话|提醒|传话给|联系一下)")


def looks_like_contact_request(text: str) -> bool:
    """粗判用户是否想让机器人主动联系某人（用于提示注入，不拦消息）。"""
    return bool(_CONTACT_RE.search(str(text or "")))
