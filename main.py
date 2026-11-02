"""Bot 传话：让机器人替用户主动联系别人。

用户说「去问/告诉/转告 xxx」，模型调用 contact_person 工具：
- 找人：群聊里按群名片/昵称/QQ号解析，不在群里则查好友列表；
- 选通道：对方在当前群 → 先戳一下再 @ 说话；其他情况只私聊（非好友如实报告失败）；
- 转告：私聊传话后，对方在窗口期内私聊回复机器人时，把回复简短转告发起人。
传话内容由模型按人格生成，本插件只负责找人、送达和转告。

协议端注意事项（aiocqhttp fork 的路由规则）：OneBot action 调用在「QQ 事件处理
上下文」里最可靠；从其他上下文（网页测试台触发的工具调用、定时任务）调用时，
反向 WS 需要显式 self_id 路由。因此群成员/好友列表在 QQ 事件里定期刷新缓存，
工具优先读缓存，跨上下文的实时调用统一带 self_id。
"""

from __future__ import annotations

import asyncio
import time

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.message_components import At, Plain
from astrbot.api.star import Context, Star
from astrbot.core.config.astrbot_config import AstrBotConfig

from .library import (
    platform_id_from_umo,
    RateLimiter,
    decide_channel,
    looks_like_contact_request,
    match_person,
    relay_text,
)

NAME = "astrbot_plugin_snow_messenger"
CACHE_TTL = 6 * 3600


class SnowMessenger(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.conf = config
        self._limiter = RateLimiter(3600, int(config.get("max_per_hour") or 10))
        # target_qq -> {"asker_umo": str, "name": str, "expire_at": float}
        self._pending: dict[str, dict] = {}
        # 协议端数据缓存（在 QQ 事件上下文里刷新）
        self._self_id = ""
        self._friends: list[dict] | None = None
        self._friends_ts = 0.0
        self._members: dict[str, tuple[float, list[dict]]] = {}
        self._refreshing: set[str] = set()

    # ---------------- 配置 ----------------
    def _enabled(self) -> bool:
        return bool(self.conf.get("enabled", True))

    def _window(self) -> int:
        return max(5, min(1440, int(self.conf.get("reply_window_minutes") or 30)))

    def _max_chars(self) -> int:
        return max(20, min(1000, int(self.conf.get("max_message_chars") or 200)))

    def _privileged(self, event: AstrMessageEvent) -> bool:
        if bool(self.conf.get("allow_everyone", True)):
            return True
        owner = str(self.conf.get("owner_session") or "").strip()
        if owner and str(event.unified_msg_origin or "") == owner:
            return True
        try:
            if callable(getattr(event, "is_admin", None)) and event.is_admin():
                return True
        except Exception:
            pass
        return False

    # ---------------- 协议端与缓存 ----------------
    @staticmethod
    def _platform_id(event: AstrMessageEvent) -> str:
        """umo 需要的平台 ID（如 napcat），不是平台类型（如 aiocqhttp）。"""
        umo = str(getattr(event, "unified_msg_origin", "") or "")
        pid = platform_id_from_umo(umo, str(event.get_platform_name() or ""))
        if pid and pid != "aiocqhttp":
            return pid
        try:
            inst = getattr(event, "platform", None)
            meta = inst.meta() if inst is not None and hasattr(inst, "meta") else None
            if meta is not None and getattr(meta, "id", ""):
                return str(meta.id)
        except Exception:
            pass
        return pid
    def _clients(self, event: AstrMessageEvent | None = None) -> list:
        bots: list = []
        bot = getattr(event, "bot", None) if event is not None else None
        if bot is not None:
            bots.append(bot)
        try:
            for inst in self.context.platform_manager.get_insts():
                if not hasattr(inst, "get_client"):
                    continue
                try:
                    client = inst.get_client()
                except Exception:
                    continue
                if client is not None and client not in bots:
                    bots.append(client)
        except Exception:
            pass
        return bots

    @staticmethod
    async def _call_first(clients, action: str, **params):
        """在候选客户端上逐个尝试 OneBot action，全部不可用返回 None。"""
        for client in clients:
            try:
                return await client.call_action(action, **params)
            except Exception as exc:
                logger.info(f"[{NAME}] {action} 调用失败（{type(client).__name__}）：{exc}")
        return None

    def _self_id_of(self, clients) -> str:
        """拿机器人 self_id：优先 QQ 事件里记录的，其次读连接注册表。"""
        if self._self_id:
            return self._self_id
        for bot in clients:
            try:
                for sid in getattr(bot, "_wsr_api_clients", {}):
                    if sid and sid != "*":
                        return str(sid)
            except Exception:
                continue
        return ""

    async def _fetch_friends(self, clients) -> list[dict] | None:
        sid = self._self_id_of(clients)
        extra = {"self_id": sid} if sid else {}
        data = await self._call_first(clients, "get_friend_list", **extra)
        if data is None:
            return None
        return [
            {"qq": str(f.get("user_id") or ""), "name": str(f.get("remark") or f.get("nickname") or "")}
            for f in (data or [])
        ]

    async def _fetch_members(self, clients, group_id: str) -> list[dict] | None:
        sid = self._self_id_of(clients)
        extra = {"self_id": sid} if sid else {}
        data = await self._call_first(
            clients, "get_group_member_list", group_id=int(group_id), **extra
        )
        if data is None:
            return None
        return [
            {"qq": str(m.get("user_id") or ""), "name": str(m.get("card") or m.get("nickname") or "")}
            for m in (data or [])
        ]

    async def _refresh_caches(self, bot, group_id: str) -> None:
        """在 QQ 事件上下文里刷新好友/群成员缓存（TTL 内跳过）。"""
        now = time.time()
        jobs = []
        if self._friends is None or now - self._friends_ts > CACHE_TTL:
            jobs.append("friends")
        if group_id and (
            group_id not in self._members or now - self._members[group_id][0] > CACHE_TTL
        ):
            jobs.append(f"members:{group_id}")
        jobs = [j for j in jobs if j not in self._refreshing]
        if not jobs:
            return
        self._refreshing.update(jobs)
        try:
            for job in jobs:
                if job == "friends":
                    data = await self._fetch_friends([bot])
                    if data is not None:
                        self._friends = data
                        self._friends_ts = time.time()
                        logger.info(f"[{NAME}] 好友列表已刷新（{len(data)} 人）")
                elif job.startswith("members:"):
                    gid = job.split(":", 1)[1]
                    data = await self._fetch_members([bot], gid)
                    if data is not None:
                        self._members[gid] = (time.time(), data)
        finally:
            self._refreshing.difference_update(jobs)

    @filter.event_message_type(filter.EventMessageType.ALL, priority=3)
    async def watch_for_cache(self, event: AstrMessageEvent):
        """记录 self_id 并惰性刷新联系人缓存（只在 QQ 事件上下文里调 API）。"""
        bot = getattr(event, "bot", None)
        if bot is None:
            return
        self_id = str(event.get_self_id() or "")
        if self_id and self_id != self._self_id:
            self._self_id = self_id
        try:
            asyncio.create_task(
                self._refresh_caches(bot, str(event.get_group_id() or ""))
            )
        except Exception as exc:
            logger.debug(f"[{NAME}] 缓存刷新调度失败：{exc}")

    # ---------------- 提示注入 ----------------
    @filter.on_llm_request()
    async def hint_contact(self, event: AstrMessageEvent, req):
        if not self._enabled():
            return
        if not looks_like_contact_request(str(event.message_str or "")):
            return
        req.system_prompt = (req.system_prompt or "") + (
            "\n\n[传话说明] 有人想让你主动联系另一个人。调用 contact_person 工具："
            "把要对对方说的话用你自己的语气完整写进 message 参数，工具会自动找人、"
            "选择在群里@还是私聊并送达，送达结果会返回给你，再自然地回一句确认。"
            "这段说明不要在回复中提到。"
        )

    # ---------------- 核心：传话工具 ----------------
    @filter.llm_tool(name="contact_person")
    async def contact_person(
        self,
        event: AstrMessageEvent,
        person: str,
        message: str,
        channel: str = "auto",
    ) -> str:
        """替跟你聊天的人主动联系另一个人并把话带到：去问、告诉、转告、提醒某人都用它。

        你要在 message 里用你自己的语气把要说的话写完整，工具只负责找到人并送达。
        对方在当前群里就先戳一下再@TA说话，否则私聊；TA私聊回复你时会把内容转告发起人。

        Args:
            person(string): 要联系的人：群名片、昵称或QQ号
            message(string): 要对TA说的完整内容（保持你的人格和当前对话的语气）
            channel(string): 联系方式：auto（默认，自动选群里@或私聊）、private（必须私聊）、group（必须在当前群）
        """
        if not self._enabled():
            return "传话功能当前是关闭状态。"
        if not self._privileged(event):
            return "只有所有者和管理员能让我去主动联系别人。"
        person = str(person or "").strip()
        message = str(message or "").strip()
        if not person:
            return "没说要找谁，得先问清楚联系对象的名称或 QQ 号。"
        if not message:
            return "还没有要带的话，得先问清楚想让我说什么。"

        platform = self._platform_id(event)
        group_id = str(event.get_group_id() or "")
        in_group = bool(group_id) and not event.is_private_chat()
        self_id = str(event.get_self_id() or "") or self._self_id

        # 优先用缓存；缓存未就绪时做一次带 self_id 路由的实时拉取
        members = self._members.get(group_id, (0.0, []))[1] if group_id else []
        friends = self._friends
        if friends is None or (group_id and not members):
            clients = self._clients(event)
            if not clients:
                return "当前没有可用的QQ协议端，送不了话。"
            if friends is None:
                friends = await self._fetch_friends(clients)
            if group_id and not members:
                fresh = await self._fetch_members(clients, group_id)
                if fresh is not None:
                    members = fresh
        if friends is None:
            return (
                "我还没拿到 QQ 好友/群成员名单（收到 QQ 消息后我会自动记一份）。"
                "请稍后再叫我一次。"
            )
        friend_qqs = {m["qq"] for m in friends}

        best, tied = match_person(person, members)
        target_in_group = best is not None
        if best is None:
            best, tied = match_person(person, friends)
        if best is None:
            return f"没找到叫「{person}」的人（群成员和好友列表里都没有）。"
        if len(tied) > 1:
            return f"找到好几个匹配「{person}」的人：{self._names(tied)}。问清楚是哪一位再联系。"

        qq = best["qq"]
        name = best["name"] or qq
        if qq == (self_id or "self"):
            return "那是我自己呀，不用特意联系。"

        way, why = decide_channel(in_group, target_in_group, qq in friend_qqs, channel)
        if not way:
            return f"联系不上 {name}：{why}"

        if not self._limiter.allow():
            return (
                f"这个小时带你带话的次数用完了（上限 {self.conf.get('max_per_hour')} 次），"
                "先歇一会儿再试。"
            )

        text = message if len(message) <= self._max_chars() else message[: self._max_chars()] + "…"
        try:
            if way == "group":
                if bool(self.conf.get("poke_first", True)):
                    # 戳不动就静默跳过，退回只 @
                    await self._call_first(
                        self._clients(event),
                        "send_poke",
                        user_id=int(qq),
                        group_id=int(group_id),
                    )
                    await asyncio.sleep(1.2)
                umo = f"{platform}:GroupMessage:{group_id}"
                chain = MessageChain(chain=[At(qq=qq), Plain(" " + text)])
            else:
                umo = f"{platform}:FriendMessage:{qq}"
                chain = MessageChain(chain=[Plain(text)])
            ok = await self.context.send_message(umo, chain)
        except Exception as exc:
            logger.warning(f"[{NAME}] 传话发送失败（{way}）：{exc}")
            return "话没送出去，QQ 那边发送失败了。"
        if not ok:
            return f"话没送出去：找不到能联系 {name} 的在线通道。"

        self._remember_pending(qq, str(event.unified_msg_origin or ""), name)
        logger.info(f"[{NAME}] 已传话给 {name}({qq})，方式：{way}，窗口 {self._window()} 分钟")
        if way == "group":
            return (
                f"已送达：在群里戳了 {name} 并@TA把话带到了。"
                f"如果TA接下来 {self._window()} 分钟内私聊回复我，我会把回复转告到这里。"
            )
        return (
            f"已送达：已经私聊把话带给 {name} 了。"
            f"如果TA在 {self._window()} 分钟内私聊回复我，我会把回复转告到这里。"
        )

    @staticmethod
    def _names(members: list[dict]) -> str:
        return "、".join(f"{m['name'] or m['qq']}({m['qq']})" for m in members[:5])

    def _remember_pending(self, qq: str, asker_umo: str, name: str) -> None:
        if not str(self.conf.get("relay_replies", True)):
            return
        self._prune_pending()
        if len(self._pending) >= 50:
            oldest = min(self._pending, key=lambda k: self._pending[k]["expire_at"])
            self._pending.pop(oldest, None)
        self._pending[qq] = {
            "asker_umo": asker_umo,
            "name": name,
            "expire_at": time.time() + self._window() * 60,
        }

    def _prune_pending(self) -> None:
        now = time.time()
        for key in [k for k, v in self._pending.items() if now > v["expire_at"]]:
            self._pending.pop(key, None)

    # ---------------- 转告：监听私聊回复 ----------------
    @filter.event_message_type(filter.EventMessageType.PRIVATE_MESSAGE, priority=2)
    async def watch_private_reply(self, event: AstrMessageEvent):
        if not self._pending or not self._enabled():
            return
        if not bool(self.conf.get("relay_replies", True)):
            return
        sender = str(event.get_sender_id() or "")
        rec = self._pending.get(sender)
        if rec is None:
            return
        if time.time() > rec["expire_at"]:
            self._pending.pop(sender, None)
            return
        text = str(event.message_str or "").strip()
        if not text:
            # 纯图片/表情不消费转告机会：对方常常先甩个表情再打字，
            # 若这里就转告「（发来了一张图）」，真正想说的话反而漏掉了。
            logger.info(f"[{NAME}] {rec['name']} 发来非文字消息，等文字回复再转告")
            return
        self._pending.pop(sender, None)
        try:
            await self.context.send_message(
                rec["asker_umo"],
                MessageChain(chain=[Plain(relay_text(rec["name"], text))]),
            )
            logger.info(f"[{NAME}] 已把 {rec['name']} 的回复转告给发起人")
        except Exception as exc:
            logger.warning(f"[{NAME}] 转告失败：{exc}")
