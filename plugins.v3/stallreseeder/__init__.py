"""卡种换源插件（StallReseeder）。

解决的问题
----------
qBittorrent 里一个种子卡在 0%（死种）时，MoviePilot 既不会判定它失败，也不会换源：
- 下载阶段卡住，对 MP 来说既不算成功也不算失败，永远不会写进下载失败台账；
- 订阅搜索会把「已推送」的集数记为已处理，缺集数归零，于是没有动机再选；
- StallCleaner 只会删任务，删完这个订阅仍然缺集，下一次收集又会选中同一个死种。

本插件把闭环串起来：
1. 判定停滞：只看一个字节都没下载的 0% 任务（默认），正在慢速下载的任务绝不处理；
2. 反查身份：用下载历史按 hash 拿到媒体身份、季集、来源站点；
3. 定位资源：回到原站点搜索同一个发布，拿到资源页面；
4. 写冷却：用 MP 自己的算法算出与该资源一致的冷却指纹，写入原生 downloadfailure 台账，
   订阅搜索就会主动跳过这颗死种，去挑下一个资源；
5. 触发重搜：定向触发该订阅的一次搜索，筛选与下载仍由 MP 自己完成，不复制规则引擎。

找不到资源页面时自动退回「只删 + 重搜」，绝不乱写指纹。默认演练模式，只记录不动作。
"""

import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.schemas import NotificationType, ServiceInfo
from app.sdk.config import settings
from app.sdk.logging import logger
from app.sdk.plugin import _PluginBase
from app.sdk.services import DownloaderHelper

# 触发重搜会阻塞到搜索结束，放到后台线程，避免拖住扫描周期
_EXECUTOR = ThreadPoolExecutor(max_workers=2, thread_name_prefix="StallReseeder")

_TIME_FMT = "%Y-%m-%d %H:%M:%S"


def _norm_text(text: Any) -> str:
    """标题归一化：只保留中文与字母数字。"""
    return re.sub(r"[^0-9a-z\u4e00-\u9fff]+", "", str(text or "").lower())


def _same_release(left: Any, right: Any) -> bool:
    """判断两个种子名是否指向同一个发布。"""
    first, second = _norm_text(left), _norm_text(right)
    if not first or not second:
        return False
    if first == second:
        return True
    shorter, longer = (first, second) if len(first) <= len(second) else (second, first)
    if len(shorter) < 12:
        return False
    return shorter in longer and len(shorter) / len(longer) >= 0.85


def _parse_season(seasons: Any) -> Optional[int]:
    """从 S01 这样的文本里解析季号。"""
    match = re.search(r"S(\d{1,3})", str(seasons or ""))
    return int(match.group(1)) if match else None


class StallReseeder(_PluginBase):
    """0% 死种自动换源。"""

    # 插件名称
    plugin_name = "卡种换源"
    # 插件描述
    plugin_desc = "下载任务卡在 0% 时自动删除、把该资源写进 MP 冷却台账，并触发订阅重新搜索换源。"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/huanqi666/MoviePilot-Plugins/main/icons/stallreseeder.svg"
    # 插件版本
    plugin_version = "1.0.0"
    # 插件作者
    plugin_author = "huanqi666"
    # 作者主页
    author_url = "https://github.com/huanqi666/MoviePilot-Plugins"
    # 插件配置项ID前缀
    plugin_config_prefix = "stallreseeder_"
    # 加载顺序
    plugin_order = 31
    # 可使用的用户级别
    auth_level = 2

    # 私有属性
    _scheduler: Optional[BackgroundScheduler] = None
    _enabled: bool = False
    _notify: bool = False
    _onlyonce: bool = False
    _dryrun: bool = True
    _cron: Optional[str] = None
    _downloaders: List[str] = []
    _stall_minutes: int = 30
    _min_speed: float = 10
    _grace_minutes: int = 30
    _zero_only: bool = True
    _max_per_round: int = 2
    _cooldown_hours: int = 24
    _retry_search: bool = True
    _exclude_tags: str = "刷流"

    def init_plugin(self, config: dict = None):
        """读取配置并按需注册一次性任务。"""
        if config:
            self._enabled = bool(config.get("enabled"))
            self._notify = bool(config.get("notify"))
            self._onlyonce = bool(config.get("onlyonce"))
            self._dryrun = bool(config.get("dryrun", True))
            self._cron = config.get("cron")
            self._downloaders = config.get("downloaders") or []
            self._stall_minutes = int(config["stall_minutes"]) if config.get("stall_minutes") is not None else 30
            self._min_speed = float(config["min_speed"]) if config.get("min_speed") is not None else 10
            self._grace_minutes = int(config["grace_minutes"]) if config.get("grace_minutes") is not None else 30
            self._zero_only = bool(config.get("zero_only", True))
            self._max_per_round = int(config["max_per_round"]) if config.get("max_per_round") is not None else 2
            self._cooldown_hours = int(config["cooldown_hours"]) if config.get("cooldown_hours") is not None else 24
            self._retry_search = bool(config.get("retry_search", True))
            self._exclude_tags = config.get("exclude_tags") or ""

        self.stop_service()

        if self._onlyonce:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            self._scheduler.add_job(
                func=self.check_stalled,
                trigger="date",
                run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
            )
            self._onlyonce = False
            self.update_config(self._build_config())
            if self._scheduler.get_jobs():
                self._scheduler.start()
            logger.info("卡种换源：已安排立即执行一次")

    def _build_config(self) -> dict:
        """当前配置快照。"""
        return {
            "enabled": self._enabled,
            "notify": self._notify,
            "onlyonce": self._onlyonce,
            "dryrun": self._dryrun,
            "cron": self._cron,
            "downloaders": self._downloaders,
            "stall_minutes": self._stall_minutes,
            "min_speed": self._min_speed,
            "grace_minutes": self._grace_minutes,
            "zero_only": self._zero_only,
            "max_per_round": self._max_per_round,
            "cooldown_hours": self._cooldown_hours,
            "retry_search": self._retry_search,
            "exclude_tags": self._exclude_tags,
        }

    def get_state(self) -> bool:
        """插件是否处于可执行状态。"""
        return bool(self._enabled and self._cron and self._downloaders)

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """不支持远程命令。"""
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        """不对外提供接口。"""
        pass

    def get_service(self) -> List[Dict[str, Any]]:
        """注册定时扫描服务。"""
        if self.get_state() and self._cron:
            return [
                {
                    "id": "StallReseeder",
                    "name": "卡种换源服务",
                    "trigger": CronTrigger.from_crontab(self._cron),
                    "func": self.check_stalled,
                    "kwargs": {},
                }
            ]
        return []

    def stop_service(self):
        """停止内部调度器。"""
        try:
            if self._scheduler:
                if self._scheduler.running:
                    self._scheduler.shutdown()
                self._scheduler = None
        except Exception as err:  # noqa: BLE001
            logger.error(f"卡种换源：停止服务出错 {err}")

    @property
    def service_infos(self) -> Optional[Dict[str, ServiceInfo]]:
        """已连接下载器实例。"""
        if not self._downloaders:
            logger.warning("卡种换源：尚未选择下载器")
            return None
        services = DownloaderHelper().get_services(name_filters=self._downloaders)
        if not services:
            logger.warning("卡种换源：获取下载器实例失败")
            return None
        active = {}
        for name, info in services.items():
            if not info.instance:
                continue
            if info.instance.is_inactive():
                logger.warning(f"卡种换源：下载器 {name} 未连接")
                continue
            active[name] = info
        return active or None

    # ------------------------------------------------------------------ 主流程

    def check_stalled(self):
        """扫描下载器，处理 0% 死种，并记录结果供详情页展示。"""
        started_at = datetime.now()
        services = self.service_infos
        if not services:
            self._save_run(
                {
                    "time": started_at.strftime(_TIME_FMT),
                    "dryrun": self._dryrun,
                    "downloaders": [],
                    "total": 0,
                    "unfinished": 0,
                    "exempt": 0,
                    "grace": 0,
                    "tracking": 0,
                    "hit": 0,
                    "items": [],
                    "error": "没有可用的下载器（未勾选、未连接或读取失败），本次未执行判定",
                }
            )
            return

        state: Dict[str, dict] = self.get_data("stall_state") or {}
        new_state: Dict[str, dict] = {}
        exclude = {tag.strip() for tag in str(self._exclude_tags).split(",") if tag.strip()}
        now = time.time()
        handled = 0
        items: List[dict] = []
        messages: List[str] = []
        errors: List[str] = []
        stats = {"total": 0, "unfinished": 0, "exempt": 0, "grace": 0}
        stop_scan = False

        for dl_name, info in services.items():
            if stop_scan:
                break
            if info.type and str(info.type).lower() != "qbittorrent":
                logger.info(f"卡种换源：暂不支持下载器类型 {info.type}，跳过 {dl_name}")
                errors.append(f"{dl_name}：下载器类型 {info.type} 暂不支持")
                continue
            try:
                torrents, error = info.instance.get_torrents()
            except Exception as err:  # noqa: BLE001
                logger.error(f"卡种换源：读取 {dl_name} 种子失败 {err}")
                errors.append(f"{dl_name}：读取种子失败 {err}")
                continue
            if error:
                logger.warning(f"卡种换源：读取 {dl_name} 种子列表出错")
                errors.append(f"{dl_name}：读取种子列表出错")
                continue
            if not torrents:
                logger.info(f"卡种换源：{dl_name} 没有种子")
                continue
            logger.info(f"卡种换源：{dl_name} 共 {len(torrents)} 个任务，开始判定")

            for torrent in torrents:
                stats["total"] += 1
                try:
                    reason, result = self._inspect(torrent, state, new_state, exclude, now)
                except Exception as err:  # noqa: BLE001
                    logger.error(f"卡种换源：分析种子出错 {err}")
                    continue
                if reason == "done":
                    continue
                stats["unfinished"] += 1
                if reason == "exempt":
                    stats["exempt"] += 1
                    continue
                if reason == "grace":
                    stats["grace"] += 1
                    continue
                if reason != "hit" or not result:
                    continue
                if handled >= max(1, self._max_per_round):
                    logger.info("卡种换源：本轮处理数量已达上限，剩余任务留到下一轮")
                    stop_scan = True
                    break

                stall_minutes, speed_kb = result
                item = self._handle(dl_name, info.instance, torrent, stall_minutes, speed_kb)
                items.append(item)
                if item.get("handled"):
                    handled += 1
                    messages.append(f"{dl_name}：{item.get('summary') or torrent.name}")
                    new_state.pop(torrent.hash, None)

        self.save_data("stall_state", new_state)
        self._save_run(
            {
                "time": started_at.strftime(_TIME_FMT),
                "dryrun": self._dryrun,
                "downloaders": list(services.keys()),
                "total": stats["total"],
                "unfinished": stats["unfinished"],
                "exempt": stats["exempt"],
                "grace": stats["grace"],
                "tracking": len(new_state),
                "hit": handled,
                "items": items,
                "error": "；".join(errors) if errors else "",
            }
        )

        if handled and self._notify:
            self.post_message(
                mtype=NotificationType.SiteMessage,
                title=f"【卡种换源】已换源 {handled} 个任务",
                text="\n".join(messages[:30]),
            )
        elif handled:
            logger.info(f"卡种换源：本次处理 {handled} 个任务")
        else:
            logger.info("卡种换源：没有需要处理的任务")

    def _inspect(
        self,
        torrent: Any,
        state: Dict[str, dict],
        new_state: Dict[str, dict],
        exclude: set,
        now: float,
    ) -> Tuple[str, Optional[Tuple[float, float]]]:
        """判定单个任务，返回 (原因, (停滞分钟, 速度KB/s))。"""
        if torrent.progress is None or torrent.progress >= 1:
            return "done", None
        # 只处理一个字节都没下来的任务，慢速下载中的任务一律不碰
        if self._zero_only:
            downloaded = getattr(torrent, "downloaded", None)
            if downloaded is None:
                downloaded = getattr(torrent, "completed", 0) or 0
            if downloaded and float(downloaded) > 0:
                return "done", None
        tags = {str(tag).strip() for tag in str(torrent.tags or "").split(",") if str(tag).strip()}
        if exclude and tags & exclude:
            return "exempt", None
        added_on = getattr(torrent, "added_on", 0) or 0
        if added_on and (now - added_on) / 60 < self._grace_minutes:
            return "grace", None

        speed_kb = (getattr(torrent, "dlspeed", 0) or 0) / 1024
        progress = round(float(torrent.progress), 6)
        record = state.get(torrent.hash)
        if record and abs(float(record.get("progress", -1)) - progress) < 1e-6 and speed_kb <= self._min_speed:
            since = float(record.get("since", now))
        else:
            since = now

        new_state[torrent.hash] = {
            "progress": progress,
            "since": since,
            "name": torrent.name,
            "speed": round(speed_kb),
        }
        stall_minutes = (now - since) / 60
        if stall_minutes >= self._stall_minutes:
            return "hit", (stall_minutes, speed_kb)
        return "tracking", None

    def _handle(self, dl_name: str, downloader: Any, torrent: Any, stall_minutes: float, speed_kb: float) -> dict:
        """处理一个死种：定位资源 → 删任务 → 写冷却 → 触发重搜。"""
        base = {
            "name": torrent.name,
            "stall": int(stall_minutes),
            "speed": round(speed_kb),
            "progress": round(float(torrent.progress or 0) * 100, 1),
            "site": "",
            "sid": "",
            "fingerprint": "",
            "result": "",
            "handled": False,
            "summary": "",
        }

        plan = self._resolve_resource(torrent)
        base["site"] = plan.get("site_name") or ""
        base["sid"] = plan.get("sid") or ""
        base["fingerprint"] = (plan.get("fingerprint") or "")[:12]
        base["note"] = plan.get("note") or ""

        if self._dryrun:
            logger.info(
                f"卡种换源[演练]：{torrent.name}｜停滞 {int(stall_minutes)} 分钟｜"
                f"站点 {plan.get('site_name') or '未知'}｜订阅 {plan.get('sid') or '未匹配'}｜"
                f"指纹 {(plan.get('fingerprint') or '未取到')[:16]}｜{plan.get('note') or ''}"
            )
            base["result"] = "演练（未处理）"
            base["summary"] = f"[演练] {torrent.name}"
            return base

        if plan.get("sid"):
            self._bump_repeat(plan)

        if not self._apply_delete(downloader, torrent.hash):
            logger.error(f"卡种换源：删除任务失败 {torrent.name}")
            base["result"] = "删除失败"
            return base

        recorded = False
        if plan.get("fingerprint"):
            recorded = self._record_cooldown(plan, torrent, dl_name, stall_minutes)

        searched = False
        if self._retry_search and plan.get("sid"):
            searched = self._trigger_search(plan.get("sid"), plan.get("title") or torrent.name)

        parts = ["已删任务"]
        parts.append("已写冷却" if recorded else "未写冷却")
        parts.append("已触发重搜" if searched else "未触发重搜")
        base["result"] = "、".join(parts)
        base["handled"] = True
        base["summary"] = (
            f"{torrent.name}｜{base['result']}｜站点 {base['site'] or '未知'}｜订阅 {base['sid'] or '未匹配'}"
        )
        return base

    # -------------------------------------------------------------- 资源定位

    def _resolve_resource(self, torrent: Any) -> dict:
        """反查这个死种对应的媒体、订阅与资源页面。"""
        plan: dict = {"sid": None, "site_name": "", "site_id": None, "fingerprint": "", "note": ""}
        history = self._history_of(torrent.hash)
        if not history:
            plan["note"] = "下载历史里找不到这个 hash，只能删任务"
            return plan

        plan["title"] = history.title or torrent.name
        plan["year"] = history.year
        plan["media_type"] = history.type
        plan["seasons"] = history.seasons
        plan["media_source"] = getattr(history.media_source, "value", history.media_source)
        plan["media_id"] = history.media_id
        plan["site_name"] = history.torrent_site or ""
        plan["sid"] = self._subscription_id(plan)
        if plan["sid"] is None:
            plan["note"] = "没匹配到订阅，只能删任务"

        site_id = self._site_id_by_name(plan["site_name"]) if plan["site_name"] else None
        plan["site_id"] = site_id
        if not site_id:
            plan["note"] = plan["note"] or "找不到来源站点，跳过冷却指纹"
            return plan

        context = self._find_context(plan["title"], site_id, torrent.name, history.torrent_name)
        if not context:
            plan["note"] = (plan["note"] + "；" if plan["note"] else "") + "站点上没搜到这颗种子，退回只删不排除"
            return plan

        # 标题搜索不识别媒体信息，这里补上订阅那边的媒体身份，指纹才能和订阅搜索对齐
        self._attach_media(context, plan)
        plan["context"] = context
        plan["fingerprint"] = self._fingerprint(context) or ""
        if not plan["fingerprint"]:
            plan["note"] = (plan["note"] + "；" if plan["note"] else "") + "指纹计算失败，退回只删不排除"
        return plan

    @staticmethod
    def _history_of(torrent_hash: str):
        """按 hash 查下载历史。"""
        try:
            from app.sdk.queries import list_download_history

            page = list_download_history({"download_hash": torrent_hash}, {"page": 1, "count": 5})
        except Exception as err:  # noqa: BLE001
            logger.error(f"卡种换源：查询下载历史失败 {err}")
            return None
        items = list(getattr(page, "items", None) or [])
        for item in items:
            if str(getattr(item, "download_hash", "") or "").lower() == str(torrent_hash).lower():
                return item
        return items[0] if items else None

    @staticmethod
    def _subscription_id(plan: dict) -> Optional[int]:
        """用媒体身份定位订阅。"""
        source, media_id = plan.get("media_source"), plan.get("media_id")
        if not source or not media_id:
            return None
        filters: Dict[str, Any] = {"media_source": source, "media_id": str(media_id)}
        season = _parse_season(plan.get("seasons"))
        if season is not None:
            filters["season"] = season
        try:
            from app.sdk.queries import list_subscriptions

            page = list_subscriptions(filters, {"page": 1, "count": 20})
        except Exception as err:  # noqa: BLE001
            logger.error(f"卡种换源：查询订阅失败 {err}")
            return None
        items = list(getattr(page, "items", None) or [])
        for item in items:
            if getattr(item, "state", "") == "R":
                return getattr(item, "id", None)
        return getattr(items[0], "id", None) if items else None

    @staticmethod
    def _site_id_by_name(name: str) -> Optional[int]:
        """站点名转 ID。"""
        try:
            from app.db.models.site import Site
            from app.db.session import SessionFactory

            with SessionFactory() as db:
                for site in Site.list_order_by_pri(db) or []:
                    if site.name == name:
                        return site.id
        except Exception as err:  # noqa: BLE001
            logger.error(f"卡种换源：解析站点 ID 失败 {err}")
        return None

    @staticmethod
    def _find_context(title: str, site_id: int, torrent_name: str, history_name: Optional[str]):
        """在原站点重搜同一发布，拿到可用于计算指纹的资源上下文。"""
        try:
            from app.chain.search import SearchChain

            contexts = SearchChain().search_by_title(title=title, sites=[site_id], cache_local=False) or []
        except Exception as err:  # noqa: BLE001
            logger.error(f"卡种换源：站点搜索失败 {err}")
            return None
        wanted = [name for name in (history_name, torrent_name) if name]
        for context in contexts:
            info = getattr(context, "torrent_info", None)
            if not info:
                continue
            for name in wanted:
                if _same_release(getattr(info, "title", ""), name):
                    logger.info(f"卡种换源：站点搜索命中 {len(contexts)} 个资源，已匹配到同一发布")
                    return context
        logger.info(f"卡种换源：站点搜索返回 {len(contexts)} 个资源，未匹配到同一发布")
        return None

    @staticmethod
    def _attach_media(context: Any, plan: dict) -> None:
        """给标题搜索的结果补上订阅侧的媒体身份。

        ``search_by_title`` 只按标题抓站点列表，不识别媒体，``media_info`` 是空的；
        而冷却指纹必须带媒体身份才能和订阅搜索算出的指纹对上，所以用下载历史里
        已有的身份回填。
        """
        if getattr(context, "media_info", None):
            return
        try:
            from app.domain.context import MediaInfo
            from app.schemas.types import MediaSource, MediaType

            media_type = MediaType.TV if str(plan.get("media_type") or "") == "电视剧" else MediaType.MOVIE
            source = plan.get("media_source")
            try:
                source = MediaSource(str(source)) if source else None
            except Exception:  # noqa: BLE001
                source = None
            season = _parse_season(plan.get("seasons"))
            context.media_info = MediaInfo(
                media_source=source,
                media_id=str(plan.get("media_id") or "") or None,
                type=media_type,
                title=plan.get("title"),
                year=plan.get("year"),
                season=season if media_type == MediaType.TV else None,
            )
        except Exception as err:  # noqa: BLE001
            logger.error(f"卡种换源：回填媒体身份失败 {err}")

    @staticmethod
    def _fingerprint(context: Any) -> Optional[str]:
        """用 MP 自己的算法计算资源冷却指纹。"""
        try:
            from app.chain.download import DownloadChain

            return DownloadChain._build_download_failure_fingerprint(  # noqa: SLF001
                context, resource_backoff=True
            )
        except Exception as err:  # noqa: BLE001
            logger.error(f"卡种换源：计算冷却指纹失败 {err}")
            return None

    # -------------------------------------------------------------- 执行动作

    @staticmethod
    def _apply_delete(downloader: Any, torrent_hash: str) -> bool:
        """删除任务，保留文件（0% 本来没有文件）。"""
        try:
            return bool(downloader.delete_torrents(delete_file=False, ids=[torrent_hash]))
        except Exception as err:  # noqa: BLE001
            logger.error(f"卡种换源：处理种子 {torrent_hash} 失败 {err}")
            return False

    def _record_cooldown(self, plan: dict, torrent: Any, dl_name: str, stall_minutes: float) -> bool:
        """把死种写进 MP 原生下载失败台账，让订阅搜索主动跳过它。"""
        context = plan.get("context")
        info = getattr(context, "torrent_info", None) if context else None
        resource = ""
        try:
            from app.chain.download import DownloadChain

            resource = DownloadChain._torrent_resource_key(info) or ""  # noqa: SLF001
        except Exception:  # noqa: BLE001
            resource = ""
        now = time.time()
        try:
            from app.db.oper.downloadfailure import DownloadFailureOper

            DownloadFailureOper().record_failure(
                fingerprint=plan["fingerprint"],
                now_time=time.strftime(_TIME_FMT, time.localtime(now)),
                next_retry_at=time.strftime(
                    _TIME_FMT, time.localtime(now + max(1, self._cooldown_hours) * 3600)
                ),
                type=plan.get("media_type"),
                title=plan.get("title"),
                year=plan.get("year"),
                media_source=plan.get("media_source"),
                media_id=str(plan.get("media_id") or "") or None,
                seasons=plan.get("seasons"),
                episodes=None,
                site=plan.get("site_id"),
                site_name=plan.get("site_name"),
                torrent_id=resource or None,
                torrent_name=getattr(info, "title", None) or torrent.name,
                torrent_size=float(getattr(info, "size", 0) or 0) or None,
                downloader=dl_name,
                source="Subscribe|stallreseeder",
                error_message=f"死种：0% 停滞 {int(stall_minutes)} 分钟，占位不下数据，已由卡种换源删除",
            )
            logger.info(
                f"卡种换源：已写冷却 {plan['fingerprint'][:16]}｜{torrent.name}｜{self._cooldown_hours} 小时"
            )
            return True
        except Exception as err:  # noqa: BLE001
            logger.error(f"卡种换源：写冷却失败 {err}")
            return False

    def _trigger_search(self, sid: Any, title: str) -> bool:
        """定向触发某个订阅的重新搜索（后台线程，避免阻塞扫描）。"""
        try:
            subscribe_id = int(sid)
        except (TypeError, ValueError):
            return False

        def _run():
            try:
                from app.chain.subscribe import SubscribeChain

                batch = SubscribeChain().search(sids=(subscribe_id,), state=None, manual=True)
                logger.info(f"卡种换源：订阅 {subscribe_id} 已重新搜索，批次 {batch}")
            except Exception as err:  # noqa: BLE001
                logger.error(f"卡种换源：订阅 {subscribe_id} 重新搜索失败 {err}")

        _EXECUTOR.submit(_run)
        logger.info(f"卡种换源：已排队触发订阅 {subscribe_id} 重新搜索｜{title}")
        return True

    def _bump_repeat(self, plan: dict):
        """记录同一资源被反复选中的次数，便于提醒用户排除关键词。"""
        try:
            repeats: Dict[str, dict] = self.get_data("repeat") or {}
            key = str(plan.get("title") or "")
            if not key:
                return
            record = repeats.get(key) or {"count": 0, "last": ""}
            record["count"] = int(record.get("count") or 0) + 1
            record["last"] = datetime.now().strftime(_TIME_FMT)
            record["site"] = plan.get("site_name")
            repeats[key] = record
            # 只保留最近 50 条
            if len(repeats) > 50:
                ordered = sorted(repeats.items(), key=lambda kv: kv[1].get("last") or "", reverse=True)[:50]
                repeats = dict(ordered)
            self.save_data("repeat", repeats)
        except Exception as err:  # noqa: BLE001
            logger.error(f"卡种换源：记录重复次数失败 {err}")

    def _save_run(self, record: dict):
        """保存本次结果，并追加进历史（最多 30 次）。"""
        try:
            self.save_data("last_run", record)
            history = self.get_data("history") or []
            if not isinstance(history, list):
                history = []
            history.insert(0, record)
            self.save_data("history", history[:30])
        except Exception as err:  # noqa: BLE001
            logger.error(f"卡种换源：保存执行记录失败 {err}")

    # -------------------------------------------------------------- 面板

    def get_page(self) -> List[dict]:
        """插件详情页。"""
        try:
            return self._build_page()
        except Exception as err:  # noqa: BLE001
            logger.error(f"卡种换源：渲染详情页失败 {err}")
            return [
                {
                    "component": "VAlert",
                    "props": {"type": "error", "variant": "tonal", "text": f"面板渲染失败：{err}"},
                }
            ]

    @staticmethod
    def _stat_card(title: str, value: str, subtitle: str = "", color: str = "primary") -> dict:
        """一行小统计卡。"""
        content: List[dict] = [
            {"component": "div", "props": {"class": "text-caption text-medium-emphasis"}, "text": title},
            {"component": "div", "props": {"class": "text-h6 font-weight-bold"}, "text": value},
        ]
        if subtitle:
            content.append(
                {"component": "div", "props": {"class": "text-caption text-medium-emphasis"}, "text": subtitle}
            )
        return {
            "component": "VCard",
            "props": {"variant": "tonal", "color": color, "class": "h-100"},
            "content": [{"component": "VCardText", "props": {"class": "py-3"}, "content": content}],
        }

    @staticmethod
    def _section(title: str, children: List[dict], subtitle: str = "") -> dict:
        """带标题的区块卡片。"""
        head: List[dict] = [
            {"component": "VCardTitle", "props": {"class": "text-subtitle-1 pb-0"}, "text": title}
        ]
        if subtitle:
            head.append(
                {
                    "component": "div",
                    "props": {"class": "text-caption text-medium-emphasis px-4 pb-2"},
                    "text": subtitle,
                }
            )
        return {
            "component": "VCard",
            "props": {"variant": "outlined", "class": "mb-3"},
            "content": head + [{"component": "VCardText", "props": {"class": "pt-2"}, "content": children}],
        }

    @staticmethod
    def _table(headers: List[str], rows: List[List[Any]]) -> dict:
        """紧凑表格。"""
        return {
            "component": "VTable",
            "props": {"hover": True, "density": "compact"},
            "content": [
                {
                    "component": "thead",
                    "content": [
                        {
                            "component": "tr",
                            "content": [
                                {
                                    "component": "th",
                                    "props": {"class": "text-start text-caption font-weight-bold"},
                                    "text": header,
                                }
                                for header in headers
                            ],
                        }
                    ],
                },
                {
                    "component": "tbody",
                    "content": [
                        {
                            "component": "tr",
                            "content": [
                                {
                                    "component": "td",
                                    "props": {"class": "text-caption"},
                                    "text": str(cell),
                                }
                                for cell in row
                            ],
                        }
                        for row in rows
                    ],
                },
            ],
        }

    def _build_page(self) -> List[dict]:
        """拼装面板内容。"""
        last: dict = self.get_data("last_run") or {}
        history: List[dict] = self.get_data("history") or []
        state: Dict[str, dict] = self.get_data("stall_state") or {}
        repeats: Dict[str, dict] = self.get_data("repeat") or {}
        now = time.time()

        running = self.get_state()
        cards = [
            self._stat_card(
                "运行状态",
                "运行中" if running else "已停止",
                f"周期 {self._cron}" if self._cron else "未设置执行周期",
                "success" if running else "warning",
            ),
            self._stat_card(
                "运行模式",
                "演练（只记录）" if self._dryrun else "正式执行",
                f"停滞阈值 {self._stall_minutes} 分钟",
                "info" if self._dryrun else "primary",
            ),
            self._stat_card(
                "冷却时长",
                f"{self._cooldown_hours} 小时",
                f"每轮最多处理 {self._max_per_round} 个",
                "secondary",
            ),
            self._stat_card(
                "上次换源",
                f"{int(last.get('hit') or 0)} 个",
                last.get("time") or "尚未执行过",
                "error" if last.get("hit") else "success",
            ),
        ]

        blocks: List[dict] = [
            {
                "component": "VRow",
                "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 3}, "content": [card]}
                    for card in cards
                ],
            }
        ]

        if last:
            rows = [
                ["扫描任务", last.get("total", 0)],
                ["未完成任务", last.get("unfinished", 0)],
                ["豁免标签", last.get("exempt", 0)],
                ["保护期内", last.get("grace", 0)],
                ["跟踪中（未达阈值）", last.get("tracking", 0)],
                ["本次换源", last.get("hit", 0)],
                ["下载器", "、".join(last.get("downloaders") or []) or "无"],
            ]
            body: List[dict] = [self._table(["项目", "数值"], rows)]
            if last.get("error"):
                body.insert(
                    0,
                    {
                        "component": "VAlert",
                        "props": {
                            "type": "warning",
                            "variant": "tonal",
                            "density": "compact",
                            "class": "mb-2",
                            "text": str(last["error"]),
                        },
                    },
                )
            blocks.append(
                self._section(
                    "上次执行概况",
                    body,
                    f"{last.get('time', '-')} · {'演练' if last.get('dryrun') else '正式执行'}",
                )
            )

            items = last.get("items") or []
            if items:
                detail = [
                    [
                        (item.get("name") or "")[:48],
                        f"{item.get('stall', 0)} 分钟",
                        item.get("site") or "-",
                        item.get("sid") or "-",
                        item.get("fingerprint") or "-",
                        item.get("result", "-"),
                    ]
                    for item in items[:30]
                ]
                blocks.append(
                    self._section(
                        "上次明细",
                        [self._table(["任务", "停滞", "站点", "订阅", "指纹", "处置"], detail)],
                        f"共 {len(items)} 条；指纹命中说明已写进 MP 冷却台账",
                    )
                )

        tracking: List[tuple] = []
        for record in (state or {}).values():
            if not isinstance(record, dict):
                continue
            try:
                progress = float(record.get("progress") or 0)
                since = float(record.get("since") or now)
            except (TypeError, ValueError):
                continue
            if progress >= 1:
                continue
            tracking.append(((now - since) / 60, record))
        tracking.sort(key=lambda pair: -pair[0])

        if tracking:
            rows = [
                [
                    (record.get("name") or "")[:52],
                    f"{int(minutes)} 分钟",
                    f"{round(float(record.get('progress') or 0) * 100, 1)}%",
                    f"{int(record.get('speed') or 0)} KB/s",
                    "已达阈值" if minutes >= self._stall_minutes else "观察中",
                ]
                for minutes, record in tracking[:15]
            ]
            blocks.append(
                self._section(
                    "当前 0% 任务",
                    [self._table(["任务", "持续无进度", "进度", "速度", "状态"], rows)],
                    f"共 {len(tracking)} 个；达到 {self._stall_minutes} 分钟后删除并换源"
                    + ("（演练模式不会真删）" if self._dryrun else ""),
                )
            )

        if repeats:
            rows = [
                [
                    (name or "")[:40],
                    record.get("count", 0),
                    record.get("site") or "-",
                    record.get("last") or "-",
                ]
                for name, record in sorted(
                    repeats.items(), key=lambda kv: int(kv[1].get("count") or 0), reverse=True
                )[:10]
            ]
            blocks.append(
                self._section(
                    "重复中招的媒体",
                    [self._table(["媒体", "触发次数", "站点", "最近一次"], rows)],
                    "同一媒体反复触发，说明换源后仍挑到死种；可考虑在订阅里排除对应发布组关键词",
                )
            )

        if history:
            rows = [
                [
                    record.get("time", "-"),
                    "演练" if record.get("dryrun") else "正式",
                    record.get("total", 0),
                    record.get("hit", 0),
                    ((record.get("items") or [{}])[0].get("name") or "")[:34] if record.get("hit") else "-",
                ]
                for record in history[:12]
            ]
            blocks.append(
                self._section(
                    "执行历史",
                    [self._table(["时间", "模式", "扫描", "换源", "最近一条"], rows)],
                    f"最近 {min(len(history), 12)} 次（最多保留 30 次）",
                )
            )

        return blocks

    # -------------------------------------------------------------- 表单

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """插件配置表单。"""
        downloader_items = [
            {"title": cfg.name, "value": cfg.name}
            for cfg in (DownloaderHelper().get_configs() or {}).values()
        ]
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {"component": "VSwitch", "props": {"model": "enabled", "label": "启用插件"}}
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {"component": "VSwitch", "props": {"model": "notify", "label": "发送通知"}}
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {"component": "VSwitch", "props": {"model": "onlyonce", "label": "立即运行一次"}}
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "dryrun", "label": "演练模式（只记录不处理）"},
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VCronField",
                                        "props": {
                                            "model": "cron",
                                            "label": "执行周期",
                                            "placeholder": "*/15 * * * *",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "multiple": True,
                                            "chips": True,
                                            "clearable": True,
                                            "model": "downloaders",
                                            "label": "下载器",
                                            "items": downloader_items,
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "stall_minutes",
                                            "label": "停滞判定时长（分钟）",
                                            "placeholder": "30",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "min_speed",
                                            "label": "低于此速度视为无速（KB/s）",
                                            "placeholder": "10",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "grace_minutes",
                                            "label": "新任务保护期（分钟）",
                                            "placeholder": "30",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "max_per_round",
                                            "label": "每轮最多换源（个）",
                                            "placeholder": "2",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "cooldown_hours",
                                            "label": "死种冷却时长（小时）",
                                            "placeholder": "24",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "zero_only", "label": "只处理 0% 任务"},
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "retry_search", "label": "删除后触发订阅重搜"},
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "exclude_tags",
                                            "label": "豁免标签（逗号分隔）",
                                            "placeholder": "刷流,MOVIEPILOT",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "text": "判定逻辑：任务进度连续 N 分钟无变化、速度低于阈值、且一个字节都没下载才算死种；"
                                            "新任务保护期内不处理。命中后会删除任务（0% 无文件）、把该资源写进 MP 的下载失败冷却台账，"
                                            "并定向触发所属订阅重新搜索，由 MP 自己挑下一个资源。建议先用演练模式跑一轮看名单。",
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                ],
            }
        ], {
            "enabled": False,
            "notify": True,
            "onlyonce": False,
            "dryrun": True,
            "cron": "*/15 * * * *",
            "downloaders": [],
            "stall_minutes": 30,
            "min_speed": 10,
            "grace_minutes": 30,
            "zero_only": True,
            "max_per_round": 2,
            "cooldown_hours": 24,
            "retry_search": True,
            "exclude_tags": "刷流",
        }
