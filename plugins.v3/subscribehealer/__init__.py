# -*- coding: utf-8 -*-
"""订阅缺集自愈。

MoviePilot 在**提交下载**时就把集号写进订阅的 note 与 episode_priority，之后种子被删、下载失败、
文件被清理都不会回滚。对开启洗版的订阅，这些"已下载"记录会让缺失集变成"洗版候选"，只有更高优先级的
资源才会被接受，于是站点上明明有资源却永远补不下来。

本插件以媒体库实际文件为唯一事实，双向对账：
1. **声明有、库里没有** → 剔除记录，让本次搜索直接补这些集；
2. **库里有、声明没有** → 补进 note，避免把已入库的集当成缺集反复下载；
3. **库里有、优先级未知** → 先按历史快照恢复，再按同订阅已知优先级推断，最后才锁定，
   避免"未知即 0"被当成"任何资源都能洗版"，从而整季重下。

搜索前钩子挂在内核真正的单订阅搜索入口（_search_subscription），手动、定时、补搜全部经过它；
核对失败时默认跳过本轮搜索而不是带着脏数据去下载（strict_guard）。

判定保守：媒体库已有该集 → 跳过；种子还在下载器 / 下载记录指向的**该集视频文件**仍在磁盘 / 记录未过保护期
→ 视为在途；只有确认种子没了、文件也没了，才认定记录失效。
注意：下载历史的 path 多数是种子目录，目录本身长期存在，不能用 exists 判断"文件还在"。
"""

import inspect
import json
import os
import re
import threading
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

from apscheduler.triggers.cron import CronTrigger

from app.db.oper.downloadhistory import DownloadHistoryOper
from app.db.oper.subscribe import SubscribeOper
from app.domain.meta.metavideo import MetaVideo
from app.schemas import NotificationType
from app.schemas.types import MediaSource
from app.sdk.logging import logger
from app.sdk.plugin import _PluginBase
from app.sdk.services import DownloaderHelper

VIDEO_EXT = {
    ".mp4", ".mkv", ".ts", ".m2ts", ".avi", ".mov", ".wmv",
    ".flv", ".rmvb", ".webm", ".iso", ".mpg", ".mpeg", ".m4v",
}
SEASON_DIR_RE = re.compile(r"(?:season|s)\s*0*(\d{1,2})", re.I)
SEASON_WORD_RE = re.compile(r"第\s*(\d{1,2})\s*季")
EPISODE_RE = re.compile(r"E(\d{1,4})(?:\s*-\s*E?(\d{1,4}))?", re.I)
SKIP_TOP_DIRS = {"downloads", "incomplete", "temp", "mt-brush", ".recycle", "lost+found", "recycle", "@eadir"}

# 搜索前钩子的全局状态：插件重载时复用同一份，避免重复包装
_HOOK: Dict[str, Any] = {"plugin": None, "originals": {}}
_TLS = threading.local()


class SubscribeHealer(_PluginBase):
    """订阅缺集自愈。"""

    plugin_name = "订阅缺集自愈"
    plugin_desc = "订阅搜索前双向核对媒体库与已下载记录：剔除失效声明、补齐库内事实、修复未知优先级，解决缺集发现不了与整季重下。"
    plugin_icon = "https://raw.githubusercontent.com/huanqi666/MoviePilot-Plugins/main/icons/subscribehealer.svg"
    plugin_version = "2.0.0"
    plugin_author = "huanqi666"
    author_url = "https://github.com/huanqi666"
    plugin_config_prefix = "subscribehealer_"
    plugin_order = 31
    auth_level = 1

    _enabled: bool = False
    _notify: bool = False
    _onlyonce: bool = False
    _dryrun: bool = False
    _auto_search: bool = True
    _heal_before_search: bool = True
    _strict_guard: bool = True
    _unknown_policy: str = "infer"
    _cron: Optional[str] = None
    _states: List[str] = ["R", "P"]
    _min_age_minutes: int = 120
    _max_actions: int = 200
    _min_file_mb: int = 10
    _max_clear_per_sub: int = 30
    _library_roots: str = ""
    _cache_ttl: int = 600
    _cache: Dict[int, Tuple[float, Set[int], Set[int]]] = {}
    _facts: Optional[Dict[str, Any]] = None

    def init_plugin(self, config: dict = None):
        """读取配置并安装搜索前钩子。"""
        if config:
            self._enabled = bool(config.get("enabled"))
            self._notify = bool(config.get("notify"))
            self._onlyonce = bool(config.get("onlyonce"))
            self._dryrun = bool(config.get("dryrun"))
            self._auto_search = config.get("auto_search", True) is not False
            self._heal_before_search = config.get("heal_before_search", True) is not False
            self._strict_guard = config.get("strict_guard", True) is not False
            policy = str(config.get("unknown_policy") or "infer").strip().lower()
            self._unknown_policy = policy if policy in {"infer", "lock", "keep"} else "infer"
            self._cron = config.get("cron")
            states = config.get("states")
            if isinstance(states, str):
                states = [s.strip() for s in states.split(",") if s.strip()]
            self._states = [str(s).upper() for s in (states or ["R", "P"])]
            self._min_age_minutes = self._as_int(config.get("min_age_minutes"), 120, 0, 10080)
            self._max_actions = self._as_int(config.get("max_actions"), 200, 1, 5000)
            self._min_file_mb = self._as_int(config.get("min_file_mb"), 10, 0, 10240)
            self._max_clear_per_sub = self._as_int(config.get("max_clear_per_sub"), 30, 1, 500)
            self._cache_ttl = self._as_int(config.get("cache_ttl"), 600, 0, 86400)
            self._library_roots = str(config.get("library_roots") or "").strip()

        self._cache = {}
        self._facts = None
        _HOOK["plugin"] = self
        if self._enabled and self._heal_before_search:
            self._install_hook()
        else:
            self._remove_hook()

        if self._onlyonce:
            self._onlyonce = False
            self.update_config(self._build_config())
            try:
                self._run_once_async()
            except Exception as err:  # noqa: BLE001
                logger.error(f"订阅缺集自愈：安排立即执行失败 {err}")

    # ------------------------------------------------------------------ 钩子

    @staticmethod
    def _unwrap(func):
        """剥掉历史包装器，拿到真正的原始入口。

        插件热重载后，类上可能还挂着旧实例留下的包装器；旧包装器读的是旧模块的
        全局状态，会把新逻辑整个旁路掉，所以必须找到原始入口重新包一层。
        """
        for _ in range(5):
            if not getattr(func, "_subscribehealer_hooked", False):
                return func
            candidate = None
            for cell in getattr(func, "__closure__", None) or ():
                try:
                    value = cell.cell_contents
                except ValueError:
                    continue
                if callable(value) and not getattr(value, "_subscribehealer_hooked", False):
                    module = getattr(value, "__module__", "") or ""
                    if module.startswith("app."):
                        candidate = value
                        break
            if candidate is None:
                return None
            func = candidate
        return func

    def _install_hook(self):
        """包装订阅搜索入口，让每次搜索前先核对一次。

        队列模式下 MP 会经由 _search_subscription 执行单订阅搜索，手动、定时、
        补搜、新订阅全部走这里，因此以它为主入口，search 只作为批次级兜底。
        """
        try:
            from app.chain.subscribe import SubscribeChain

            originals = _HOOK.setdefault("originals", {})
            raw_search = getattr(SubscribeChain, "_sh_raw_search", None) or self._unwrap(
                getattr(SubscribeChain, "search", None)
            )
            raw_single = getattr(SubscribeChain, "_sh_raw_single", None) or self._unwrap(
                getattr(SubscribeChain, "_search_subscription", None)
            )
            if raw_search is None or raw_single is None:
                logger.error("订阅缺集自愈：找不到订阅搜索原始入口，钩子未安装")
                return
            SubscribeChain._sh_raw_search = raw_search  # type: ignore[attr-defined]
            SubscribeChain._sh_raw_single = raw_single  # type: ignore[attr-defined]
            originals["search"] = raw_search
            originals["single"] = raw_single

            def hooked_search(chain_self, *args, **kwargs):
                plugin = _HOOK.get("plugin")
                if plugin is not None and not getattr(_TLS, "running", False):
                    _TLS.running = True
                    try:
                        bound = inspect.signature(raw_search).bind(chain_self, *args, **kwargs)
                        bound.apply_defaults()
                        plugin._heal_for_search(
                            sid=bound.arguments.get("sid"),
                            sids=bound.arguments.get("sids"),
                            state=bound.arguments.get("state"),
                        )
                    except Exception as err:  # noqa: BLE001
                        logger.error(f"订阅缺集自愈：搜索前核对失败 {err}")
                    finally:
                        _TLS.running = False
                return raw_search(chain_self, *args, **kwargs)

            hooked_search._subscribehealer_hooked = True  # type: ignore[attr-defined]
            hooked_search._sh_v = self.plugin_version  # type: ignore[attr-defined]
            SubscribeChain.search = hooked_search

            def hooked_single(chain_self, subscribe, *args, **kwargs):
                plugin = _HOOK.get("plugin")
                if plugin is not None and not getattr(_TLS, "single", False):
                    _TLS.single = True
                    try:
                        state = plugin._heal_subscribe(subscribe)
                        if state is None and plugin._strict_guard:
                            logger.error(
                                f"订阅缺集自愈：{getattr(subscribe, 'name', '?')} 搜索前核对失败，"
                                f"本轮跳过搜索，避免用失效记录触发下载"
                            )
                            return subscribe
                        refreshed = chain_self.subscription_repository.get(subscribe.id)
                        if refreshed is not None:
                            subscribe = refreshed
                    except Exception as err:  # noqa: BLE001
                        logger.error(f"订阅缺集自愈：单订阅搜索前核对失败 {err}")
                        if plugin is not None and plugin._strict_guard:
                            return subscribe
                    finally:
                        _TLS.single = False
                return raw_single(chain_self, subscribe, *args, **kwargs)

            hooked_single._subscribehealer_hooked = True  # type: ignore[attr-defined]
            hooked_single._sh_v = self.plugin_version  # type: ignore[attr-defined]
            SubscribeChain._search_subscription = hooked_single

            logger.info(f"订阅缺集自愈：已挂载订阅搜索前核对钩子 v{self.plugin_version}")
        except Exception as err:  # noqa: BLE001
            logger.error(f"订阅缺集自愈：挂载钩子失败 {err}")

    def _remove_hook(self):
        try:
            from app.chain.subscribe import SubscribeChain

            originals = _HOOK.get("originals") or {}
            single = getattr(SubscribeChain, "_sh_raw_single", None) or originals.get("single")
            if single and getattr(getattr(SubscribeChain, "_search_subscription", None), "_subscribehealer_hooked", False):
                SubscribeChain._search_subscription = single
            search = getattr(SubscribeChain, "_sh_raw_search", None) or originals.get("search")
            if search and getattr(getattr(SubscribeChain, "search", None), "_subscribehealer_hooked", False):
                SubscribeChain.search = search
            if single or search:
                logger.info("订阅缺集自愈：已卸载订阅搜索前核对钩子")
        except Exception as err:  # noqa: BLE001
            logger.warning(f"订阅缺集自愈：卸载钩子失败 {err}")

    @staticmethod
    def _is_tv(subscribe) -> bool:
        """兼容字符串与枚举两种 type 表示，避免整条链路被静默跳过。"""
        value = getattr(subscribe, "type", None)
        value = getattr(value, "value", value)
        text = str(value or "").strip()
        return text in {"电视剧", "TV", "tv"} or text.startswith("MediaType.TV")

    def _heal_subscribe(self, subscribe) -> Optional[bool]:
        """单个订阅搜索前核对；True/False 表示完成，None 表示核对失败。"""
        if not self._enabled or not self._heal_before_search:
            return False
        if subscribe is None or not self._is_tv(subscribe):
            return False
        try:
            active = self._active_hashes()
            changed, count = self._check_one(
                subscribe, active, datetime.now(), self._max_actions, trigger_search=False
            )
        except Exception as err:  # noqa: BLE001
            logger.error(f"订阅缺集自愈：搜索前核对 {getattr(subscribe, 'name', '?')} 失败 {err}")
            return None
        if changed:
            logger.info(f"订阅缺集自愈：搜索前核对修复 {getattr(subscribe, 'name', '?')} {count} 集事实")
        return changed

    def _heal_for_search(self, sid=None, sids=None, state=None):
        """搜索前核对：只处理本次要搜索的订阅。"""
        if not self._enabled or not self._heal_before_search or self._dryrun:
            return
        subs = self._target_subscribes(sid=sid, sids=sids, state=state)
        if not subs:
            return
        active = self._active_hashes()
        now = datetime.now()
        budget = self._max_actions
        healed = cleared = 0
        for subscribe in subs:
            if budget <= 0:
                break
            try:
                changed, count = self._check_one(subscribe, active, now, budget, trigger_search=False)
            except Exception as err:  # noqa: BLE001
                logger.error(f"订阅缺集自愈：搜索前核对 {getattr(subscribe, 'name', '?')} 失败 {err}")
                continue
            if changed:
                healed += 1
                cleared += count
                budget -= count
        if cleared:
            logger.info(f"订阅缺集自愈：搜索前核对清理 {cleared} 集失效记录，涉及 {healed} 个订阅")

    @staticmethod
    def _target_subscribes(sid=None, sids=None, state=None) -> List[Any]:
        oper = SubscribeOper()
        try:
            if sid:
                item = oper.get(int(sid))
                return [item] if item else []
            if sids:
                result = []
                for one in sids:
                    try:
                        item = oper.get(int(one))
                    except (TypeError, ValueError):
                        continue
                    if item:
                        result.append(item)
                return result
            return list(oper.list(state=str(state or "R")) or [])
        except Exception as err:  # noqa: BLE001
            logger.warning(f"订阅缺集自愈：读取待搜索订阅失败 {err}")
            return []

    # ------------------------------------------------------------------ 配置

    @staticmethod
    def _as_int(value: Any, default: int, low: int, high: int) -> int:
        try:
            number = int(float(value))
        except (TypeError, ValueError):
            return default
        return max(low, min(high, number))

    def _build_config(self) -> dict:
        return {
            "enabled": self._enabled,
            "notify": self._notify,
            "onlyonce": False,
            "dryrun": self._dryrun,
            "auto_search": self._auto_search,
            "heal_before_search": self._heal_before_search,
            "strict_guard": self._strict_guard,
            "unknown_policy": self._unknown_policy,
            "cron": self._cron,
            "states": self._states,
            "min_age_minutes": self._min_age_minutes,
            "max_actions": self._max_actions,
            "min_file_mb": self._min_file_mb,
            "max_clear_per_sub": self._max_clear_per_sub,
            "cache_ttl": self._cache_ttl,
            "library_roots": self._library_roots,
        }

    def get_state(self) -> bool:
        return bool(self._enabled)

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    def get_service(self) -> List[Dict[str, Any]]:
        """每日兜底巡检。"""
        if not self._enabled or not self._cron:
            return []
        return [
            {
                "id": "SubscribeHealer",
                "name": "订阅缺集自愈服务",
                "trigger": CronTrigger.from_crontab(self._cron),
                "func": self.check,
                "kwargs": {},
            }
        ]

    def stop_service(self):
        """插件停用/重载时卸掉钩子。"""
        if not self._enabled:
            self._remove_hook()

    def _run_once_async(self):
        thread = threading.Thread(target=self.check, name="SubscribeHealerOnce", daemon=True)
        thread.start()
        logger.info("订阅缺集自愈：已安排立即执行一次")

    # ------------------------------------------------------------------ 扫描

    @staticmethod
    def _norm(text: Any) -> str:
        """归一化名称：仅保留字母数字与汉字，忽略空格、标点与大小写。"""
        return re.sub(r"[^0-9a-z\u4e00-\u9fff]", "", str(text or "").lower())

    @property
    def _lib_roots(self) -> List[str]:
        if self._library_roots:
            items = [item.strip() for item in re.split(r"[,;\n]", self._library_roots) if item.strip()]
            if items:
                return items
        roots: List[str] = []
        base = "/media"
        try:
            for entry in os.listdir(base):
                if entry.lower() in SKIP_TOP_DIRS or entry.startswith("."):
                    continue
                path = os.path.join(base, entry)
                if os.path.isdir(path):
                    roots.append(path)
        except OSError:
            pass
        if os.path.isdir(base):
            roots.append(base)
        return roots

    def _library_ready(self) -> bool:
        """媒体库是否可读；挂载失效时禁止把全部记录判成失效。"""
        roots = [root for root in self._lib_roots if os.path.isdir(root)]
        if not roots:
            logger.error("订阅缺集自愈：媒体库根目录不可读，本轮不判定任何失效记录")
            return False
        for root in roots:
            try:
                with os.scandir(root) as entries:
                    for entry in entries:
                        if not entry.name.startswith("."):
                            return True
            except OSError:
                continue
        logger.error("订阅缺集自愈：媒体库根目录为空，本轮不判定任何失效记录")
        return False

    def _show_dirs(self, name: str, year: str) -> List[str]:
        """按名称定位媒体库中的剧集目录，避免全库遍历。"""
        name_norm = self._norm(name)
        if not name_norm:
            return []
        matched: List[str] = []
        seen: Set[str] = set()
        for root in self._lib_roots:
            if not os.path.isdir(root) or root in seen:
                continue
            seen.add(root)
            stack: List[Tuple[str, int]] = [(root, 0)]
            while stack:
                current, depth = stack.pop()
                if depth >= 3:
                    continue
                try:
                    entries = list(os.scandir(current))
                except OSError:
                    continue
                for entry in entries:
                    try:
                        if not entry.is_dir() or entry.name.startswith("."):
                            continue
                    except OSError:
                        continue
                    if entry.name.lower() in SKIP_TOP_DIRS:
                        continue
                    dir_norm = self._norm(entry.name)
                    if dir_norm.startswith(name_norm):
                        if not year or year in entry.name or dir_norm == name_norm:
                            matched.append(entry.path)
                            continue
                    stack.append((entry.path, depth + 1))
        return matched

    def _library_episodes(self, subscribe) -> Tuple[Set[int], Set[int]]:
        """媒体库中该订阅该季的集号。

        返回 (按订阅名匹配到的集号, 该季目录下所有视频的集号)。后者用于识别"库里有文件但片名对不上"
        的情况，此时不做判定，避免把命名差异误判成缺集。
        """
        sid = int(getattr(subscribe, "id", 0) or 0)
        cached = self._cache.get(sid)
        now = time.time()
        if cached and self._cache_ttl and now - cached[0] < self._cache_ttl:
            return cached[1], cached[2]

        name = str(subscribe.name or "")
        year = str(getattr(subscribe, "year", "") or "")
        season = int(subscribe.season or 0)
        name_norm = self._norm(name)
        min_bytes = self._min_file_mb * 1024 * 1024
        matched: Set[int] = set()
        all_episodes: Set[int] = set()
        for directory in self._show_dirs(name, year):
            for current, dirs, files in os.walk(directory):
                dirs[:] = [d for d in dirs if not d.startswith(".") and d.lower() not in SKIP_TOP_DIRS]
                for filename in files:
                    if os.path.splitext(filename)[1].lower() not in VIDEO_EXT:
                        continue
                    stem = os.path.splitext(filename)[0]
                    path = os.path.join(current, filename)
                    try:
                        if os.path.getsize(path) < min_bytes:
                            continue
                    except OSError:
                        continue
                    try:
                        meta = MetaVideo(stem)
                    except Exception:  # noqa: BLE001
                        continue
                    found = [e for e in (meta.episode_list or []) if e and e > 0]
                    if not found:
                        continue
                    file_season = meta.begin_season
                    if file_season is None:
                        season_match = SEASON_DIR_RE.search(current) or SEASON_WORD_RE.search(current)
                        file_season = int(season_match.group(1)) if season_match else season
                    if season and int(file_season) != season:
                        continue
                    all_episodes.update(found)
                    if not name_norm or name_norm in self._norm(stem):
                        matched.update(found)
        self._cache[sid] = (now, matched, all_episodes)
        return matched, all_episodes

    # ------------------------------------------------------------------ 判定

    @staticmethod
    def _parse_episodes(text: Any) -> Set[int]:
        result: Set[int] = set()
        raw = str(text or "")
        for matched in EPISODE_RE.finditer(raw):
            start = int(matched.group(1))
            end = int(matched.group(2)) if matched.group(2) else start
            if end < start or end - start > 500:
                continue
            result.update(range(start, end + 1))
        if result:
            return result
        for token in re.findall(r"\d{1,4}", raw):
            try:
                result.add(int(token))
            except ValueError:
                continue
        return result

    @staticmethod
    def _parse_dt(value: Any) -> Optional[datetime]:
        if isinstance(value, datetime):
            return value
        text = str(value or "").strip()
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(text[:19], fmt)
            except ValueError:
                continue
        return None

    def _active_hashes(self) -> Set[str]:
        hashes: Set[str] = set()
        try:
            services = DownloaderHelper().get_services()
        except Exception as err:  # noqa: BLE001
            logger.warning(f"订阅缺集自愈：获取下载器失败 {err}")
            return hashes
        for _, info in (services or {}).items():
            instance = getattr(info, "instance", None)
            if not instance:
                continue
            try:
                torrents, _ = instance.get_torrents()
            except Exception as err:  # noqa: BLE001
                logger.warning(f"订阅缺集自愈：读取下载器任务失败 {err}")
                continue
            for torrent in torrents or []:
                torrent_hash = getattr(torrent, "hash", None)
                if torrent_hash:
                    hashes.add(str(torrent_hash).lower())
        return hashes

    def _history_rows(self, subscribe) -> List[Any]:
        try:
            media_source = MediaSource(str(subscribe.media_source))
        except Exception:  # noqa: BLE001
            return []
        try:
            return DownloadHistoryOper().get_by_media_identity(
                media_source=media_source,
                media_id=str(subscribe.media_id or ""),
            ) or []
        except Exception as err:  # noqa: BLE001
            logger.warning(f"订阅缺集自愈：读取下载历史失败 {err}")
            return []

    @staticmethod
    def _season_of_history(row: Any) -> Optional[int]:
        raw = str(getattr(row, "seasons", "") or "")
        matched = re.search(r"S(\d{1,2})", raw, re.I)
        if matched:
            return int(matched.group(1))
        if raw.isdigit():
            return int(raw)
        return None

    # ------------------------------------------------------------------ 主流程

    def check(self):
        """每日兜底巡检。"""
        started = datetime.now()
        stats = {
            "time": started.strftime("%Y-%m-%d %H:%M:%S"),
            "dryrun": self._dryrun,
            "scanned": 0,
            "healed": 0,
            "cleared": 0,
            "skipped": 0,
            "searched": 0,
            "items": [],
            "errors": [],
        }
        try:
            self._cache = {}
            self._facts = None
            if self._enabled and self._heal_before_search and not self._is_hooked():
                logger.warning("订阅缺集自愈：搜索前钩子缺失，正在重新挂载")
                self._install_hook()
                if not self._is_hooked():
                    stats["errors"].append("搜索前钩子挂载失败，搜索将使用未经核对的事实")
            active = self._active_hashes()
            now = datetime.now()
            try:
                subscribes = SubscribeOper().list(state=",".join(self._states))
            except TypeError:
                subscribes = SubscribeOper().list()
            budget = self._max_actions
            for subscribe in subscribes or []:
                if not self._is_tv(subscribe):
                    continue
                try:
                    healed, cleared = self._check_one(
                        subscribe, active, now, budget, trigger_search=self._auto_search, stats=stats
                    )
                except Exception as err:  # noqa: BLE001
                    logger.error(f"订阅缺集自愈：处理订阅 {getattr(subscribe, 'name', '?')} 失败 {err}")
                    stats["errors"].append(f"{getattr(subscribe, 'name', '?')}：{err}")
                    continue
                budget -= cleared
                stats["scanned"] += 1
                if healed:
                    stats["healed"] += 1
                stats["cleared"] += cleared
                if budget <= 0:
                    logger.warning("订阅缺集自愈：本轮处理量已达上限，剩余订阅留待下次")
                    break
        except Exception as err:  # noqa: BLE001
            logger.error(f"订阅缺集自愈：巡检异常 {err}")
            stats["errors"].append(str(err))

        self._save_run(stats)
        logger.info(
            f"订阅缺集自愈：扫描 {stats['scanned']} 个订阅，修复 {stats['healed']} 个，"
            f"清理 {stats['cleared']} 集，触发补搜 {stats['searched']} 次，"
            f"跳过 {stats['skipped']} 集（{'演练' if self._dryrun else '正式'}）"
        )
        if self._notify and (stats["healed"] or stats["errors"]):
            self._notify_result(stats)

    def _check_one(
        self,
        subscribe,
        active: Set[str],
        now: datetime,
        budget: int,
        trigger_search: bool,
        stats: Optional[Dict[str, Any]] = None,
    ) -> Tuple[bool, int]:
        """核对单个订阅，返回（是否有改动, 改动集数）。"""
        stats = stats if stats is not None else {"skipped": 0, "items": [], "errors": [], "searched": 0}
        if not self._is_tv(subscribe):
            return False, 0
        if not self._library_ready():
            return False, 0
        sid = int(getattr(subscribe, "id", 0) or 0)
        name = str(subscribe.name or "")
        season = int(subscribe.season or 0)
        start_episode = int(subscribe.start_episode or 1)
        total_episode = int(subscribe.total_episode or 0)

        note = self._as_list(subscribe.note)
        priority = self._as_priority(subscribe.episode_priority)
        claimed: Set[int] = set(note)
        for key, value in priority.items():
            try:
                if int(value) > 0:
                    claimed.add(int(key))
            except (TypeError, ValueError):
                continue

        if total_episode:
            target: Set[int] = set(range(start_episode, total_episode + 1))
            claimed &= target
        else:
            target = set(claimed)

        actual, season_files = self._library_episodes(subscribe)

        # 无任何已下载记录：只处理"残留洗版门槛"，不做推断
        if not claimed and not actual:
            if (
                subscribe.best_version
                and int(getattr(subscribe, "current_priority", 0) or 0) > 0
                and target
            ):
                logger.info(
                    f"订阅缺集自愈：{name} 第{season}季 无已下载记录但洗版门槛为 "
                    f"{subscribe.current_priority}，重置为 0"
                )
                if not self._dryrun:
                    try:
                        SubscribeOper().update(sid, {"current_priority": 0})
                    except Exception as err:  # noqa: BLE001
                        logger.error(f"订阅缺集自愈：重置 {name} 洗版门槛失败 {err}")
                        return False, 0
                self._cache.pop(sid, None)
                return True, 0
            return False, 0

        if not target:
            target = set(claimed) | set(actual)
        if not actual and season_files:
            # 库里有该季文件，但和订阅名对不上（片名别名/命名差异），不下结论
            logger.debug(f"订阅缺集自愈：{name} 第{season}季 库内命名不匹配，跳过核对")
            return False, 0

        # ---- 方向一：库里有文件、订阅声明没有 → 补进事实，避免被当成缺集
        adopted: Set[int] = set()
        if total_episode and actual:
            adopted = {episode for episode in (actual - claimed) if episode in target}

        # ---- 方向二：订阅声明有、库里没有 → 判定是否真的失效
        stale: List[int] = []
        reasons: Dict[int, str] = {}
        missing_claimed = claimed - actual
        if missing_claimed:
            history = self._history_rows(subscribe)
            rows_by_episode: Dict[int, List[Any]] = {}
            for row in history:
                row_season = self._season_of_history(row)
                if row_season is not None and season and row_season != season:
                    continue
                for episode in self._parse_episodes(getattr(row, "episodes", "")):
                    rows_by_episode.setdefault(episode, []).append(row)

            for episode in sorted(missing_claimed):
                rows = rows_by_episode.get(episode) or []
                if rows:
                    hashes = {str(getattr(r, "download_hash", "") or "").lower() for r in rows}
                    if any(h and h in active for h in hashes):
                        stats["skipped"] += 1
                        reasons[episode] = "种子仍在下载器"
                        continue
                    if any(self._download_alive(r, episode) for r in rows):
                        stats["skipped"] += 1
                        reasons[episode] = "该集下载文件仍在磁盘"
                        continue
                    newest = None
                    for row in rows:
                        stamp = self._parse_dt(getattr(row, "date", None))
                        if stamp and (newest is None or stamp > newest):
                            newest = stamp
                    if newest and (now - newest) < timedelta(minutes=self._min_age_minutes):
                        stats["skipped"] += 1
                        reasons[episode] = "记录过新，等待下载"
                        continue
                    stale.append(episode)
                    reasons[episode] = "种子已删除且媒体库无文件"
                else:
                    stale.append(episode)
                    reasons[episode] = "无下载记录且媒体库无文件"

        if len(stale) > self._max_clear_per_sub:
            logger.error(
                f"订阅缺集自愈：{name} 第{season}季 一次将清理 {len(stale)} 集，超过单订阅上限 "
                f"{self._max_clear_per_sub}，已放弃本次改动（疑似媒体库不可见或命名突变，请先人工确认）"
            )
            stats["errors"].append(
                f"{name} 第{season}季 待清理 {len(stale)} 集超过上限，已跳过"
            )
            return False, 0
        stale = sorted(stale)[: max(0, budget)]

        # ---- 汇总新的事实：保留声明 + 入库补全 - 失效
        stale_set = set(stale)
        keep = sorted((claimed | adopted) - stale_set)

        # ---- 方向三：库里有该集但优先级未知 → 恢复/推断/锁定，避免"未知即 0"被当成任意资源可洗版
        facts = self._facts_get()
        facts_priority = (facts.get(str(sid)) or {}).get("priority") or {}
        known_values: List[int] = []
        for source in (priority, facts_priority):
            for key, value in (source or {}).items():
                try:
                    if int(key) in keep and int(value) > 0:
                        known_values.append(int(value))
                except (TypeError, ValueError):
                    continue
        baseline = max(known_values) if known_values else 0

        new_priority: Dict[str, int] = {}
        for key, value in priority.items():
            try:
                episode = int(key)
            except (TypeError, ValueError):
                continue
            if episode in stale_set:
                continue
            new_priority[str(episode)] = int(value)

        fixed: Dict[int, str] = {}
        # 只有洗版订阅才依赖 episode_priority；普通订阅写入优先级会改动整包优先策略
        for episode in (keep if getattr(subscribe, "best_version", 0) else []):
            key = str(episode)
            if int(new_priority.get(key, 0) or 0) > 0:
                continue
            restored = 0
            try:
                restored = int((facts_priority or {}).get(key) or 0)
            except (TypeError, ValueError):
                restored = 0
            if restored > 0:
                new_priority[key] = restored
                fixed[episode] = "按历史快照恢复优先级"
                continue
            if self._unknown_policy == "keep":
                continue
            if self._unknown_policy == "infer" and baseline > 0:
                new_priority[key] = baseline
                fixed[episode] = "未知版本，按同订阅已知优先级推断"
                continue
            new_priority[key] = 100
            fixed[episode] = "未知版本已锁定，需人工确认后才参与洗版"

        lack = len(target - set(keep))
        changed_now = bool(stale or adopted or fixed) or lack != int(getattr(subscribe, "lack_episode", 0) or 0)
        if not changed_now and set(keep) == set(note) and new_priority == priority:
            return False, 0

        detail = {
            "name": f"{name} ({getattr(subscribe, 'year', '') or ''})".strip(),
            "season": season,
            "cleared": self._episode_text(stale),
            "adopted": self._episode_text(sorted(adopted)),
            "fixed": self._episode_text(sorted(fixed)),
            "lack": lack,
            "reason": "；".join(sorted({reasons.get(e, "") for e in stale if reasons.get(e)})),
        }
        stats["items"].append(detail)
        logger.info(
            f"订阅缺集自愈：{name} 第{season}季 失效声明 {self._episode_text(stale) or '无'}，"
            f"库内补全 {self._episode_text(sorted(adopted)) or '无'}，"
            f"优先级修复 {self._episode_text(sorted(fixed)) or '无'}，"
            f"媒体库现有 {len(actual)} 集，缺失重算为 {lack}"
        )

        if self._dryrun:
            logger.info(
                f"订阅缺集自愈：搜索前核对（演练）{name} 第{season}季 声明 {len(claimed)} 集 / "
                f"库内 {len(actual)} 集 / 缺失 {lack}，将清理 {self._episode_text(stale) or '无'}、"
                f"补全 {self._episode_text(sorted(adopted)) or '无'}、修优先级 {self._episode_text(sorted(fixed)) or '无'}"
            )
            return True, len(stale) + len(adopted) + len(fixed)

        payload: Dict[str, Any] = {
            "note": keep,
            "episode_priority": {k: new_priority[k] for k in sorted(new_priority, key=lambda x: int(x))},
            "lack_episode": lack,
        }
        # 洗版订阅的升级门槛取自 current_priority；整批记录都被清空说明这一季其实没有版本，
        # 门槛不清掉会让所有候选资源继续被判"优先级未达到升级条件"，等于白清。
        if (
            subscribe.best_version
            and not [v for v in new_priority.values() if v > 0]
            and int(getattr(subscribe, "current_priority", 0) or 0) != 0
        ):
            payload["current_priority"] = 0
        try:
            SubscribeOper().update(sid, payload)
        except Exception as err:  # noqa: BLE001
            logger.error(f"订阅缺集自愈：写入订阅 {name} 失败 {err}")
            stats["errors"].append(f"{name} 写库失败：{err}")
            return False, 0

        # 写库成功后再保存事实快照，作为后续"优先级丢失"的恢复来源
        self._facts_put(sid, keep, new_priority, actual)
        self._cache.pop(sid, None)

        if trigger_search:
            try:
                from app.chain.subscribe import SubscribeChain

                SubscribeChain().search(sid=sid, manual=True)
                stats["searched"] += 1
                logger.info(f"订阅缺集自愈：已触发 {name} 补搜")
            except Exception as err:  # noqa: BLE001
                logger.error(f"订阅缺集自愈：触发 {name} 补搜失败 {err}")
                stats["errors"].append(f"{name} 补搜失败：{err}")

        return True, len(stale) + len(adopted) + len(fixed)

    # ------------------------------------------------------------------ 事实快照

    def _facts_get(self) -> Dict[str, Any]:
        """读取按订阅保存的 note/优先级快照，用于恢复被清空的优先级。"""
        if self._facts is None:
            try:
                data = self.get_data("facts")
            except Exception as err:  # noqa: BLE001
                logger.warning(f"订阅缺集自愈：读取事实快照失败 {err}")
                data = None
            self._facts = data if isinstance(data, dict) else {}
        return self._facts

    def _facts_put(self, sid: int, note: List[int], priority: Dict[str, int], actual: Set[int]):
        """保存本次核对后的快照；只在优先级有值时覆盖，避免用空值冲掉可用基线。"""
        if not sid:
            return
        try:
            facts = dict(self._facts_get())
            old = facts.get(str(sid)) or {}
            merged = {
                str(k): int(v)
                for k, v in {**(old.get("priority") or {}), **priority}.items()
                if int(v) > 0
            }
            facts[str(sid)] = {
                "note": sorted(int(e) for e in note),
                "priority": merged,
                "actual": sorted(int(e) for e in actual),
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
            if len(facts) > 800:
                ordered = sorted(
                    facts.items(),
                    key=lambda item: str((item[1] or {}).get("time") or ""),
                    reverse=True,
                )[:800]
                facts = dict(ordered)
            self._facts = facts
            self.save_data("facts", facts)
        except Exception as err:  # noqa: BLE001
            logger.warning(f"订阅缺集自愈：保存事实快照失败 {err}")

    # ------------------------------------------------------------------ 解析

    @staticmethod
    def _as_list(value: Any) -> List[int]:
        if isinstance(value, list):
            items = value
        elif isinstance(value, str):
            text = value.strip()
            if text.startswith("["):
                try:
                    items = json.loads(text)
                except Exception:  # noqa: BLE001
                    items = re.findall(r"\d+", text)
            else:
                items = re.findall(r"\d+", text)
        else:
            items = []
        result: List[int] = []
        for item in items or []:
            try:
                result.append(int(item))
            except (TypeError, ValueError):
                continue
        return result

    @staticmethod
    def _as_priority(value: Any) -> Dict[str, int]:
        data: Any = value
        if isinstance(value, str):
            try:
                data = json.loads(value)
            except Exception:  # noqa: BLE001
                data = {}
        if not isinstance(data, dict):
            return {}
        result: Dict[str, int] = {}
        for key, item in data.items():
            try:
                result[str(int(key))] = int(item)
            except (TypeError, ValueError):
                continue
        return result

    def _download_alive(self, row: Any, episode: int) -> bool:
        """判断下载历史指向的**该集视频文件**是否还真实存在且有效。

        下载历史的 path 多数是种子目录，目录在种子删除、文件整理后依然存在，
        用 exists 判断会把所有记录永远当成"在途"，本插件就变成空转。
        """
        path = str(getattr(row, "path", "") or "").strip()
        if not path:
            return False
        min_bytes = max(1, self._min_file_mb) * 1024 * 1024
        try:
            if os.path.isfile(path):
                return os.path.getsize(path) >= min_bytes
            if not os.path.isdir(path):
                return False
            for entry in os.scandir(path):
                try:
                    if not entry.is_file():
                        continue
                except OSError:
                    continue
                if os.path.splitext(entry.name)[1].lower() not in VIDEO_EXT:
                    continue
                try:
                    if entry.stat().st_size < min_bytes:
                        continue
                except OSError:
                    continue
                stem = os.path.splitext(entry.name)[0]
                try:
                    episodes = {int(e) for e in (MetaVideo(stem).episode_list or []) if e}
                except Exception:  # noqa: BLE001
                    episodes = set()
                if episodes:
                    if episode in episodes:
                        return True
                    continue
                if re.search(r"(?:^|[^0-9a-z])e(?:p)?0*%d(?:[^0-9]|$)" % episode, stem, re.I):
                    return True
        except OSError:
            return False
        return False

    @staticmethod
    def _path_exists(path: Any) -> bool:
        text = str(path or "").strip()
        if not text:
            return False
        try:
            return os.path.exists(text)
        except OSError:
            return False

    @staticmethod
    def _episode_text(episodes: List[int]) -> str:
        if not episodes:
            return ""
        values = sorted(set(int(e) for e in episodes))
        parts: List[str] = []
        start = prev = values[0]
        for value in values[1:]:
            if value == prev + 1:
                prev = value
                continue
            parts.append(f"E{start:02d}" if start == prev else f"E{start:02d}-E{prev:02d}")
            start = prev = value
        parts.append(f"E{start:02d}" if start == prev else f"E{start:02d}-E{prev:02d}")
        return ",".join(parts)

    # ------------------------------------------------------------------ 记录

    def _save_run(self, record: dict):
        try:
            self.save_data("last_run", record)
            history = self.get_data("history") or []
            if not isinstance(history, list):
                history = []
            history.insert(0, record)
            self.save_data("history", history[:30])
        except Exception as err:  # noqa: BLE001
            logger.error(f"订阅缺集自愈：保存执行记录失败 {err}")

    def _notify_result(self, stats: dict):
        lines = [
            f"扫描订阅：{stats['scanned']} 个",
            f"修复订阅：{stats['healed']} 个",
            f"清理记录：{stats['cleared']} 集",
            f"触发补搜：{stats['searched']} 次",
        ]
        if stats["dryrun"]:
            lines.insert(0, "【演练模式】仅记录，未改动数据")
        for item in stats["items"][:10]:
            extra = []
            if item.get("adopted"):
                extra.append(f"补全 {item['adopted']}")
            if item.get("fixed"):
                extra.append(f"修优先级 {item['fixed']}")
            tail = f"｜{'，'.join(extra)}" if extra else ""
            lines.append(
                f"· {item['name']} 第{item['season']}季 清理 {item['cleared'] or '无'}（剩余缺 {item['lack']}）{tail}"
            )
        if stats["errors"]:
            lines.append("错误：" + "；".join(stats["errors"][:5]))
        try:
            self.post_message(
                mtype=NotificationType.Plugin,
                title="【订阅缺集自愈】已清理失效记录并补搜",
                text="\n".join(lines),
            )
        except Exception as err:  # noqa: BLE001
            logger.error(f"订阅缺集自愈：发送通知失败 {err}")

    # ------------------------------------------------------------------ 面板

    @staticmethod
    def _stat_card(title: str, value: str, subtitle: str = "", color: str = "primary") -> dict:
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

    def get_page(self) -> List[dict]:
        try:
            last: dict = self.get_data("last_run") or {}
            cards = [
                self._stat_card(
                    "运行状态",
                    "运行中" if self._enabled else "已停止",
                    "搜索前核对已挂载" if self._is_hooked() else "搜索前核对未挂载",
                    "success" if self._enabled and self._is_hooked() else "warning",
                ),
                self._stat_card(
                    "上次扫描",
                    f"{int(last.get('scanned') or 0)} 个订阅",
                    last.get("time") or "尚未执行过",
                    "primary",
                ),
                self._stat_card(
                    "上次清理",
                    f"{int(last.get('cleared') or 0)} 集",
                    f"修复 {int(last.get('healed') or 0)} 个订阅",
                    "error" if last.get("cleared") else "success",
                ),
                self._stat_card(
                    "模式",
                    "演练" if last.get("dryrun", self._dryrun) else "正式",
                    f"跳过 {int(last.get('skipped') or 0)} 集",
                    "info" if (last.get("dryrun", self._dryrun)) else "primary",
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
            items = last.get("items") or []
            if items:
                rows = [
                    [
                        item.get("name", ""),
                        f"第{item.get('season', '')}季",
                        item.get("cleared", "") or "无",
                        item.get("adopted", "") or "无",
                        item.get("fixed", "") or "无",
                        str(item.get("lack", "")),
                        item.get("reason", ""),
                    ]
                    for item in items[:30]
                ]
                blocks.append(
                    {
                        "component": "VCard",
                        "props": {"variant": "outlined", "class": "mt-3"},
                        "content": [
                            {
                                "component": "VCardTitle",
                                "props": {"class": "text-subtitle-1"},
                                "text": "上次清理明细",
                            },
                            {
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
                                                        "props": {"class": "text-start text-caption"},
                                                        "text": header,
                                                    }
                                                    for header in ("订阅", "季", "失效声明", "库内补全", "优先级修复", "剩余缺失", "原因")
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
                            },
                        ],
                    }
                )
            else:
                blocks.append(
                    {
                        "component": "VAlert",
                        "props": {
                            "type": "success",
                            "variant": "tonal",
                            "text": "上次巡检未发现失效记录：订阅声明与媒体库文件一致。",
                        },
                    }
                )
            if last.get("errors"):
                blocks.append(
                    {
                        "component": "VAlert",
                        "props": {
                            "type": "warning",
                            "variant": "tonal",
                            "class": "mt-3",
                            "text": "；".join(last["errors"][:5]),
                        },
                    }
                )
            return blocks
        except Exception as err:  # noqa: BLE001
            logger.error(f"订阅缺集自愈：渲染详情页失败 {err}")
            return [
                {
                    "component": "VAlert",
                    "props": {"type": "error", "variant": "tonal", "text": f"面板渲染失败：{err}"},
                }
            ]

    @classmethod
    def _is_hooked(cls) -> bool:
        """钩子必须存在且版本匹配；旧实例残留的包装器要重装。"""
        try:
            from app.chain.subscribe import SubscribeChain

            search = getattr(SubscribeChain.search, "_sh_v", None)
            single = getattr(getattr(SubscribeChain, "_search_subscription", None), "_sh_v", None)
            return bool(search == cls.plugin_version and single == cls.plugin_version)
        except Exception:  # noqa: BLE001
            return False

    # ------------------------------------------------------------------ 表单

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
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
                                        "props": {"model": "dryrun", "label": "演练模式（只记录不写库）"},
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
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "heal_before_search",
                                            "label": "每次订阅搜索前先核对（推荐）",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "auto_search", "label": "兜底巡检时自动补搜"},
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "strict_guard",
                                            "label": "严格守卫：核对失败就跳过本轮搜索",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VCronField",
                                        "props": {
                                            "model": "cron",
                                            "label": "兜底巡检周期",
                                            "placeholder": "0 4 * * *",
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
                                            "model": "min_age_minutes",
                                            "label": "记录保护期（分钟）",
                                            "placeholder": "120",
                                            "hint": "记录新于此值时不判定，避免抢在下完/整理前重下",
                                            "persistent-hint": True,
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
                                            "model": "max_actions",
                                            "label": "单轮最多清理集数",
                                            "placeholder": "200",
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
                                            "model": "max_clear_per_sub",
                                            "label": "单订阅单轮最多清理集数",
                                            "placeholder": "30",
                                            "hint": "超过上限直接放弃改动，防止媒体库不可见时整季判失效",
                                            "persistent-hint": True,
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
                                            "model": "min_file_mb",
                                            "label": "有效文件大小下限（MB）",
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
                                            "model": "cache_ttl",
                                            "label": "媒体库扫描缓存（秒）",
                                            "placeholder": "600",
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
                                        "component": "VTextField",
                                        "props": {
                                            "model": "library_roots",
                                            "label": "媒体库目录（留空自动扫描 /media 各分类）",
                                            "placeholder": "/media/国产剧,/media/欧美剧",
                                        },
                                    }
                                ],
                            }
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
                                        "component": "VSelect",
                                        "props": {
                                            "model": "unknown_policy",
                                            "label": "库内剧集优先级未知时的处理",
                                            "items": [
                                                {"title": "按同订阅已知优先级推断（推荐）", "value": "infer"},
                                                {"title": "锁定，不参与洗版", "value": "lock"},
                                                {"title": "保持 0，允许任意洗版（有整季重下风险）", "value": "keep"},
                                            ],
                                            "hint": "优先级为 0 等于“任何资源都算升级”，是整季重下的根因",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            }
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
                                            "text": (
                                                "仅处理电视剧订阅。双向核对：① 订阅 note / episode_priority 声明已下载但媒体库无文件，"
                                                "且种子已不在下载器、该集下载文件不在磁盘、记录超过保护期 → 剔除记录；"
                                                "② 媒体库有该集但订阅没记录 → 补进事实；"
                                                "③ 库内剧集优先级为 0/缺失 → 按历史快照恢复或同订阅推断，避免“未知即任意升级”。"
                                            ),
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
            "heal_before_search": True,
            "auto_search": True,
            "strict_guard": True,
            "unknown_policy": "infer",
            "cron": "0 4 * * *",
            "states": ["R", "P"],
            "min_age_minutes": 120,
            "max_actions": 200,
            "min_file_mb": 10,
            "max_clear_per_sub": 30,
            "cache_ttl": 600,
            "library_roots": "",
        }
