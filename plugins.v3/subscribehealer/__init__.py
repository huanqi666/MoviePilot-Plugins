# -*- coding: utf-8 -*-
"""订阅缺集自愈。

MoviePilot 在**提交下载**时就把集号写进订阅的 note 与 episode_priority，之后种子被删、下载失败、
文件被清理都不会回滚。对开启洗版的订阅，这些"已下载"记录会让缺失集变成"洗版候选"，只有更高优先级的
资源才会被接受，于是站点上明明有资源却永远补不下来。

本插件以媒体库实际文件为唯一事实：
1. **每次订阅搜索开始前**先核对一遍（挂载 SubscribeChain.search 前钩子），
   发现订阅声明了已下载、媒体库却没有的集，就剔除记录，让本次搜索直接补这些集；
2. 另有每日兜底巡检，处理长期不触发搜索的订阅。

判定保守：媒体库已有该集 → 跳过；种子还在下载器 / 下载记录指向的文件仍在磁盘 / 记录未过保护期 →
视为在途；只有确认种子没了、文件也没了，才认定记录失效。
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
    plugin_desc = "订阅搜索前先核对媒体库与已下载记录，剔除失效声明并补齐缺集，解决缺集永远发现不了。"
    plugin_icon = "https://raw.githubusercontent.com/huanqi666/MoviePilot-Plugins/main/icons/subscribehealer.svg"
    plugin_version = "1.1.0"
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
    _cron: Optional[str] = None
    _states: List[str] = ["R", "P"]
    _min_age_minutes: int = 120
    _max_actions: int = 200
    _min_file_mb: int = 10
    _library_roots: str = ""
    _cache_ttl: int = 600
    _cache: Dict[int, Tuple[float, Set[int], Set[int]]] = {}

    def init_plugin(self, config: dict = None):
        """读取配置并安装搜索前钩子。"""
        if config:
            self._enabled = bool(config.get("enabled"))
            self._notify = bool(config.get("notify"))
            self._onlyonce = bool(config.get("onlyonce"))
            self._dryrun = bool(config.get("dryrun"))
            self._auto_search = config.get("auto_search", True) is not False
            self._heal_before_search = config.get("heal_before_search", True) is not False
            self._cron = config.get("cron")
            states = config.get("states")
            if isinstance(states, str):
                states = [s.strip() for s in states.split(",") if s.strip()]
            self._states = [str(s).upper() for s in (states or ["R", "P"])]
            self._min_age_minutes = self._as_int(config.get("min_age_minutes"), 120, 0, 10080)
            self._max_actions = self._as_int(config.get("max_actions"), 200, 1, 5000)
            self._min_file_mb = self._as_int(config.get("min_file_mb"), 10, 0, 10240)
            self._cache_ttl = self._as_int(config.get("cache_ttl"), 600, 0, 86400)
            self._library_roots = str(config.get("library_roots") or "").strip()

        self._cache = {}
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

    def _install_hook(self):
        """包装订阅搜索入口，让每次搜索前先核对一次。

        队列模式下 MP 会在内部直接调用单订阅搜索，因此同时挂 search 与单订阅入口。
        """
        try:
            from app.chain.subscribe import SubscribeChain

            originals = _HOOK.setdefault("originals", {})

            current = SubscribeChain.search
            if not getattr(current, "_subscribehealer_hooked", False):
                originals["search"] = current

                def hooked_search(chain_self, *args, **kwargs):
                    plugin = _HOOK.get("plugin")
                    if plugin is not None and not getattr(_TLS, "running", False):
                        _TLS.running = True
                        try:
                            bound = inspect.signature(current).bind(chain_self, *args, **kwargs)
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
                    return current(chain_self, *args, **kwargs)

                hooked_search._subscribehealer_hooked = True  # type: ignore[attr-defined]
                SubscribeChain.search = hooked_search

            single = getattr(SubscribeChain, "_search_subscription", None)
            if single and not getattr(single, "_subscribehealer_hooked", False):
                originals["single"] = single

                def hooked_single(chain_self, subscribe, *args, **kwargs):
                    plugin = _HOOK.get("plugin")
                    if plugin is not None and not getattr(_TLS, "single", False):
                        _TLS.single = True
                        try:
                            plugin._heal_subscribe(subscribe)
                            refreshed = chain_self.subscription_repository.get(subscribe.id)
                            if refreshed is not None:
                                subscribe = refreshed
                        except Exception as err:  # noqa: BLE001
                            logger.error(f"订阅缺集自愈：单订阅搜索前核对失败 {err}")
                        finally:
                            _TLS.single = False
                    return single(chain_self, subscribe, *args, **kwargs)

                hooked_single._subscribehealer_hooked = True  # type: ignore[attr-defined]
                SubscribeChain._search_subscription = hooked_single

            logger.info("订阅缺集自愈：已挂载订阅搜索前核对钩子")
        except Exception as err:  # noqa: BLE001
            logger.error(f"订阅缺集自愈：挂载钩子失败 {err}")

    def _remove_hook(self):
        try:
            from app.chain.subscribe import SubscribeChain

            originals = _HOOK.get("originals") or {}
            single = originals.get("single")
            if single and getattr(getattr(SubscribeChain, "_search_subscription", None), "_subscribehealer_hooked", False):
                SubscribeChain._search_subscription = single
            search = originals.get("search")
            if search and getattr(getattr(SubscribeChain, "search", None), "_subscribehealer_hooked", False):
                SubscribeChain.search = search
            if single or search:
                logger.info("订阅缺集自愈：已卸载订阅搜索前核对钩子")
        except Exception as err:  # noqa: BLE001
            logger.warning(f"订阅缺集自愈：卸载钩子失败 {err}")

    def _heal_subscribe(self, subscribe):
        """单个订阅搜索前核对，返回是否发生清理。"""
        if not self._enabled or not self._heal_before_search or self._dryrun:
            return False
        if subscribe is None or str(getattr(subscribe, "type", "")) != "电视剧":
            return False
        active = self._active_hashes()
        changed, count = self._check_one(
            subscribe, active, datetime.now(), self._max_actions, trigger_search=False
        )
        if changed:
            logger.info(f"订阅缺集自愈：搜索前核对清理 {getattr(subscribe, 'name', '?')} {count} 集记录")
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
            "cron": self._cron,
            "states": self._states,
            "min_age_minutes": self._min_age_minutes,
            "max_actions": self._max_actions,
            "min_file_mb": self._min_file_mb,
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
            active = self._active_hashes()
            now = datetime.now()
            try:
                subscribes = SubscribeOper().list(state=",".join(self._states))
            except TypeError:
                subscribes = SubscribeOper().list()
            budget = self._max_actions
            for subscribe in subscribes or []:
                if str(getattr(subscribe, "type", "")) != "电视剧":
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
        """核对单个订阅，返回（是否有清理, 清理集数）。"""
        stats = stats if stats is not None else {"skipped": 0, "items": [], "errors": [], "searched": 0}
        name = str(subscribe.name or "")
        season = int(subscribe.season or 0)
        start_episode = int(subscribe.start_episode or 1)
        total_episode = int(subscribe.total_episode or 0)

        note = subscribe.note or []
        if isinstance(note, str):
            note = [int(x) for x in re.findall(r"\d+", note)]
        claimed: Set[int] = set()
        for episode in note:
            try:
                claimed.add(int(episode))
            except (TypeError, ValueError):
                continue

        priority = subscribe.episode_priority or {}
        if isinstance(priority, str):
            try:
                priority = json.loads(priority)
            except Exception:  # noqa: BLE001
                priority = {}
        if not isinstance(priority, dict):
            priority = {}
        for key, value in priority.items():
            try:
                if float(value) > 0:
                    claimed.add(int(key))
            except (TypeError, ValueError):
                continue

        if total_episode:
            target = set(range(start_episode, total_episode + 1))
            claimed = {e for e in claimed if e in target}
        else:
            target = set(claimed)

        actual, season_files = self._library_episodes(subscribe)

        if not claimed:
            # 没有任何已下载记录，但洗版门槛还挂在订阅上：说明上一次清理只清了记录没清门槛，
            # 站点上同级资源会一直被"优先级未达到升级条件"挡掉。
            if (
                subscribe.best_version
                and int(getattr(subscribe, "current_priority", 0) or 0) > 0
                and target
                and (target - actual)
            ):
                logger.info(
                    f"订阅缺集自愈：{name} 第{season}季 无已下载记录但洗版门槛为 "
                    f"{subscribe.current_priority}，重置为 0"
                )
                if not self._dryrun:
                    try:
                        SubscribeOper().update(subscribe.id, {"current_priority": 0})
                    except Exception as err:  # noqa: BLE001
                        logger.error(f"订阅缺集自愈：重置 {name} 洗版门槛失败 {err}")
                        return False, 0
                self._cache.pop(int(getattr(subscribe, "id", 0) or 0), None)
                return True, 0
            return False, 0

        if not target:
            target = set(claimed)
        if not actual and season_files:
            # 库里有该季文件，但和订阅名对不上（片名别名/命名差异），不下结论
            logger.debug(f"订阅缺集自愈：{name} 第{season}季 库内命名不匹配，跳过核对")
            return False, 0
        missing_claimed = claimed - actual
        if not missing_claimed:
            return False, 0

        history = self._history_rows(subscribe)
        rows_by_episode: Dict[int, List[Any]] = {}
        for row in history:
            row_season = self._season_of_history(row)
            if row_season is not None and season and row_season != season:
                continue
            for episode in self._parse_episodes(getattr(row, "episodes", "")):
                rows_by_episode.setdefault(episode, []).append(row)

        stale: List[int] = []
        reasons: Dict[int, str] = {}
        for episode in sorted(missing_claimed):
            rows = rows_by_episode.get(episode) or []
            if rows:
                hashes = {str(getattr(r, "download_hash", "") or "").lower() for r in rows}
                if any(h and h in active for h in hashes):
                    stats["skipped"] += 1
                    reasons[episode] = "种子仍在下载器"
                    continue
                if any(self._path_exists(getattr(r, "path", "")) for r in rows):
                    stats["skipped"] += 1
                    reasons[episode] = "下载文件仍在磁盘"
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

        if not stale:
            return False, 0

        stale = stale[: max(0, budget)]
        if not stale:
            return False, 0

        stale_set = set(stale)
        keep = sorted(claimed - stale_set)
        new_priority = {
            str(key): value
            for key, value in priority.items()
            if not (str(key).isdigit() and int(key) in stale_set)
        }
        lack = len(target - set(keep))

        stats["items"].append(
            {
                "name": f"{name} ({getattr(subscribe, 'year', '') or ''})".strip(),
                "season": season,
                "cleared": self._episode_text(stale),
                "lack": lack,
                "reason": "；".join(sorted({reasons.get(e, "") for e in stale if reasons.get(e)})),
            }
        )
        logger.info(
            f"订阅缺集自愈：{name} 第{season}季 清理失效记录 {self._episode_text(stale)}，"
            f"媒体库现有 {len(actual)} 集，缺失重算为 {lack}"
        )

        if self._dryrun:
            return True, len(stale)

        payload: Dict[str, Any] = {
            "note": keep,
            "episode_priority": new_priority,
            "lack_episode": lack,
        }
        # 洗版订阅的升级门槛取自 current_priority；整批记录都被清空说明这一季其实没有版本，
        # 门槛不清掉会让所有候选资源继续被判"优先级未达到升级条件"，等于白清。
        if (
            subscribe.best_version
            and not new_priority
            and int(getattr(subscribe, "current_priority", 0) or 0) != 0
        ):
            payload["current_priority"] = 0
        try:
            SubscribeOper().update(subscribe.id, payload)
        except Exception as err:  # noqa: BLE001
            logger.error(f"订阅缺集自愈：写入订阅 {name} 失败 {err}")
            stats["errors"].append(f"{name} 写库失败：{err}")
            return False, 0

        self._cache.pop(int(getattr(subscribe, "id", 0) or 0), None)

        if trigger_search:
            try:
                from app.chain.subscribe import SubscribeChain

                SubscribeChain().search(sid=subscribe.id, manual=True)
                stats["searched"] += 1
                logger.info(f"订阅缺集自愈：已触发 {name} 补搜")
            except Exception as err:  # noqa: BLE001
                logger.error(f"订阅缺集自愈：触发 {name} 补搜失败 {err}")
                stats["errors"].append(f"{name} 补搜失败：{err}")

        return True, len(stale)

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
            lines.append(f"· {item['name']} 第{item['season']}季 清理 {item['cleared']}（剩余缺 {item['lack']}）")
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
                        item.get("cleared", ""),
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
                                                    for header in ("订阅", "季", "清理集号", "剩余缺失", "原因")
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

    @staticmethod
    def _is_hooked() -> bool:
        try:
            from app.chain.subscribe import SubscribeChain

            return bool(getattr(SubscribeChain.search, "_subscribehealer_hooked", False))
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
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "text": (
                                                "仅处理电视剧订阅。判定依据：订阅 note / episode_priority 声明已下载，"
                                                "但媒体库无对应文件，且种子已不在下载器、下载文件已不在磁盘、记录超过保护期。"
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
            "cron": "0 4 * * *",
            "states": ["R", "P"],
            "min_age_minutes": 120,
            "max_actions": 200,
            "min_file_mb": 10,
            "cache_ttl": 600,
            "library_roots": "",
        }
