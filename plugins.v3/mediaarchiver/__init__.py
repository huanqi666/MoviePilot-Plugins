# -*- coding: utf-8 -*-
"""媒体归档：把已经定版的资源上传到网盘。

MoviePilot 自带的整理是「下载完就整理」，洗版订阅会反复换版本，所以直接用
网盘的存储做媒体库，等于每换一版就重传一遍。这里换个思路做**旁路归档**：

1. 只认整理历史（transferhistory 里 status=1 的记录），也就是「下载并整理进库」的资源；
2. 同一个媒体（电影按片、剧集按季）取**最后一条整理时间**，距今超过静默天数，
   才认为版本已经稳定；中间任何一个新版本入库，时间都会刷新，静默期自动重置；
3. 还在运行/暂停的订阅直接跳过，避免把追更中的剧当成定版；
4. 本地原样保留，只用网盘存储的 upload 复制一份过去，断点续传按「远端已存在且大小一致」跳过。

网盘侧走 MoviePilot 自己的存储实现（115 直连、Alist/OpenList 中转夸克等），
所以认证、分片、风控都复用系统存储，插件不碰账号密码。

配置里的目标写成「存储类型:根路径」一行一个，例如：

    u115:/归档
    alist:/夸克归档

同一部片对每个目标分别记账，后来新增网盘时会把存量补传，不会重复传已经传过的。
"""

import os
import re
import sqlite3
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from apscheduler.triggers.cron import CronTrigger

from app.schemas import NotificationType
from app.sdk.logging import logger
from app.sdk.plugin import _PluginBase

DB_PATH = "/config/user.db"
SUCCESS_STATUS = 1
SKIP_DIRS = {"incomplete", "temp", ".recycle", "recycle", "lost+found", "@eadir", "#recycle"}
# 这些不是媒体库：下载暂存、刷流目录、网盘挂载点
NON_LIBRARY_DIRS = {"downloads", "mt-brush", "brush", "strm"}
SEASON_RE = re.compile(r"^[Ss]eason[\s._-]*(\d{1,2})$")
SKIP_FILE_EXT = {".part", ".tmp", ".downloading", ".!qb"}
# 活跃订阅状态：运行中、暂停
ACTIVE_SUB_STATES = {"R", "P"}
# 各存储实现类；键与 MoviePilot 的 StorageSchema 值一致
STORAGE_IMPL = {
    "u115": ("app.modules.filemanager.storages.u115", "U115Pan"),
    "alipan": ("app.modules.filemanager.storages.alipan", "AliPan"),
    "alist": ("app.modules.filemanager.storages.alist", "Alist"),
    "rclone": ("app.modules.filemanager.storages.rclone", "Rclone"),
}


class MediaArchiver(_PluginBase):
    """把定版资源旁路归档到网盘。"""

    plugin_name = "媒体归档"
    plugin_desc = "订阅资源洗完版、静默若干天后，自动复制一份到 115 / OpenList 等网盘，本地原样保留。"
    plugin_icon = "https://raw.githubusercontent.com/huanqi666/MoviePilot-Plugins/main/icons/mediaarchiver.svg"
    plugin_version = "1.0.0"
    plugin_author = "huanqi666"
    author_url = "https://github.com/huanqi666/MoviePilot-Plugins"
    plugin_config_prefix = "mediaarchiver_"
    plugin_order = 35
    auth_level = 1

    _enabled: bool = False
    _notify: bool = False
    _onlyonce: bool = False
    _dryrun: bool = False
    _cron: Optional[str] = "0 4 * * *"
    _quiet_days: int = 7
    _min_size_mb: int = 500
    _max_items: int = 2
    _types: str = "电影,电视剧"
    _targets: str = ""
    _library_roots: str = ""

    _lock = threading.Lock()
    _dir_cache: Dict[str, Any] = {}

    # ------------------------------------------------------------------ 生命周期

    def init_plugin(self, config: dict = None):
        if config:
            self._enabled = bool(config.get("enabled"))
            self._notify = bool(config.get("notify"))
            self._onlyonce = bool(config.get("onlyonce"))
            self._dryrun = bool(config.get("dryrun"))
            self._cron = str(config.get("cron") or "").strip() or None
            self._quiet_days = self._as_int(config.get("quiet_days"), 7, 1, 180)
            self._min_size_mb = self._as_int(config.get("min_size_mb"), 500, 0, 1024 * 1024)
            self._max_items = self._as_int(config.get("max_items"), 2, 1, 50)
            self._types = str(config.get("types") or "电影,电视剧").strip()
            self._targets = str(config.get("targets") or "").strip()
            self._library_roots = str(config.get("library_roots") or "").strip()

        if self._onlyonce:
            self._onlyonce = False
            self.update_config(self._build_config())
            self.start_background("MediaArchiverOnce")

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    def get_service(self) -> List[Dict[str, Any]]:
        if self._enabled and self._cron:
            return [
                {
                    "id": "MediaArchiver",
                    "name": "媒体归档",
                    "trigger": CronTrigger.from_crontab(self._cron),
                    "func": self.start_background,
                    "kwargs": {"name": "MediaArchiverCron"},
                }
            ]
        return []

    def stop_service(self):
        return

    def start_background(self, name: str = "MediaArchiverCron"):
        """起独立线程跑归档，别占住调度线程。"""
        if not self._enabled:
            logger.info("媒体归档：插件未启用，跳过")
            return
        if self._lock.locked():
            logger.info("媒体归档：上一轮尚未结束，本轮跳过")
            return
        try:
            threading.Thread(target=self._run_once, name=name, daemon=True).start()
        except Exception as err:  # noqa: BLE001
            logger.error(f"媒体归档：启动线程失败 {err}")

    # ------------------------------------------------------------------ 配置

    @staticmethod
    def _as_int(value: Any, default: int, low: int, high: int) -> int:
        try:
            parsed = int(float(value))
        except (TypeError, ValueError):
            return default
        return max(low, min(high, parsed))

    def _build_config(self) -> dict:
        return {
            "enabled": self._enabled,
            "notify": self._notify,
            "onlyonce": self._onlyonce,
            "dryrun": self._dryrun,
            "cron": self._cron,
            "quiet_days": self._quiet_days,
            "min_size_mb": self._min_size_mb,
            "max_items": self._max_items,
            "types": self._types,
            "targets": self._targets,
            "library_roots": self._library_roots,
        }

    @staticmethod
    def _split(value: str) -> List[str]:
        return [item.strip() for item in re.split(r"[,;，；\n]", value or "") if item.strip()]

    @property
    def _lib_roots(self) -> List[str]:
        configured = self._split(self._library_roots)
        if configured:
            return [p for p in configured if os.path.isdir(p)]
        base = "/media"
        roots: List[str] = []
        try:
            for entry in sorted(os.listdir(base)):
                if entry.startswith(".") or entry.lower() in SKIP_DIRS:
                    continue
                if entry.lower() in NON_LIBRARY_DIRS:
                    continue
                path = os.path.join(base, entry)
                if os.path.isdir(path):
                    roots.append(path)
        except OSError:
            pass
        if os.path.isdir(base):
            roots.insert(0, base)
        return roots

    @property
    def _allowed_types(self) -> List[str]:
        return self._split(self._types) or ["电影", "电视剧"]

    def _parse_targets(self, quiet: bool = False) -> List[Tuple[str, str]]:
        """把「存储类型:根路径」解析成列表，非法行直接报出来。"""
        result: List[Tuple[str, str]] = []
        for line in self._split(self._targets):
            if ":" not in line:
                if not quiet:
                    logger.error(f"媒体归档：目标写法不对（应为 存储类型:/路径）：{line}")
                continue
            storage, root = line.split(":", 1)
            storage = storage.strip().lower()
            root = root.strip()
            if not root.startswith("/"):
                root = "/" + root
            if storage not in STORAGE_IMPL:
                if not quiet:
                    logger.error(f"媒体归档：不支持的存储类型 {storage}，可用：{', '.join(STORAGE_IMPL)}")
                continue
            result.append((storage, root.rstrip("/") or "/"))
        return result

    # ------------------------------------------------------------------ 候选

    @staticmethod
    def _connect() -> sqlite3.Connection:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        conn.row_factory = sqlite3.Row
        return conn

    def _active_subscriptions(self) -> List[sqlite3.Row]:
        try:
            conn = self._connect()
            try:
                return conn.execute(
                    "select name, year, season, state, best_version, total_episode"
                    " from subscribe where state in ('R','P')"
                ).fetchall()
            finally:
                conn.close()
        except Exception as err:  # noqa: BLE001
            logger.error(f"媒体归档：读取订阅失败 {err}")
            return []

    def _collect_candidates(self) -> List[dict]:
        """按整理历史聚合，挑出已经静默的定版资源。"""
        try:
            conn = self._connect()
            try:
                rows = conn.execute(
                    "select type, media_id, title, year, seasons, dest, date, category"
                    " from transferhistory"
                    " where status=? and dest is not null and dest<>''",
                    (SUCCESS_STATUS,),
                ).fetchall()
            finally:
                conn.close()
        except Exception as err:  # noqa: BLE001
            logger.error(f"媒体归档：读取整理历史失败 {err}")
            return []

        allowed = set(self._allowed_types)
        groups: Dict[Tuple, dict] = {}
        for row in rows:
            mtype = str(row["type"] or "")
            if mtype not in allowed:
                continue
            dest = str(row["dest"] or "")
            target_dir = self._resolve_dir(dest, mtype)
            if not target_dir or not os.path.isdir(target_dir):
                continue
            key = (mtype, str(row["media_id"] or ""), str(row["seasons"] or ""), target_dir)
            stamp = self._parse_time(row["date"])
            current = groups.get(key)
            if current is None:
                groups[key] = {
                    "type": mtype,
                    "media_id": str(row["media_id"] or ""),
                    "title": str(row["title"] or ""),
                    "year": str(row["year"] or ""),
                    "season": str(row["seasons"] or ""),
                    "category": str(row["category"] or ""),
                    "dir": target_dir,
                    "last": stamp,
                }
            elif stamp and (current["last"] is None or stamp > current["last"]):
                current["last"] = stamp
                current["category"] = str(row["category"] or current["category"])

        now = datetime.now()
        threshold = now - timedelta(days=self._quiet_days)
        active = self._active_subscriptions()
        done_state = self._load_state()
        candidates: List[dict] = []
        for item in groups.values():
            last = item.get("last")
            if last is None:
                continue
            if last > threshold:
                continue
            if self._matches_active(item, active):
                continue
            size, files = self._dir_size(item["dir"])
            if size < self._min_size_mb * 1024 * 1024:
                continue
            item["size"] = size
            item["files"] = files
            item["idle_days"] = round((now - last).total_seconds() / 86400, 1)
            item["done_targets"] = self._done_targets(done_state, item["dir"], size)
            candidates.append(item)

        candidates.sort(key=lambda x: x.get("last") or datetime.min, reverse=True)
        return candidates

    def _matches_active(self, item: dict, active: List[sqlite3.Row]) -> bool:
        """追更中的剧集不归档，避免刚播一半就被当成定版。

        电影没有追更概念，洗版靠静默期约束即可，不受订阅状态影响。
        """
        if item.get("type") != "电视剧":
            return False
        title = item.get("title") or ""
        year = item.get("year") or ""
        for sub in active:
            if str(sub["name"] or "") != title:
                continue
            sub_year = str(sub["year"] or "")
            if year and sub_year and sub_year != year:
                continue
            logger.info(f"媒体归档：{title} 还有活跃订阅（状态 {sub['state']}），本轮跳过")
            return True
        return False

    @staticmethod
    def _resolve_dir(dest: str, mtype: str) -> Optional[str]:
        """从整理历史的 dest 反推这次整理落在哪个目录。"""
        if os.path.isdir(dest):
            return dest
        parent = os.path.dirname(dest)
        if not parent:
            return None
        if mtype == "电视剧":
            # 剧集取季目录：.../剧名 (2026)/Season 1/xxx.mkv
            if SEASON_RE.match(os.path.basename(parent)):
                return parent
            # 有些资源没有季目录，退回剧名目录
            return parent
        return parent

    @staticmethod
    def _parse_time(value: Any) -> Optional[datetime]:
        text = str(value or "").strip()
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                continue
        return None

    @staticmethod
    def _dir_size(path: str) -> Tuple[int, int]:
        total = 0
        count = 0
        for root, dirnames, filenames in os.walk(path):
            dirnames[:] = [d for d in dirnames if not d.startswith(".")]
            for name in filenames:
                if os.path.splitext(name)[1].lower() in SKIP_FILE_EXT:
                    continue
                try:
                    total += os.path.getsize(os.path.join(root, name))
                    count += 1
                except OSError:
                    continue
        return total, count

    # ------------------------------------------------------------------ 归档记录

    def _load_state(self) -> dict:
        try:
            return self.get_data("archive_state") or {}
        except Exception:  # noqa: BLE001
            return {}

    def _save_state(self, state: dict):
        try:
            self.save_data("archive_state", state)
        except Exception as err:  # noqa: BLE001
            logger.error(f"媒体归档：保存归档记录失败 {err}")

    @staticmethod
    def _state_key(path: str, size: int) -> str:
        return f"{path}|{size}"

    def _done_targets(self, state: dict, path: str, size: int) -> Dict[str, dict]:
        items = (state or {}).get("items") or {}
        record = items.get(self._state_key(path, size)) or {}
        return record.get("targets") or {}

    # ------------------------------------------------------------------ 主流程

    def _run_once(self):
        if not self._lock.acquire(blocking=False):
            logger.info("媒体归档：已有任务在执行，跳过")
            return
        started = datetime.now()
        stats = {
            "time": started.strftime("%Y-%m-%d %H:%M:%S"),
            "candidates": 0,
            "archived": 0,
            "skipped": 0,
            "uploaded_files": 0,
            "uploaded_size": 0,
            "dryrun": self._dryrun,
            "items": [],
            "errors": [],
        }
        try:
            targets = self._parse_targets()
            if not targets and not self._dryrun:
                logger.error("媒体归档：没有配置归档目标，本轮结束")
                stats["errors"].append("未配置归档目标（格式：存储类型:/路径）")
                self._save_run(stats)
                return

            candidates = self._collect_candidates()
            stats["candidates"] = len(candidates)
            logger.info(
                f"媒体归档：静默 {self._quiet_days} 天以上的定版资源 {len(candidates)} 个，"
                f"目标 {len(targets)} 个，模式 {'演练' if self._dryrun else '正式'}"
            )

            if self._dryrun:
                target_text = "、".join(f"{s}:{p}" for s, p in targets) or "未配置目标"
                for item in candidates[: self._max_items]:
                    stats["items"].append({
                        "title": item["title"],
                        "season": item["season"],
                        "dir": item["dir"],
                        "size": round(item["size"] / 1024 ** 3, 2),
                        "idle": item["idle_days"],
                        "status": "演练",
                        "detail": target_text,
                    })
                    logger.info(
                        f"媒体归档（演练）：{item['title']} {item['season']} "
                        f"{item['size'] / 1024 ** 3:.2f}GB 静默 {item['idle_days']} 天 → {target_text}"
                    )
                self._save_run(stats)
                return

            state = self._load_state()
            state.setdefault("items", {})
            budget = self._max_items
            for item in candidates:
                if budget <= 0:
                    break
                done = self._done_targets(state, item["dir"], item["size"])
                pending = [(s, p) for s, p in targets if f"{s}:{p}" not in done]
                if not pending:
                    stats["skipped"] += 1
                    continue
                budget -= 1
                self._archive_item(item, pending, state, stats)
                self._save_state(state)

            self._save_run(stats)
            if self._notify and (stats["archived"] or stats["errors"]):
                self._notify_result(stats)
        except Exception as err:  # noqa: BLE001
            logger.error(f"媒体归档：执行失败 {err}")
            stats["errors"].append(str(err))
            self._save_run(stats)
        finally:
            self._dir_cache = {}
            self._lock.release()

    def _archive_item(self, item: dict, pending: List[Tuple[str, str]], state: dict, stats: dict):
        local_dir = Path(item["dir"])
        rel = self._relative_parts(item["dir"])
        if rel is None:
            stats["errors"].append(f"{item['title']} 不在媒体库目录下，跳过")
            logger.error(f"媒体归档：{item['dir']} 不在媒体库目录下，跳过")
            return

        total_files = 0
        total_size = 0
        reached: List[str] = []
        for storage_type, root in pending:
            storage = self._storage(storage_type)
            if storage is None:
                stats["errors"].append(f"存储 {storage_type} 不可用（请先在 设置→存储 里配置）")
                logger.error(f"媒体归档：存储 {storage_type} 未配置或初始化失败")
                continue
            remote_base = Path(root).joinpath(*rel)
            logger.info(f"媒体归档：开始上传 {item['title']} {item['season']} → {storage_type}:{remote_base.as_posix()}")
            ok_files, ok_size, failures = self._upload_tree(storage, local_dir, remote_base)
            total_files += ok_files
            total_size += ok_size
            if failures:
                stats["errors"].append(
                    f"{item['title']} {item['season']} → {storage_type} 有 {len(failures)} 个文件失败"
                )
                logger.error(f"媒体归档：{len(failures)} 个文件上传失败，例如 {failures[0]}")
                continue
            reached.append(f"{storage_type}:{root}")

        stats["uploaded_files"] += total_files
        stats["uploaded_size"] += total_size
        if reached:
            stats["archived"] += 1
            record = state["items"].setdefault(self._state_key(item["dir"], item["size"]), {
                "title": item["title"],
                "year": item["year"],
                "season": item["season"],
                "dir": item["dir"],
                "size": item["size"],
                "files": item["files"],
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "targets": {},
            })
            for target in reached:
                record["targets"][target] = {
                    "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "files": item["files"],
                }
            record["time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            stats["items"].append({
                "title": item["title"],
                "season": item["season"],
                "dir": item["dir"],
                "size": round(item["size"] / 1024 ** 3, 2),
                "idle": item["idle_days"],
                "status": "已归档",
                "detail": "、".join(reached),
            })
            logger.info(
                f"媒体归档：{item['title']} {item['season']} 完成，"
                f"{total_files} 个文件 {total_size / 1024 ** 3:.2f}GB"
            )

    def _relative_parts(self, dir_path: str) -> Optional[List[str]]:
        """把库内绝对路径换算成网盘上的相对结构，保持原有分类和季目录。"""
        for root in self._lib_roots:
            try:
                rel = Path(dir_path).relative_to(root)
            except ValueError:
                continue
            parts = [p for p in rel.parts if p not in (".", "")]
            if parts:
                return parts
        return None

    # ------------------------------------------------------------------ 上传

    @staticmethod
    def _storage(storage_type: str):
        spec = STORAGE_IMPL.get(storage_type)
        if not spec:
            return None
        module_name, class_name = spec
        try:
            module = __import__(module_name, fromlist=[class_name])
            cls = getattr(module, class_name)
            instance = cls()
            # rclone 的配置写在独立文件里，get_conf 可能为空，不能因此判成未配置
            if storage_type != "rclone" and hasattr(instance, "get_conf"):
                if not instance.get_conf():
                    logger.error(f"媒体归档：存储 {storage_type} 还没有配置，请先在 设置→存储 里完成授权")
                    return None
            return instance
        except Exception as err:  # noqa: BLE001
            logger.error(f"媒体归档：初始化存储 {storage_type} 失败 {err}")
            return None

    def _safe_item(self, storage, path: Path):
        try:
            return storage.get_item_strict(path)
        except Exception:  # noqa: BLE001
            return None

    def _ensure_dir(self, storage, path: Path, storage_type: str = ""):
        """逐级确保远端目录存在，返回最深一级的目录项。"""
        cache_key = f"{storage_type}:{path.as_posix()}"
        if cache_key in self._dir_cache:
            return self._dir_cache[cache_key]
        current = Path("/")
        node = self._safe_item(storage, current)
        if node is None:
            logger.error(f"媒体归档：远端根目录不可访问（{storage_type}）")
            return None
        for part in path.parts[1:]:
            current = current / part
            item = self._safe_item(storage, current)
            if item is None:
                try:
                    item = storage.create_folder(node, part)
                except Exception as err:  # noqa: BLE001
                    logger.error(f"媒体归档：创建远端目录 {current} 失败 {err}")
                    return None
                if item is None:
                    item = self._safe_item(storage, current)
                if item is None:
                    logger.error(f"媒体归档：创建远端目录后仍不可见 {current}")
                    return None
            node = item
        self._dir_cache[cache_key] = node
        return node

    def _upload_tree(self, storage, local_dir: Path, remote_base: Path) -> Tuple[int, int, List[str]]:
        """把本地目录按相同结构复制到远端，已存在且大小一致的文件跳过。"""
        files: List[Path] = []
        for root, dirnames, filenames in os.walk(local_dir):
            dirnames[:] = [d for d in dirnames if not d.startswith(".")]
            for name in filenames:
                if os.path.splitext(name)[1].lower() in SKIP_FILE_EXT:
                    continue
                full = Path(root) / name
                try:
                    if full.stat().st_size <= 0:
                        continue
                except OSError:
                    continue
                files.append(full)

        ok_files = 0
        ok_size = 0
        failures: List[str] = []
        storage_type = getattr(getattr(storage, "schema", None), "value", "")
        for index, local_file in enumerate(files, 1):
            rel = local_file.relative_to(local_dir)
            remote_file = remote_base / rel
            try:
                size = local_file.stat().st_size
            except OSError:
                continue
            existing = self._safe_item(storage, remote_file)
            if existing is not None:
                if getattr(existing, "size", None) == size:
                    ok_files += 1
                    ok_size += size
                    continue
                # 同名但大小不同：换源或换封装后的新版本，先删掉旧的再传，免得存储端拒绝或留双份
                try:
                    storage.delete(existing)
                    logger.info(f"媒体归档：远端旧文件已删除，准备重传 {remote_file.as_posix()}")
                except Exception as err:  # noqa: BLE001
                    logger.error(f"媒体归档：删除远端旧文件失败 {remote_file.as_posix()} {err}")
            parent = self._ensure_dir(storage, remote_file.parent, storage_type)
            if parent is None:
                failures.append(f"{rel}（远端目录不可创建）")
                continue
            try:
                uploaded = storage.upload(parent, local_file, remote_file.name)
            except Exception as err:  # noqa: BLE001
                logger.error(f"媒体归档：上传 {local_file.name} 失败 {err}")
                failures.append(f"{rel}（{err}）")
                continue
            if uploaded is None:
                failures.append(f"{rel}（存储返回空结果）")
                continue
            ok_files += 1
            ok_size += size
            if index % 10 == 0 or index == len(files):
                logger.info(
                    f"媒体归档：{remote_base.name} 进度 {index}/{len(files)}，"
                    f"已传 {ok_size / 1024 ** 3:.2f}GB"
                )
        return ok_files, ok_size, failures

    # ------------------------------------------------------------------ 输出

    def _save_run(self, stats: dict):
        try:
            self.save_data("last_run", stats)
        except Exception as err:  # noqa: BLE001
            logger.error(f"媒体归档：保存结果失败 {err}")

    def _notify_result(self, stats: dict):
        try:
            lines = [
                f"本轮候选 {stats['candidates']} 个，归档 {stats['archived']} 个",
                f"上传 {stats['uploaded_files']} 个文件，"
                f"{stats['uploaded_size'] / 1024 ** 3:.2f}GB",
                f"静默期 {self._quiet_days} 天｜本地文件保留不动",
            ]
            for item in stats["items"][:5]:
                lines.append(f"· {item['title']} {item['season']} {item['size']}GB → {item['detail']}")
            if stats.get("errors"):
                lines.append("异常：" + "；".join(stats["errors"][:3]))
            self.post_message(
                mtype=NotificationType.Plugin,
                title="【媒体归档】执行完成",
                text="\n".join(lines),
            )
        except Exception as err:  # noqa: BLE001
            logger.error(f"媒体归档：发送通知失败 {err}")

    @staticmethod
    def _stat_card(title: str, value: str, subtitle: str = "", color: str = "primary") -> dict:
        return {
            "component": "VCard",
            "props": {"variant": "tonal", "color": color, "class": "h-100"},
            "content": [
                {
                    "component": "VCardText",
                    "props": {"class": "py-3"},
                    "content": [
                        {"component": "div", "props": {"class": "text-caption"}, "text": title},
                        {"component": "div", "props": {"class": "text-h6"}, "text": value},
                        {"component": "div", "props": {"class": "text-caption"}, "text": subtitle},
                    ],
                }
            ],
        }

    def get_page(self) -> List[dict]:
        try:
            last: dict = self.get_data("last_run") or {}
            state: dict = self.get_data("archive_state") or {}
            archived_total = len((state.get("items") or {}))
            archived_size = sum(int((item or {}).get("size") or 0) for item in (state.get("items") or {}).values())
            cards = [
                self._stat_card("运行状态", "运行中" if self._enabled else "已停止",
                                f"周期 {self._cron or '未设置'}｜静默 {self._quiet_days} 天",
                                "success" if self._enabled else "warning"),
                self._stat_card("累计归档", f"{archived_total} 项",
                                f"约 {archived_size / 1024 ** 3:.2f} GB", "primary"),
                self._stat_card("上次候选", f"{int(last.get('candidates') or 0)} 个",
                                last.get("time") or "尚未执行过", "info"),
                self._stat_card("上次上传", f"{int(last.get('uploaded_files') or 0)} 文件",
                                f"{int(last.get('uploaded_size') or 0) / 1024 ** 3:.2f} GB｜"
                                f"归档 {int(last.get('archived') or 0)} 项",
                                "success" if last.get("archived") else "secondary"),
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

            if not self._parse_targets(quiet=True):
                blocks.append({
                    "component": "VAlert",
                    "props": {
                        "type": "warning",
                        "variant": "tonal",
                        "class": "mt-3",
                        "text": "还没有配置归档目标。请先在 设置→存储 里添加并授权 115 / OpenList，"
                                "再回到本插件填写「u115:/归档」「alist:/夸克归档」。",
                    },
                })

            items = last.get("items") or []
            if items:
                rows = [
                    [item.get("title", ""), item.get("season", ""),
                     f"{item.get('size', '')} GB", f"{item.get('idle', '')} 天",
                     item.get("status", ""), item.get("detail", "")]
                    for item in items[:30]
                ]
                blocks.append(self._table("上次归档明细", rows,
                                          ("片名", "季", "大小", "静默", "状态", "目标")))
            else:
                blocks.append({
                    "component": "VAlert",
                    "props": {
                        "type": "info",
                        "variant": "tonal",
                        "class": "mt-3",
                        "text": "还没有归档记录。开启插件后，整理入库满静默期、且没有活跃订阅的资源会被自动上传。"
                                "建议先开一次演练模式看候选。",
                    },
                })

            history = sorted(
                ((state.get("items") or {}).values()),
                key=lambda x: x.get("time") or "",
                reverse=True,
            )[:20]
            if history:
                rows = [
                    [item.get("title", ""), item.get("season", ""),
                     f"{int(item.get('size') or 0) / 1024 ** 3:.2f} GB",
                     "、".join((item.get("targets") or {}).keys()) or "-",
                     item.get("time", "")]
                    for item in history
                ]
                blocks.append(self._table("已归档清单", rows, ("片名", "季", "大小", "已传目标", "完成时间")))

            if last.get("errors"):
                blocks.append({
                    "component": "VAlert",
                    "props": {"type": "warning", "variant": "tonal", "class": "mt-3",
                              "text": "；".join(last["errors"][:5])},
                })
            return blocks
        except Exception as err:  # noqa: BLE001
            logger.error(f"媒体归档：渲染面板失败 {err}")
            return [{
                "component": "VAlert",
                "props": {"type": "error", "variant": "tonal", "text": f"面板渲染失败：{err}"},
            }]

    @staticmethod
    def _table(title: str, rows: List[List[str]], headers: Tuple[str, ...]) -> dict:
        return {
            "component": "VCard",
            "props": {"variant": "outlined", "class": "mt-3"},
            "content": [
                {"component": "VCardTitle", "props": {"class": "text-subtitle-1"}, "text": title},
                {
                    "component": "VTable",
                    "props": {"hover": True, "density": "compact"},
                    "content": [
                        {
                            "component": "thead",
                            "content": [{
                                "component": "tr",
                                "content": [
                                    {"component": "th", "props": {"class": "text-start text-caption"},
                                     "text": header}
                                    for header in headers
                                ],
                            }],
                        },
                        {
                            "component": "tbody",
                            "content": [
                                {
                                    "component": "tr",
                                    "content": [
                                        {"component": "td", "props": {"class": "text-caption"},
                                         "text": str(cell)}
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

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12, "md": 3},
                             "content": [{"component": "VSwitch", "props": {"model": "enabled", "label": "启用插件"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 3},
                             "content": [{"component": "VSwitch", "props": {"model": "notify", "label": "发送通知"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 3},
                             "content": [{"component": "VSwitch", "props": {"model": "onlyonce", "label": "立即运行一次"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 3},
                             "content": [{"component": "VSwitch", "props": {"model": "dryrun", "label": "演练模式（只列候选）"}}]},
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12, "md": 3},
                             "content": [{"component": "VTextField", "props": {
                                 "model": "cron", "label": "执行周期", "placeholder": "0 4 * * *"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 3},
                             "content": [{"component": "VTextField", "props": {
                                 "model": "quiet_days", "label": "静默天数",
                                 "placeholder": "7",
                                 "hint": "多少天没有新版本入库才认为定版",
                                 "persistent-hint": True}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 3},
                             "content": [{"component": "VTextField", "props": {
                                 "model": "min_size_mb", "label": "最小体积（MB）",
                                 "placeholder": "500"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 3},
                             "content": [{"component": "VTextField", "props": {
                                 "model": "max_items", "label": "单轮最多归档",
                                 "placeholder": "2",
                                 "hint": "上行有限时别一次性排太多",
                                 "persistent-hint": True}}]},
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12, "md": 4},
                             "content": [{"component": "VTextField", "props": {
                                 "model": "types", "label": "归档类型",
                                 "placeholder": "电影,电视剧"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 8},
                             "content": [{"component": "VTextField", "props": {
                                 "model": "library_roots", "label": "媒体库目录（留空自动扫描 /media）",
                                 "placeholder": "/media/电影,/media/国产剧",
                                 "hint": "网盘上的目录结构按这里的相对路径还原",
                                 "persistent-hint": True}}]},
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12},
                             "content": [{"component": "VTextarea", "props": {
                                 "model": "targets",
                                 "label": "归档目标（一行一个，格式 存储类型:/根路径）",
                                 "placeholder": "u115:/归档\nalist:/夸克归档",
                                 "rows": 3,
                                 "hint": "存储类型：u115 / alipan / alist / rclone。先到 设置→存储 里完成授权，"
                                         "这里只写类型和网盘上的根目录",
                                 "persistent-hint": True}}]},
                        ],
                    },
                    {
                        "component": "VAlert",
                        "props": {
                            "type": "info",
                            "variant": "tonal",
                            "class": "mt-3",
                            "text": "判定逻辑：整理历史里该片（剧集按季）最后一条成功整理时间距今超过静默天数；"
                                    "期间任何新版本入库都会重置计时，所以刚洗完版不会立刻上传。"
                                    "还在运行或暂停的订阅直接跳过。"
                                    "上传按文件逐个进行，远端已存在且大小一致就跳过，断点续传不用重来；"
                                    "本地文件一律保留，不做删除。",
                        },
                    },
                ],
            }
        ], {
            "enabled": False,
            "notify": True,
            "onlyonce": False,
            "dryrun": True,
            "cron": "0 4 * * *",
            "quiet_days": 7,
            "min_size_mb": 500,
            "max_items": 2,
            "types": "电影,电视剧",
            "targets": "u115:/归档",
            "library_roots": "",
        }
