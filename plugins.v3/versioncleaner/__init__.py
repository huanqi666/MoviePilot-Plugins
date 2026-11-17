# -*- coding: utf-8 -*-
"""洗版残留清理。

MoviePilot 洗版时只保证**媒体库**里不出现多版本：覆盖模式 latest 会覆盖同名目标，
新版本文件名不同时也会清掉同目录同季集的其它版本。但下载目录里那份旧数据不会消失
——整理用的是硬链接，库内链接被替换后，下载目录里的文件只剩自己（链接数 1），
却仍挂在下载器里做种，于是同一集在磁盘上留了两份。

v1.1.0 起改用**整理历史 + inode 校验**判定，不再靠剧名文本匹配：

1. 从 MoviePilot 整理历史取「下载路径 → 库内路径」的对应关系（成功记录）；
2. 下载文件仍存在、硬链接数为 1（说明库里已不再引用它）、过了保护期；
3. 库内目标文件已存在且**不是同一个 inode、内容也不同**（被洗版换成了新版本），
   或目标已不存在但同目录里已有同季集的其它版本 → 判定为被淘汰的旧版，删除。

剧名匹配的旧做法在「库内重命名成中文、发布名是英文」时会整批漏判
（实测 VisionQuest → 幻视任务 漏了 8 个文件 35.69GB），所以彻底换掉。

只删下载目录里的孤儿文件，媒体库文件不受影响。

v1.2.0 补上收尾：文件删空后，按整理历史里的 download_hash 把对应下载任务一起移除
（只删任务、不删文件），再清掉留下的空目录。否则种子目录还在、任务还挂在做种列表里，
下载器助手按「文件丢失」判定也不会命中。
"""

import os
import re
import sqlite3
import threading
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

from apscheduler.triggers.cron import CronTrigger

from app.schemas import NotificationType
from app.sdk.logging import logger
from app.sdk.plugin import _PluginBase

VIDEO_EXT = {
    ".mp4", ".mkv", ".ts", ".m2ts", ".avi", ".mov", ".wmv",
    ".flv", ".rmvb", ".webm", ".iso", ".mpg", ".mpeg", ".m4v",
}
SKIP_DIRS = {"incomplete", "temp", ".recycle", "recycle", "lost+found", "@eadir", "#recycle"}
ACTIVE_STATES = {
    "downloading", "metaDL", "forcedMetaDL", "allocating", "checkingDL",
    "checkingUP", "checkingResumeData", "moving", "queuedDL", "queuedUP", "forcedDL",
}
EPISODE_RE = re.compile(r"[Ss](\d{1,2})[\s._-]*[Ee](\d{1,3})")
DB_PATH = "/config/user.db"
SUCCESS_STATUS = 1
VALID_MODES = {"link", "copy", "move"}


class VersionCleaner(_PluginBase):
    """洗版残留清理。"""

    plugin_name = "洗版残留清理"
    plugin_desc = "按整理历史删除下载目录里已被洗版淘汰的旧版本文件，并清理文件已丢失的僵尸任务。"
    plugin_icon = "https://raw.githubusercontent.com/huanqi666/MoviePilot-Plugins/main/icons/versioncleaner.svg"
    plugin_version = "1.4.0"
    plugin_author = "huanqi666"
    author_url = "https://github.com/huanqi666/MoviePilot-Plugins"
    plugin_config_prefix = "versioncleaner_"
    plugin_order = 34
    auth_level = 1

    _enabled: bool = False
    _notify: bool = False
    _onlyonce: bool = False
    _dryrun: bool = False
    _remove_task: bool = True
    _prune_dirs: bool = True
    _remove_ghost: bool = True
    _cron: Optional[str] = "0 5 * * *"
    _min_age_hours: int = 24
    _min_file_mb: int = 100
    _max_files: int = 200
    _download_roots: str = ""
    _library_roots: str = ""

    def init_plugin(self, config: dict = None):
        """读取配置并按需安排立即执行。"""
        if config:
            self._enabled = bool(config.get("enabled"))
            self._notify = bool(config.get("notify"))
            self._onlyonce = bool(config.get("onlyonce"))
            self._dryrun = bool(config.get("dryrun"))
            self._remove_task = bool(config.get("remove_task", True))
            self._prune_dirs = bool(config.get("prune_dirs", True))
            self._remove_ghost = bool(config.get("remove_ghost", True))
            self._cron = str(config.get("cron") or "").strip() or None
            self._min_age_hours = self._as_int(config.get("min_age_hours"), 24, 0, 720)
            self._min_file_mb = self._as_int(config.get("min_file_mb"), 100, 0, 102400)
            self._max_files = self._as_int(config.get("max_files"), 200, 1, 5000)
            self._download_roots = str(config.get("download_roots") or "").strip()
            self._library_roots = str(config.get("library_roots") or "").strip()

        if self._onlyonce:
            self._onlyonce = False
            self.update_config(self._build_config())
            try:
                threading.Thread(target=self.check, name="VersionCleanerOnce", daemon=True).start()
                logger.info("洗版残留清理：已安排立即执行一次")
            except Exception as err:  # noqa: BLE001
                logger.error(f"洗版残留清理：安排立即执行失败 {err}")

    # ------------------------------------------------------------------ 基础

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
            "remove_task": self._remove_task,
            "prune_dirs": self._prune_dirs,
            "remove_ghost": self._remove_ghost,
            "cron": self._cron,
            "min_age_hours": self._min_age_hours,
            "min_file_mb": self._min_file_mb,
            "max_files": self._max_files,
            "download_roots": self._download_roots,
            "library_roots": self._library_roots,
        }

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
                    "id": "VersionCleaner",
                    "name": "洗版残留清理",
                    "trigger": CronTrigger.from_crontab(self._cron),
                    "func": self.check,
                    "kwargs": {},
                }
            ]
        return []

    def stop_service(self):
        """插件停用/重载时无需额外处理。"""
        return

    # ------------------------------------------------------------------ 路径

    @staticmethod
    def _split(value: str) -> List[str]:
        return [item.strip() for item in re.split(r"[,;\n]", value or "") if item.strip()]

    @property
    def _lib_roots(self) -> List[str]:
        configured = self._split(self._library_roots)
        if configured:
            return [p for p in configured if os.path.isdir(p)]
        base = "/media"
        roots: List[str] = []
        try:
            for entry in os.listdir(base):
                if entry.startswith(".") or entry.lower() in SKIP_DIRS:
                    continue
                path = os.path.join(base, entry)
                if os.path.isdir(path):
                    roots.append(path)
        except OSError:
            pass
        if os.path.isdir(base):
            roots.append(base)
        return roots

    @property
    def _dl_roots(self) -> List[str]:
        configured = self._split(self._download_roots)
        if configured:
            return [p for p in configured if os.path.isdir(p)]
        base = "/media/downloads"
        return [base] if os.path.isdir(base) else []

    @staticmethod
    def _under(path: str, roots: List[str]) -> bool:
        if not roots:
            return True
        target = os.path.abspath(path)
        for root in roots:
            base = os.path.abspath(root)
            if target == base or target.startswith(base + os.sep):
                return True
        return False

    # ------------------------------------------------------------------ 整理历史

    @staticmethod
    def _history_rows() -> Tuple[List[Tuple[str, str, str, str, str, str]], str]:
        """读取整理历史：(src, dest, mode, date, title, download_hash)，只取成功记录。"""
        if not os.path.isfile(DB_PATH):
            return [], f"找不到 MoviePilot 数据库 {DB_PATH}"
        try:
            conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=10)
        except sqlite3.Error as err:
            return [], f"读取整理历史失败：{err}"
        try:
            cursor = conn.execute(
                "select src, dest, mode, date, title, download_hash from transferhistory "
                "where status = ? and src is not null and dest is not null "
                "and src <> '' and dest <> ''",
                (SUCCESS_STATUS,),
            )
            rows = [
                (str(row[0] or ""), str(row[1] or ""), str(row[2] or ""),
                 str(row[3] or ""), str(row[4] or ""), str(row[5] or ""))
                for row in cursor.fetchall()
            ]
        except sqlite3.Error as err:
            return [], f"读取整理历史失败：{err}"
        finally:
            conn.close()
        return rows, ""

    @staticmethod
    def _same_file(src: os.stat_result, dest: os.stat_result) -> bool:
        """库内目标是否仍是这份数据：同 inode（硬链接），或复制模式下内容一致。"""
        if src.st_ino == dest.st_ino:
            return True
        if src.st_size != dest.st_size:
            return False
        return abs(src.st_mtime - dest.st_mtime) < 300

    @staticmethod
    def _sibling_exists(dest: str) -> bool:
        """库内目标已不在时，看库内是否已有同一部片子／同一季集的其它版本。"""
        folder = os.path.dirname(dest)
        try:
            names = os.listdir(folder)
        except OSError:
            return False
        videos = [
            name for name in names
            if os.path.splitext(name)[1].lower() in VIDEO_EXT
        ]
        if not videos:
            return False
        match = EPISODE_RE.search(os.path.basename(dest))
        if not match:
            # 电影：每部片子独占一个目录，目录里还有视频即说明这份已被新版本替换。
            # 洗版换封装格式时（.iso → .mkv）库内文件名会变，旧目标自然不存在，
            # 早先这里只认同季集，导致电影洗干净后旧版一直没人清。
            return True
        season, episode = match.group(1), match.group(2)
        for name in videos:
            other = EPISODE_RE.search(name)
            if not other:
                continue
            if other.group(1) == season and other.group(2) == episode:
                return True
        return False

    # ------------------------------------------------------------------ 主流程

    def check(self):
        """按整理历史扫描并清理已被洗版淘汰的旧版本文件。"""
        started = datetime.now()
        stats: Dict[str, Any] = {
            "time": started.strftime("%Y-%m-%d %H:%M:%S"),
            "dryrun": self._dryrun,
            "history": 0,
            "in_use": 0,
            "candidates": 0,
            "deleted": 0,
            "freed": 0,
            "tasks": 0,
            "ghosts": 0,
            "ghost_names": [],
            "pruned": 0,
            "skipped": 0,
            "unmatched": 0,
            "items": [],
            "errors": [],
        }
        try:
            rows, error = self._history_rows()
            if error:
                stats["errors"].append(error)
                logger.error(f"洗版残留清理：{error}")
            stats["history"] = len(rows)

            latest: Dict[str, Tuple[str, str, str, str]] = {}
            srcs_by_hash: Dict[str, List[str]] = {}
            for src, dest, mode, date, title, download_hash in rows:
                if download_hash:
                    srcs_by_hash.setdefault(download_hash, []).append(src)
                if mode and mode not in VALID_MODES:
                    continue
                prev = latest.get(src)
                if not prev or date >= prev[1]:
                    latest[src] = (dest, date, title, download_hash)

            dl_roots = self._dl_roots
            lib_roots = self._lib_roots
            min_age = self._min_age_hours * 3600
            min_bytes = self._min_file_mb * 1024 * 1024
            now = time.time()
            budget = self._max_files
            deleted_hashes: Set[str] = set()
            deleted_dirs: Set[str] = set()

            for src, (dest, _date, title, download_hash) in latest.items():
                if budget <= 0:
                    break
                if os.path.splitext(src)[1].lower() not in VIDEO_EXT:
                    continue
                if not self._under(src, dl_roots) or not self._under(dest, lib_roots):
                    continue
                try:
                    src_stat = os.stat(src)
                except OSError:
                    continue
                if not os.path.isfile(src):
                    continue
                if src_stat.st_nlink != 1:
                    stats["in_use"] += 1
                    continue
                if src_stat.st_size < min_bytes:
                    continue
                if min_age and now - src_stat.st_mtime < min_age:
                    stats["skipped"] += 1
                    continue

                reason = ""
                try:
                    dest_stat = os.stat(dest)
                except OSError:
                    dest_stat = None
                if dest_stat:
                    if self._same_file(src_stat, dest_stat):
                        stats["in_use"] += 1
                        continue
                    reason = "库内已换成新版本"
                elif self._sibling_exists(dest):
                    reason = "库内同集已换成新版本"
                else:
                    stats["unmatched"] += 1
                    continue

                stats["candidates"] += 1
                name = os.path.basename(src)
                if self._dryrun:
                    logger.info(
                        f"洗版残留清理[演练]：{name}（{src_stat.st_size / 1024 ** 3:.2f}GB，{reason}）"
                    )
                else:
                    try:
                        os.remove(src)
                        stats["deleted"] += 1
                        stats["freed"] += src_stat.st_size
                        if download_hash:
                            deleted_hashes.add(download_hash)
                        deleted_dirs.add(os.path.dirname(src))
                        logger.info(
                            f"洗版残留清理：已删除旧版 {name}"
                            f"（{src_stat.st_size / 1024 ** 3:.2f}GB，{reason}）"
                        )
                    except OSError as err:
                        stats["errors"].append(f"{name}：{err}")
                        logger.error(f"洗版残留清理：删除 {name} 失败 {err}")
                        continue
                budget -= 1
                stats["items"].append(
                    {
                        "name": name[:80],
                        "size": round(src_stat.st_size / 1024 ** 3, 2),
                        "reason": reason,
                        "library": os.path.basename(dest)[:60],
                        "title": str(title or "")[:40],
                    }
                )
            if budget <= 0:
                logger.warning(f"洗版残留清理：本轮已达 {self._max_files} 个上限，剩余留待下次")

            if not self._dryrun and deleted_hashes:
                if self._remove_task:
                    stats["tasks"] = self._cleanup_tasks(deleted_hashes, srcs_by_hash)
                if self._prune_dirs:
                    stats["pruned"] = self._prune_empty_dirs(deleted_dirs)

            if self._remove_ghost:
                stats["ghosts"], stats["ghost_names"] = self._cleanup_ghost_tasks()
        except Exception as err:  # noqa: BLE001
            logger.error(f"洗版残留清理：执行异常 {err}")
            stats["errors"].append(str(err))

        self._save_run(stats)
        logger.info(
            f"洗版残留清理：整理历史 {stats['history']} 条，库内在用 {stats['in_use']} 个，"
            f"命中 {stats['candidates']} 个，删除 {stats['deleted']} 个，"
            f"释放 {stats['freed'] / 1024 ** 3:.2f}GB，"
            f"移除任务 {stats['tasks']} 个，清理空目录 {stats['pruned']} 个，"
            f"移除僵尸任务 {stats['ghosts']} 个"
            f"（{'演练' if self._dryrun else '正式'}）"
        )
        if self._notify and (stats["deleted"] or stats["errors"] or stats.get("ghosts")):
            self._notify_result(stats)

    # ------------------------------------------------------------------ 收尾

    @staticmethod
    def _torrent_field(torrent: Any, key: str) -> str:
        if isinstance(torrent, dict):
            return str(torrent.get(key) or "")
        return str(getattr(torrent, key, "") or "")

    def _cleanup_tasks(self, hashes: Set[str], srcs_by_hash: Dict[str, List[str]]) -> int:
        """文件已清空的种子，把下载任务一并移除（只删任务，不再删文件）。"""
        pending = [
            item for item in hashes
            if item and srcs_by_hash.get(item)
            and all(not os.path.exists(path) for path in srcs_by_hash[item])
        ]
        if not pending:
            return 0
        try:
            from app.sdk.services import DownloaderHelper

            services = DownloaderHelper().get_services()
        except Exception as err:  # noqa: BLE001
            logger.warning(f"洗版残留清理：获取下载器失败，跳过移除任务 {err}")
            return 0
        removed = 0
        for dl_name, info in (services or {}).items():
            instance = getattr(info, "instance", None)
            if not instance:
                continue
            try:
                if instance.is_inactive():
                    logger.warning(f"洗版残留清理：下载器 {dl_name} 未连接，跳过")
                    continue
            except Exception:  # noqa: BLE001
                pass
            try:
                torrents, error = instance.get_torrents()
            except Exception as err:  # noqa: BLE001
                logger.warning(f"洗版残留清理：读取 {dl_name} 任务失败 {err}")
                continue
            if error:
                logger.warning(f"洗版残留清理：读取 {dl_name} 任务失败 {error}")
                continue
            ids = [
                self._torrent_field(torrent, "hash")
                for torrent in (torrents or [])
                if self._torrent_field(torrent, "hash") in pending
            ]
            if not ids:
                continue
            try:
                if instance.delete_torrents(delete_file=False, ids=ids):
                    removed += len(ids)
                    logger.info(
                        f"洗版残留清理：从 {dl_name} 移除空任务 {len(ids)} 个"
                        f"（文件已删，未再删文件）"
                    )
                else:
                    logger.warning(f"洗版残留清理：{dl_name} 移除任务返回失败")
            except Exception as err:  # noqa: BLE001
                logger.warning(f"洗版残留清理：{dl_name} 移除任务失败 {err}")
        return removed

    @staticmethod
    def _float_field(torrent: Any, key: str, default: float = 0.0) -> float:
        try:
            if isinstance(torrent, dict):
                value = torrent.get(key)
            else:
                value = getattr(torrent, key, None)
            return float(value)
        except (TypeError, ValueError):
            return default

    def _first_dir(self, raw: str) -> str:
        """把下载器视角的路径映射成容器内路径，返回第一个真实存在的目录。

        下载器和 MoviePilot 的挂载点不一定一致：qB 可能把 moviepilot 根目录挂成
        /downloads（于是路径变成 /downloads/downloads/电影/…），也可能直接挂下载目录。
        这里逐个剥掉开头的路径段去试，谁真实存在就用谁。
        """
        clean = (raw or "").strip().rstrip("/")
        if not clean:
            return ""
        segs = [part for part in clean.split("/") if part]
        roots = self._dl_roots or ["/media/downloads"]
        for skip in range(0, min(4, len(segs))):
            rest = "/".join(segs[skip:])
            for root in roots:
                candidate = os.path.join(root, rest) if rest else root
                if os.path.isdir(candidate):
                    return candidate
        return ""

    def _cleanup_ghost_tasks(self) -> Tuple[int, List[str]]:
        """已完成但数据文件已经不存在的任务，一律移除；只删任务，不动任何文件。

        僵尸任务的来源不只是本插件删文件：整部片子被手动删掉、库目录被清空、
        整理后旧目录被回收，都会留下「任务在做种、文件却没了」的条目，qB 会一直挂着。
        """
        try:
            from app.sdk.services import DownloaderHelper

            services = DownloaderHelper().get_services()
        except Exception as err:  # noqa: BLE001
            logger.warning(f"洗版残留清理：获取下载器失败，跳过僵尸任务清理 {err}")
            return 0, []
        if not self._dl_roots:
            logger.warning("洗版残留清理：下载目录不可用，跳过僵尸任务清理")
            return 0, []
        min_age = self._min_age_hours * 3600
        now = time.time()
        removed = 0
        names: List[str] = []
        for dl_name, info in (services or {}).items():
            instance = getattr(info, "instance", None)
            if not instance:
                continue
            try:
                if instance.is_inactive():
                    logger.warning(f"洗版残留清理：下载器 {dl_name} 未连接，跳过僵尸任务清理")
                    continue
            except Exception:  # noqa: BLE001
                pass
            try:
                torrents, error = instance.get_torrents()
            except Exception as err:  # noqa: BLE001
                logger.warning(f"洗版残留清理：读取 {dl_name} 任务失败 {err}")
                continue
            if error:
                logger.warning(f"洗版残留清理：读取 {dl_name} 任务失败 {error}")
                continue
            ids: List[str] = []
            for torrent in (torrents or []):
                torrent_hash = self._torrent_field(torrent, "hash")
                if not torrent_hash:
                    continue
                # 只在「已完成」的任务里找：下载中/校验中/移动中的文件缺失是正常过程
                if self._float_field(torrent, "progress", 1.0) < 0.999:
                    continue
                if self._torrent_field(torrent, "state") in ACTIVE_STATES:
                    continue
                content = self._torrent_field(torrent, "content_path")
                save_raw = self._torrent_field(torrent, "save_path")
                if not content:
                    continue
                save_host = self._first_dir(save_raw)
                # 保存目录本身都找不到：多半是挂载掉线或整块目录被搬走，不判
                if not save_host:
                    continue
                save_raw = save_raw.rstrip("/")
                if not content.rstrip("/").startswith(save_raw):
                    continue
                content_host = save_host + content.rstrip("/")[len(save_raw):]
                if os.path.exists(content_host):
                    continue
                added = self._float_field(torrent, "added_on", 0.0)
                if min_age and (not added or now - added < min_age):
                    continue
                ids.append(torrent_hash)
                names.append(self._torrent_field(torrent, "name")[:60])
            if not ids:
                continue
            if self._dryrun:
                logger.info(
                    f"洗版残留清理[演练]：{dl_name} 有 {len(ids)} 个文件已丢失的任务，"
                    f"如 {names[0] if names else ''}"
                )
                continue
            try:
                if instance.delete_torrents(delete_file=False, ids=ids):
                    removed += len(ids)
                    logger.info(
                        f"洗版残留清理：从 {dl_name} 移除文件已丢失的任务 {len(ids)} 个"
                        f"（只删任务，不动文件）"
                    )
                else:
                    logger.warning(f"洗版残留清理：{dl_name} 移除僵尸任务返回失败")
            except Exception as err:  # noqa: BLE001
                logger.warning(f"洗版残留清理：{dl_name} 移除僵尸任务失败 {err}")
        return removed, names

    def _prune_empty_dirs(self, dirs: Set[str]) -> int:
        """删掉清理后留下的空目录，最多向上回到下载根。"""
        roots = [os.path.abspath(path) for path in self._dl_roots]
        if not roots:
            return 0
        removed = 0
        for folder in sorted(dirs, key=len, reverse=True):
            current = os.path.abspath(folder)
            while current not in roots and self._under(current, roots):
                try:
                    os.rmdir(current)
                except OSError:
                    break
                removed += 1
                current = os.path.dirname(current)
        if removed:
            logger.info(f"洗版残留清理：清理空目录 {removed} 个")
        return removed

    # ------------------------------------------------------------------ 输出

    def _save_run(self, record: dict):
        try:
            self.save_data("last_run", record)
        except Exception as err:  # noqa: BLE001
            logger.error(f"洗版残留清理：保存结果失败 {err}")

    def _notify_result(self, stats: dict):
        try:
            lines = [
                f"命中 {stats['candidates']} 个旧版本，删除 {stats['deleted']} 个，"
                f"释放 {stats['freed'] / 1024 ** 3:.2f}GB",
                f"移除下载任务 {int(stats.get('tasks') or 0)} 个，"
                f"清理空目录 {int(stats.get('pruned') or 0)} 个",
                f"模式：{'演练' if stats.get('dryrun') else '正式'}",
            ]
            if stats.get("ghosts"):
                names = "、".join((stats.get("ghost_names") or [])[:3])
                lines.append(f"移除僵尸任务 {int(stats['ghosts'])} 个（文件已不存在）：{names}")
            if stats.get("errors"):
                lines.append("异常：" + "；".join(stats["errors"][:3]))
            self.post_message(
                mtype=NotificationType.Plugin,
                title="【洗版残留清理】执行完成",
                text="\n".join(lines),
            )
        except Exception as err:  # noqa: BLE001
            logger.error(f"洗版残留清理：发送通知失败 {err}")

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
            cards = [
                self._stat_card("运行状态", "运行中" if self._enabled else "已停止",
                                f"周期 {self._cron or '未设置'}", "success" if self._enabled else "warning"),
                self._stat_card("整理历史", f"{int(last.get('history') or 0)} 条",
                                last.get("time") or "尚未执行过", "primary"),
                self._stat_card("库内仍在用", f"{int(last.get('in_use') or 0)} 个",
                                f"跳过 {int(last.get('skipped') or 0)} 个未过保护期", "info"),
                self._stat_card("上次释放", f"{int(last.get('freed') or 0) / 1024 ** 3:.2f} GB",
                                f"删除 {int(last.get('deleted') or 0)} 个文件，"
                                f"移除 {int(last.get('tasks') or 0)} 个空任务 / "
                                f"{int(last.get('ghosts') or 0)} 个僵尸任务",
                                "error" if last.get("deleted") else "success"),
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
                    [item.get("name", ""), f"{item.get('size', '')} GB",
                     item.get("reason", ""), item.get("library", "")]
                    for item in items[:30]
                ]
                blocks.append(
                    {
                        "component": "VCard",
                        "props": {"variant": "outlined", "class": "mt-3"},
                        "content": [
                            {"component": "VCardTitle", "props": {"class": "text-subtitle-1"},
                             "text": "上次清理明细"},
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
                                                for header in ("删除的旧版文件", "大小", "判定依据", "库内现用文件")
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
                )
            else:
                blocks.append({
                    "component": "VAlert",
                    "props": {
                        "type": "success",
                        "variant": "tonal",
                        "text": "上次巡检没有发现洗版残留：整理历史里记录的下载文件都还在被媒体库引用。",
                    },
                })
            if last.get("errors"):
                blocks.append({
                    "component": "VAlert",
                    "props": {"type": "warning", "variant": "tonal", "class": "mt-3",
                              "text": "；".join(last["errors"][:5])},
                })
            return blocks
        except Exception as err:  # noqa: BLE001
            logger.error(f"洗版残留清理：渲染详情页失败 {err}")
            return [{
                "component": "VAlert",
                "props": {"type": "error", "variant": "tonal", "text": f"面板渲染失败：{err}"},
            }]

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
                             "content": [{"component": "VSwitch", "props": {"model": "dryrun", "label": "演练模式"}}]},
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12, "md": 3},
                             "content": [{"component": "VSwitch", "props": {
                                 "model": "remove_task", "label": "顺带移除空任务"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 3},
                             "content": [{"component": "VSwitch", "props": {
                                 "model": "prune_dirs", "label": "清理空目录"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 3},
                             "content": [{"component": "VSwitch", "props": {
                                 "model": "remove_ghost", "label": "移除僵尸任务"}}]},
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12, "md": 3},
                             "content": [{"component": "VTextField", "props": {
                                 "model": "cron", "label": "执行周期", "placeholder": "0 5 * * *"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 3},
                             "content": [{"component": "VTextField", "props": {
                                 "model": "min_age_hours", "label": "保护期（小时）",
                                 "placeholder": "刚整理的文件不处理，默认 24"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 3},
                             "content": [{"component": "VTextField", "props": {
                                 "model": "min_file_mb", "label": "最小文件（MB）", "placeholder": "默认 100"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 3},
                             "content": [{"component": "VTextField", "props": {
                                 "model": "max_files", "label": "单轮最多删除", "placeholder": "默认 200"}}]},
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12, "md": 6},
                             "content": [{"component": "VTextField", "props": {
                                 "model": "download_roots", "label": "下载目录",
                                 "placeholder": "留空自动使用 /media/downloads，多个用逗号分隔"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 6},
                             "content": [{"component": "VTextField", "props": {
                                 "model": "library_roots", "label": "媒体库目录",
                                 "placeholder": "留空自动扫描 /media，多个用逗号分隔"}}]},
                        ],
                    },
                    {
                        "component": "VAlert",
                        "props": {
                            "type": "info",
                            "variant": "tonal",
                            "class": "mt-3",
                            "text": "判定依据是 MoviePilot 的整理历史加 inode 校验：下载文件硬链接数=1，"
                                    "而库里对应的目标已换成别的文件（或同目录已有新版本），才删。"
                                    "不靠剧名文本比对，库内重命名成中文也不会漏判；"
                                    "删完按 download_hash 把已清空的下载任务一并移除（只删任务不删文件），"
                                    "再清掉留下的空目录。"
                                    "另外每轮会扫一遍下载器：已完成、但数据文件已经不存在的任务"
                                    "（不管是本插件删的，还是你手动删的）也一并移除，避免做种列表挂着空任务。"
                                    "媒体库文件不受影响。建议先开演练模式跑一轮确认。",
                        },
                    },
                ],
            }
        ], self._build_config()
