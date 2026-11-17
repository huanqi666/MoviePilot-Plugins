# -*- coding: utf-8 -*-
"""洗版残留清理。

MoviePilot 的洗版只在**媒体库**里覆盖同名文件（整理覆盖模式 latest 时会删掉同集其它版本），
但下载目录里那一条旧版本的数据文件不会消失：整理用硬链接，旧文件在库内的链接被删掉后，
下载目录里的这份就只剩它自己（硬链接数 1），却还挂在 qB 里做种，于是同一集在磁盘上留了两份。

本插件按"媒体库为唯一事实"清理这些残留：
1. 扫描媒体库，建立「剧名 + 季 + 集 → 库内文件」索引；
2. 扫描下载目录，找硬链接数为 1 的视频文件，且该集在库里已有其它文件；
3. 过了保护期、且不是库里正在用的那份，就删除下载目录里的旧版本。

只删下载目录里的孤儿文件，媒体库文件不受影响（删除的是另一个硬链接，inode 仍被库里引用）。
残缺的下载任务由 MoviePilot 自带的下载器助手按「文件丢失」规则清理。
"""

import os
import re
import threading
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

from apscheduler.triggers.cron import CronTrigger

from app.domain.meta.metavideo import MetaVideo
from app.schemas import NotificationType
from app.sdk.logging import logger
from app.sdk.plugin import _PluginBase

VIDEO_EXT = {
    ".mp4", ".mkv", ".ts", ".m2ts", ".avi", ".mov", ".wmv",
    ".flv", ".rmvb", ".webm", ".iso", ".mpg", ".mpeg", ".m4v",
}
SKIP_DIRS = {"incomplete", "temp", ".recycle", "recycle", "lost+found", "@eadir", "#recycle"}
SEASON_DIR_RE = re.compile(r"(?:season|s)\s*0*(\d{1,2})", re.I)


class VersionCleaner(_PluginBase):
    """洗版残留清理。"""

    plugin_name = "洗版残留清理"
    plugin_desc = "洗版把媒体库换成新版本后，删掉下载目录里已被淘汰的旧版本文件，避免同一集占两份空间。"
    plugin_icon = "https://raw.githubusercontent.com/huanqi666/MoviePilot-Plugins/main/icons/versioncleaner.svg"
    plugin_version = "1.0.0"
    plugin_author = "huanqi666"
    author_url = "https://github.com/huanqi666/MoviePilot-Plugins"
    plugin_config_prefix = "versioncleaner_"
    plugin_order = 34
    auth_level = 1

    _enabled: bool = False
    _notify: bool = False
    _onlyonce: bool = False
    _dryrun: bool = False
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
    def _norm(text: Any) -> str:
        return re.sub(r"[^0-9a-z\u4e00-\u9fff]", "", str(text or "").lower())

    # ------------------------------------------------------------------ 扫描

    @staticmethod
    def _parse(path: str) -> Optional[Tuple[str, int, int]]:
        """解析出（剧名关键词集合, 季, 集）。"""
        stem = os.path.splitext(os.path.basename(path))[0]
        try:
            meta = MetaVideo(stem)
        except Exception:  # noqa: BLE001
            return None
        episodes = [e for e in (getattr(meta, "episode_list", None) or []) if e and e > 0]
        if not episodes:
            return None
        season = int(getattr(meta, "begin_season", 1) or 1)
        return str(getattr(meta, "name", "") or ""), season, int(episodes[0])

    def _library_index(self) -> Dict[Tuple[int, int], List[Tuple[Set[str], str, int]]]:
        """建立「季 + 集」到库内文件（含剧名关键词）的索引。"""
        min_bytes = self._min_file_mb * 1024 * 1024
        dl_abs = [os.path.abspath(p) for p in self._dl_roots]
        index: Dict[Tuple[int, int], List[Tuple[Set[str], str, int]]] = {}
        for root in self._lib_roots:
            root_abs = os.path.abspath(root)
            for current, dirs, files in os.walk(root_abs):
                dirs[:] = [d for d in dirs if not d.startswith(".") and d.lower() not in SKIP_DIRS]
                if any(current == d or current.startswith(d + os.sep) for d in dl_abs):
                    dirs[:] = []
                    continue
                for name in files:
                    if os.path.splitext(name)[1].lower() not in VIDEO_EXT:
                        continue
                    path = os.path.join(current, name)
                    try:
                        size = os.path.getsize(path)
                    except OSError:
                        continue
                    if size < min_bytes:
                        continue
                    parsed = self._parse(path)
                    if not parsed:
                        continue
                    meta_name, season, episode = parsed
                    folder = os.path.basename(current)
                    if SEASON_DIR_RE.search(folder):
                        show_dir = os.path.basename(os.path.dirname(current))
                    else:
                        show_dir = folder
                    tokens = {
                        self._norm(meta_name),
                        self._norm(show_dir),
                        self._norm(re.sub(r"[\(\[]?\d{4}[\)\]]?", "", show_dir)),
                    }
                    tokens = {t for t in tokens if len(t) >= 2 and not re.fullmatch(r"s\d+|season\d+|\d+", t)}
                    if not tokens:
                        continue
                    index.setdefault((season, episode), []).append((tokens, path, size))
        return index

    def _match(self, path: str, tokens_lib: Set[str]) -> bool:
        """下载文件是否属于库内同一部剧：看文件名/父目录/祖父目录是否包含剧名关键词。"""
        stem = os.path.splitext(os.path.basename(path))[0]
        folder = os.path.basename(os.path.dirname(path))
        parent = os.path.basename(os.path.dirname(os.path.dirname(path)))
        candidates = [self._norm(stem), self._norm(folder), self._norm(parent)]
        for token in tokens_lib:
            if len(token) < 2:
                continue
            for text in candidates:
                if token and token in text:
                    return True
        return False

    # ------------------------------------------------------------------ 主流程

    def check(self):
        """扫描下载目录，清理已被洗版淘汰的旧版本文件。"""
        started = datetime.now()
        stats: Dict[str, Any] = {
            "time": started.strftime("%Y-%m-%d %H:%M:%S"),
            "dryrun": self._dryrun,
            "library": 0,
            "candidates": 0,
            "deleted": 0,
            "freed": 0,
            "skipped": 0,
            "items": [],
            "errors": [],
        }
        try:
            index = self._library_index()
            stats["library"] = sum(len(v) for v in index.values())
            min_age = self._min_age_hours * 3600
            now = time.time()
            budget = self._max_files
            for root in self._dl_roots:
                for current, dirs, files in os.walk(root):
                    dirs[:] = [d for d in dirs if not d.startswith(".") and d.lower() not in SKIP_DIRS]
                    for name in files:
                        if budget <= 0:
                            break
                        if os.path.splitext(name)[1].lower() not in VIDEO_EXT:
                            continue
                        path = os.path.join(current, name)
                        try:
                            st = os.stat(path)
                        except OSError:
                            continue
                        if st.st_nlink != 1:
                            continue
                        if st.st_size < self._min_file_mb * 1024 * 1024:
                            continue
                        if min_age and now - st.st_mtime < min_age:
                            stats["skipped"] += 1
                            continue
                        parsed = self._parse(path)
                        if not parsed:
                            continue
                        _, season, episode = parsed
                        entries = index.get((season, episode)) or []
                        if not entries:
                            continue
                        hit = None
                        for tokens_lib, lib_path, _ in entries:
                            if lib_path == path:
                                continue
                            if self._match(path, tokens_lib):
                                hit = lib_path
                                break
                        if not hit:
                            continue
                        stats["candidates"] += 1
                        if self._dryrun:
                            logger.info(
                                f"洗版残留清理[演练]：{name}（{st.st_size / 1024 ** 3:.2f}GB，"
                                f"库内已有 {os.path.basename(hit)}）"
                            )
                        else:
                            try:
                                os.remove(path)
                                stats["deleted"] += 1
                                stats["freed"] += st.st_size
                                logger.info(
                                    f"洗版残留清理：已删除旧版 {name}"
                                    f"（{st.st_size / 1024 ** 3:.2f}GB，库内保留 {os.path.basename(hit)}）"
                                )
                            except OSError as err:
                                stats["errors"].append(f"{name}：{err}")
                                logger.error(f"洗版残留清理：删除 {name} 失败 {err}")
                                continue
                        budget -= 1
                        stats["items"].append(
                            {
                                "name": name[:80],
                                "size": round(st.st_size / 1024 ** 3, 2),
                                "path": current,
                                "library": os.path.basename(hit),
                            }
                        )
                    if budget <= 0:
                        break
                if budget <= 0:
                    logger.warning(f"洗版残留清理：本轮已达 {self._max_files} 个上限，剩余留待下次")
                    break
        except Exception as err:  # noqa: BLE001
            logger.error(f"洗版残留清理：执行异常 {err}")
            stats["errors"].append(str(err))

        self._save_run(stats)
        logger.info(
            f"洗版残留清理：库内索引 {stats['library']} 个文件，命中 {stats['candidates']} 个，"
            f"删除 {stats['deleted']} 个，释放 {stats['freed'] / 1024 ** 3:.2f}GB"
            f"（{'演练' if self._dryrun else '正式'}）"
        )
        if self._notify and (stats["deleted"] or stats["errors"]):
            self._notify_result(stats)

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
                f"模式：{'演练' if stats.get('dryrun') else '正式'}",
            ]
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
                self._stat_card("上次扫描", f"{int(last.get('library') or 0)} 个库文件",
                                last.get("time") or "尚未执行过", "primary"),
                self._stat_card("命中旧版本", f"{int(last.get('candidates') or 0)} 个",
                                f"跳过 {int(last.get('skipped') or 0)} 个未过保护期", "info"),
                self._stat_card("上次释放", f"{int(last.get('freed') or 0) / 1024 ** 3:.2f} GB",
                                f"删除 {int(last.get('deleted') or 0)} 个文件",
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
                    [item.get("name", ""), f"{item.get('size', '')} GB", item.get("library", "")]
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
                                                for header in ("删除的旧版文件", "大小", "库内保留")
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
                        "text": "上次巡检没有发现洗版残留：下载目录里没有已淘汰的旧版本文件。",
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
                             "content": [{"component": "VTextField", "props": {
                                 "model": "cron", "label": "执行周期", "placeholder": "0 5 * * *"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 3},
                             "content": [{"component": "VTextField", "props": {
                                 "model": "min_age_hours", "label": "保护期（小时）",
                                 "placeholder": "刚下载的文件不处理，默认 24"}}]},
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
                            "text": "只删除下载目录中「硬链接数=1 且媒体库已有同集」的旧版本视频文件，"
                                    "媒体库文件不受影响；残缺的下载任务由下载器助手按文件丢失规则清理。"
                                    "建议先开演练模式跑一轮确认。",
                        },
                    },
                ],
            }
        ], self._build_config()
