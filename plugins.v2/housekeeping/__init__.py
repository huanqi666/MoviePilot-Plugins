"""系统垃圾清理插件。

定期清理 MoviePilot 运行产生的可安全重建数据：
- 图片/接口缓存（config/cache，超过阈值才清，避免海报被反复重拉）
- 运行临时文件（config/temp）
- 旧日志（轮转日志 + 超过保留天数的订阅/插件日志，永不删当前 moviepilot.log）
- 调试残留（config/tmp_*.html、tmp_*.json 等）

设计原则：只动 /config 下的白名单子路径，绝不触碰 user.db、app.env、cookies、plugins。
所有删除均支持演练模式，且每次执行都会记录明细，便于回溯。
"""

import os
import re
import shutil
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.schemas import NotificationType
from app.sdk.config import settings
from app.sdk.logging import logger
from app.sdk.plugin import _PluginBase

# 当前正在写入的日志，任何情况下都不删
CURRENT_LOG = "moviepilot.log"
# 绝不允许整目录清空的保护名单（即使配置误开也会被跳过）
PROTECTED_NAMES = {"user.db", "user.db-wal", "user.db-shm", "app.env", "cookies", "plugins",
                   "cookie_sync.py", "plugin_transactions", "logs"}
MB = 1024 * 1024


class HouseKeeping(_PluginBase):
    """系统垃圾清理。"""

    plugin_name = "系统垃圾清理"
    plugin_desc = "定时清理 MoviePilot 的图片缓存、临时文件、旧日志与调试残留，只动可安全重建的数据。"
    plugin_icon = "https://raw.githubusercontent.com/huanqi666/MoviePilot-Plugins/main/icons/housekeeping.svg"
    plugin_version = "1.1.0"
    plugin_author = "huanqi666"
    author_url = "https://github.com/huanqi666"
    plugin_config_prefix = "housekeeping_"
    plugin_order = 31
    auth_level = 2

    _scheduler: Optional[BackgroundScheduler] = None
    _enabled: bool = False
    _notify: bool = True
    _onlyonce: bool = False
    _dryrun: bool = False
    _cron: str = "0 4 * * 1"
    _clean_cache: bool = True
    _cache_threshold_mb: int = 200
    _clean_temp: bool = True
    _clean_logs: bool = True
    _log_keep_days: int = 7
    _clean_debug: bool = True

    def init_plugin(self, config: dict = None):
        """读取配置，按需立即执行一次。"""
        if config:
            self._enabled = bool(config.get("enabled"))
            self._notify = bool(config.get("notify", True))
            self._onlyonce = bool(config.get("onlyonce"))
            self._dryrun = bool(config.get("dryrun"))
            self._cron = config.get("cron") or "0 4 * * 1"
            self._clean_cache = bool(config.get("clean_cache", True))
            self._cache_threshold_mb = self._to_int(config.get("cache_threshold_mb"), 200)
            self._clean_temp = bool(config.get("clean_temp", True))
            self._clean_logs = bool(config.get("clean_logs", True))
            self._log_keep_days = self._to_int(config.get("log_keep_days"), 7)
            self._clean_debug = bool(config.get("clean_debug", True))

        self.stop_service()

        if self._onlyonce:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            self._scheduler.add_job(
                func=self.clean,
                trigger="date",
                run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
            )
            self._onlyonce = False
            self.update_config(self._build_config())
            if self._scheduler.get_jobs():
                self._scheduler.start()
            logger.info("系统垃圾清理：已安排立即执行一次")

    @staticmethod
    def _to_int(value: Any, default: int) -> int:
        """配置里的 0 是合法值，不能用 or 兜底。"""
        if value is None or value == "":
            return default
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    def _build_config(self) -> dict:
        return {
            "enabled": self._enabled,
            "notify": self._notify,
            "onlyonce": self._onlyonce,
            "dryrun": self._dryrun,
            "cron": self._cron,
            "clean_cache": self._clean_cache,
            "cache_threshold_mb": self._cache_threshold_mb,
            "clean_temp": self._clean_temp,
            "clean_logs": self._clean_logs,
            "log_keep_days": self._log_keep_days,
            "clean_debug": self._clean_debug,
        }

    def get_state(self) -> bool:
        return bool(self._enabled and self._cron)

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        pass

    # ------------------------------------------------------------------ 面板

    def get_page(self) -> List[dict]:
        """插件详情页：运行状态、上次清理结果、当前占用与执行历史。"""
        try:
            return self._build_page()
        except Exception as err:  # noqa: BLE001
            logger.error(f"系统垃圾清理：渲染详情页失败 {err}")
            return [
                {
                    "component": "VAlert",
                    "props": {"type": "error", "variant": "tonal", "text": f"面板渲染失败：{err}"},
                }
            ]

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

    @staticmethod
    def _section(title: str, children: List[dict], subtitle: str = "") -> dict:
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
                                    "text": h,
                                }
                                for h in headers
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
                                {"component": "td", "props": {"class": "text-caption"}, "text": str(cell)}
                                for cell in row
                            ],
                        }
                        for row in rows
                    ],
                },
            ],
        }

    def _save_run(self, record: dict):
        """保存本次结果，并追加进历史（最多保留 30 次）。"""
        try:
            self.save_data("last_result", record)
            history = self.get_data("history") or []
            if not isinstance(history, list):
                history = []
            history.insert(0, record)
            self.save_data("history", history[:30])
        except Exception as err:  # noqa: BLE001
            logger.error(f"系统垃圾清理：保存执行记录失败 {err}")

    def _build_page(self) -> List[dict]:
        last: dict = self.get_data("last_result") or {}
        history: List[dict] = self.get_data("history") or []
        root = self.config_root()
        running = self.get_state()

        enabled_items = [
            label
            for label, flag in (
                ("缓存", self._clean_cache),
                ("临时文件", self._clean_temp),
                ("旧日志", self._clean_logs),
                ("调试残留", self._clean_debug),
            )
            if flag
        ]

        cards = [
            self._stat_card(
                "运行状态",
                "运行中" if running else "已停止",
                f"周期 {self._cron}" if self._cron else "未设置执行周期",
                "success" if running else "warning",
            ),
            self._stat_card(
                "执行模式",
                "演练（只记录）" if self._dryrun else "正式清理",
                ("清理项：" + "、".join(enabled_items)) if enabled_items else "未勾选任何清理项",
                "info" if self._dryrun else "primary",
            ),
            self._stat_card(
                "缓存阈值",
                f"{self._cache_threshold_mb} MB",
                f"日志保留 {self._log_keep_days} 天",
                "secondary",
            ),
            self._stat_card(
                "上次释放",
                self._fmt(int(last.get("size") or 0)),
                last.get("time") or "尚未执行过",
                "success",
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

        # 上次执行
        if last:
            body: List[dict] = [
                self._table(
                    ["项目", "数值"],
                    [
                        ["清理文件数", last.get("files", 0)],
                        ["释放空间", self._fmt(int(last.get("size") or 0))],
                        ["缓存+临时+日志占用", f"{self._fmt(int(last.get('before') or 0))} → {self._fmt(int(last.get('after') or 0))}"],
                    ],
                )
            ]
            details = last.get("details") or []
            if details:
                body.append(
                    {
                        "component": "div",
                        "props": {"class": "text-caption mt-2"},
                        "text": "明细：" + "；".join(str(d) for d in details),
                    }
                )
            blocks.append(
                self._section(
                    "上次执行",
                    body,
                    f"{last.get('time', '-')} · {'演练' if last.get('dryrun') else '正式清理'}",
                )
            )

        # 当前占用（实时统计）
        usage_rows = []
        for label, sub, flag in (
            ("图片/接口缓存", "cache", self._clean_cache),
            ("运行临时文件", "temp", self._clean_temp),
            ("日志", "logs", self._clean_logs),
        ):
            path = root / sub
            usage_rows.append([label, self._fmt(self._size(path)), self._count(path), "启用" if flag else "未启用"])
        blocks.append(
            self._section(
                "当前占用",
                [self._table(["目录", "占用", "文件数", "清理项"], usage_rows)],
                "打开面板时实时统计，不修改任何文件",
            )
        )

        # 历史
        if history:
            rows = [
                [
                    h.get("time", "-"),
                    "演练" if h.get("dryrun") else "正式",
                    h.get("files", 0),
                    self._fmt(int(h.get("size") or 0)),
                    f"{self._fmt(int(h.get('before') or 0))} → {self._fmt(int(h.get('after') or 0))}",
                ]
                for h in history[:12]
            ]
            blocks.append(
                self._section(
                    "执行历史",
                    [self._table(["时间", "模式", "文件数", "释放", "占用变化"], rows)],
                    f"最近 {min(len(history), 12)} 次（最多保留 30 次）",
                )
            )
        else:
            blocks.append(
                self._section(
                    "执行历史",
                    [
                        {
                            "component": "div",
                            "props": {"class": "text-caption text-medium-emphasis"},
                            "text": "还没有执行记录。可打开「立即运行一次」开关跑一轮，结果会显示在这里。",
                        }
                    ],
                )
            )

        return blocks

    def get_service(self) -> List[Dict[str, Any]]:
        """注册定时清理服务。"""
        if self.get_state() and self._cron:
            return [
                {
                    "id": "HouseKeeping",
                    "name": "系统垃圾清理服务",
                    "trigger": CronTrigger.from_crontab(self._cron),
                    "func": self.clean,
                    "kwargs": {},
                }
            ]
        return []

    def stop_service(self):
        try:
            if self._scheduler:
                if self._scheduler.running:
                    self._scheduler.shutdown()
                self._scheduler = None
        except Exception as err:  # noqa: BLE001
            logger.error(f"系统垃圾清理：停止服务出错 {err}")

    # ------------------------------------------------------------------ 工具

    @staticmethod
    def config_root() -> Path:
        root = getattr(settings, "CONFIG_PATH", None) or os.environ.get("CONFIG_PATH") or "/config"
        return Path(str(root))

    @staticmethod
    def _fmt(size: int) -> str:
        if size >= 1024 ** 3:
            return f"{size / 1024 ** 3:.2f} GB"
        if size >= MB:
            return f"{size / MB:.1f} MB"
        if size >= 1024:
            return f"{size / 1024:.1f} KB"
        return f"{size} B"

    @classmethod
    def _size(cls, path: Path) -> int:
        """目录或文件的字节数。"""
        if not path.exists():
            return 0
        if path.is_file():
            try:
                return path.stat().st_size
            except OSError:
                return 0
        total = 0
        for dirpath, _, filenames in os.walk(path):
            for name in filenames:
                try:
                    total += os.path.getsize(os.path.join(dirpath, name))
                except OSError:
                    continue
        return total

    @classmethod
    def _count(cls, path: Path) -> int:
        if path.is_file():
            return 1
        n = 0
        for _, _, filenames in os.walk(path):
            n += len(filenames)
        return n

    def _purge_dir(self, target: Path, dryrun: bool) -> Tuple[int, int]:
        """清空目录内容（保留目录本身），返回 (文件数, 字节数)。"""
        if not target.exists() or not target.is_dir():
            return 0, 0
        if target.name in PROTECTED_NAMES:
            logger.warning(f"系统垃圾清理：{target} 在保护名单内，跳过")
            return 0, 0
        files = 0
        size = 0
        try:
            entries = list(os.scandir(target))
        except OSError as err:
            logger.error(f"系统垃圾清理：读取 {target} 失败 {err}")
            return 0, 0
        for entry in entries:
            if entry.name in PROTECTED_NAMES:
                continue
            path = Path(entry.path)
            try:
                if entry.is_dir(follow_symlinks=False):
                    size += self._size(path)
                    files += self._count(path)
                    if not dryrun:
                        shutil.rmtree(path, ignore_errors=True)
                else:
                    size += entry.stat().st_size
                    files += 1
                    if not dryrun:
                        os.remove(path)
            except OSError as err:
                logger.warning(f"系统垃圾清理：删除 {path} 失败 {err}")
        return files, size

    def _purge_logs(self, dryrun: bool) -> Tuple[int, int]:
        """删除轮转日志与超过保留天数的日志，保留当前 moviepilot.log。"""
        logs = self.config_root() / "logs"
        if not logs.exists():
            return 0, 0
        cutoff = time.time() - self._log_keep_days * 86400
        files = 0
        size = 0
        for dirpath, _, filenames in os.walk(logs):
            for name in filenames:
                if name == CURRENT_LOG:
                    continue
                path = Path(dirpath) / name
                try:
                    stat = path.stat()
                except OSError:
                    continue
                rotated = bool(re.search(r"\.log\.\d+$", name)) or name.endswith(".gz")
                expired = stat.st_mtime < cutoff
                if not (rotated or expired):
                    continue
                files += 1
                size += stat.st_size
                if not dryrun:
                    try:
                        os.remove(path)
                    except OSError as err:
                        logger.warning(f"系统垃圾清理：删除日志 {path} 失败 {err}")
        return files, size

    def _purge_debug(self, dryrun: bool) -> Tuple[int, int]:
        """清理 /config 根下调试遗留的临时文件。"""
        root = self.config_root()
        files = 0
        size = 0
        patterns = ("tmp_*.html", "tmp_*.json", "*.tmp", "tmp_*.log")
        for pattern in patterns:
            for path in root.glob(pattern):
                if not path.is_file() or path.name in PROTECTED_NAMES:
                    continue
                try:
                    stat = path.stat()
                except OSError:
                    continue
                files += 1
                size += stat.st_size
                if not dryrun:
                    try:
                        os.remove(path)
                        logger.info(f"系统垃圾清理：删除调试残留 {path.name}")
                    except OSError as err:
                        logger.warning(f"系统垃圾清理：删除 {path} 失败 {err}")
        return files, size

    # ------------------------------------------------------------------ 主流程

    def clean(self):
        """执行一次清理。"""
        root = self.config_root()
        if not root.exists():
            logger.error(f"系统垃圾清理：配置目录不存在 {root}")
            return

        tag = "[演练] " if self._dryrun else ""
        details: List[str] = []
        total_files = 0
        total_size = 0
        before = self._size(root / "cache") + self._size(root / "temp") + self._size(root / "logs")
        logger.info(f"系统垃圾清理：{tag}开始，配置目录 {root}")

        if self._clean_cache:
            cache = root / "cache"
            cache_size = self._size(cache)
            threshold = self._cache_threshold_mb * MB
            if cache_size >= threshold:
                n, s = self._purge_dir(cache, self._dryrun)
                total_files += n
                total_size += s
                details.append(f"图片/接口缓存：{n} 个文件，{self._fmt(s)}")
                logger.info(f"系统垃圾清理：{tag}缓存 {self._fmt(cache_size)} >= 阈值 {self._cache_threshold_mb}MB，已清理 {n} 个文件")
            else:
                details.append(f"图片/接口缓存：{self._fmt(cache_size)}，未达阈值 {self._cache_threshold_mb}MB，跳过")
                logger.info(f"系统垃圾清理：缓存 {self._fmt(cache_size)} 未达阈值，跳过")

        if self._clean_temp:
            n, s = self._purge_dir(root / "temp", self._dryrun)
            total_files += n
            total_size += s
            details.append(f"运行临时文件：{n} 个文件，{self._fmt(s)}")

        if self._clean_logs:
            n, s = self._purge_logs(self._dryrun)
            total_files += n
            total_size += s
            details.append(f"旧日志（保留 {self._log_keep_days} 天）：{n} 个文件，{self._fmt(s)}")

        if self._clean_debug:
            n, s = self._purge_debug(self._dryrun)
            total_files += n
            total_size += s
            details.append(f"调试残留：{n} 个文件，{self._fmt(s)}")

        after = self._size(root / "cache") + self._size(root / "temp") + self._size(root / "logs")
        summary = (
            f"{tag}共清理 {total_files} 个文件，释放 {self._fmt(total_size)}\n"
            + "\n".join(details)
            + f"\n缓存+临时+日志占用：{self._fmt(before)} -> {self._fmt(after)}"
        )
        logger.info(f"系统垃圾清理：{tag}完成\n{summary}")

        self._save_run({
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "dryrun": self._dryrun,
            "files": total_files,
            "size": total_size,
            "before": before,
            "after": after,
            "details": details,
        })

        if self._notify and total_files:
            self.post_message(
                mtype=NotificationType.SiteMessage,
                title=f"【系统垃圾清理】释放 {self._fmt(total_size)}",
                text=summary,
            )

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """插件配置表单。"""
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
                                "content": [{"component": "VSwitch", "props": {"model": "enabled", "label": "启用插件"}}],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [{"component": "VSwitch", "props": {"model": "notify", "label": "发送通知"}}],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [{"component": "VSwitch", "props": {"model": "onlyonce", "label": "立即运行一次"}}],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [{"component": "VSwitch", "props": {"model": "dryrun", "label": "演练模式（只记录不删除）"}}],
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
                                        "component": "VTextField",
                                        "props": {
                                            "model": "cron",
                                            "label": "执行周期（cron）",
                                            "placeholder": "0 4 * * 1",
                                            "hint": "默认每周一 04:00。常用：0 4 * * * 每天；0 4 1 * * 每月 1 号",
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
                                            "model": "cache_threshold_mb",
                                            "label": "缓存清理阈值(MB)",
                                            "type": "number",
                                            "hint": "缓存超过该值才清，避免海报被反复重拉，0 表示每次都清",
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
                                            "model": "log_keep_days",
                                            "label": "日志保留天数",
                                            "type": "number",
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
                                "content": [{"component": "VSwitch", "props": {"model": "clean_cache", "label": "清理图片/接口缓存"}}],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [{"component": "VSwitch", "props": {"model": "clean_temp", "label": "清理运行临时文件"}}],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [{"component": "VSwitch", "props": {"model": "clean_logs", "label": "清理旧日志"}}],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [{"component": "VSwitch", "props": {"model": "clean_debug", "label": "清理调试残留"}}],
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
                                            "text": "只清理 config 下可安全重建的数据；user.db、app.env、cookies、plugins 与媒体库、下载目录一律不碰。",
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                ],
            }
        ], {
            "enabled": True,
            "notify": True,
            "onlyonce": False,
            "dryrun": False,
            "cron": "0 4 * * 1",
            "clean_cache": True,
            "cache_threshold_mb": 200,
            "clean_temp": True,
            "clean_logs": True,
            "log_keep_days": 7,
            "clean_debug": True,
        }
