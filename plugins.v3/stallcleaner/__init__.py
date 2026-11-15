"""停滞下载清理插件。

定期扫描下载器中的未完成任务：若任务连续多分钟进度毫无变化、且当前下载速度低于阈值，
则按配置暂停或删除该任务，避免死种长期占用下载队列（qBittorrent 原生只按分享率/做种时长删种，
对下载阶段 0 速任务没有处置能力）。
"""

import time
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


class StallCleaner(_PluginBase):
    """停滞下载清理。"""

    # 插件名称
    plugin_name = "停滞下载清理"
    # 插件描述
    plugin_desc = "连续无进度且速度低于阈值的下载任务自动暂停或删除，防止死种长期占用队列。"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/huanqi666/MoviePilot-Plugins/main/icons/stallcleaner.svg"
    # 插件版本
    plugin_version = "1.1.0"
    # 插件作者
    plugin_author = "huanqi666"
    # 作者主页
    author_url = "https://github.com/huanqi666"
    # 插件配置项ID前缀
    plugin_config_prefix = "stallcleaner_"
    # 加载顺序
    plugin_order = 30
    # 可使用的用户级别
    auth_level = 2

    # 私有属性
    _scheduler: Optional[BackgroundScheduler] = None
    _enabled: bool = False
    _notify: bool = False
    _onlyonce: bool = False
    _dryrun: bool = False
    _cron: Optional[str] = None
    _downloaders: List[str] = []
    _stall_minutes: int = 60
    _min_speed: float = 10
    _grace_minutes: int = 30
    _action: str = "delete"
    _exclude_tags: str = "刷流"

    def init_plugin(self, config: dict = None):
        """读取配置并按需注册定时任务。"""
        if config:
            self._enabled = bool(config.get("enabled"))
            self._notify = bool(config.get("notify"))
            self._onlyonce = bool(config.get("onlyonce"))
            self._dryrun = bool(config.get("dryrun"))
            self._cron = config.get("cron")
            self._downloaders = config.get("downloaders") or []
            self._stall_minutes = int(config["stall_minutes"]) if config.get("stall_minutes") is not None else 60
            self._min_speed = float(config["min_speed"]) if config.get("min_speed") is not None else 10
            self._grace_minutes = int(config["grace_minutes"]) if config.get("grace_minutes") is not None else 30
            self._action = config.get("action") or "delete"
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
            logger.info("停滞下载清理：已安排立即执行一次")

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
            "action": self._action,
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

    # ------------------------------------------------------------------ 面板

    ACTION_TEXT = {
        "pause": "暂停任务",
        "delete": "删除任务（保留文件）",
        "deletefile": "删除任务及文件",
    }

    def get_page(self) -> List[dict]:
        """插件详情页：运行状态、上次结果、跟踪名单与执行历史。"""
        try:
            return self._build_page()
        except Exception as err:  # noqa: BLE001
            logger.error(f"停滞下载清理：渲染详情页失败 {err}")
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
        """拼装面板内容，数据来自插件本地存储，随每次打开实时重算。"""
        last: dict = self.get_data("last_run") or {}
        history: List[dict] = self.get_data("history") or []
        state: Dict[str, dict] = self.get_data("stall_state") or {}
        now = time.time()

        running = self.get_state()
        action_text = self.ACTION_TEXT.get(self._action, str(self._action))
        mode = "演练模式（只记录不处理）" if self._dryrun else "正式执行"

        cards = [
            self._stat_card(
                "运行状态",
                "运行中" if running else "已停止",
                f"周期 {self._cron}" if self._cron else "未设置执行周期",
                "success" if running else "warning",
            ),
            self._stat_card(
                "处置方式",
                action_text,
                mode,
                "info" if self._dryrun else "primary",
            ),
            self._stat_card(
                "判定阈值",
                f"{self._stall_minutes} 分钟",
                f"速度 ≤ {self._min_speed:.0f} KB/s · 保护期 {self._grace_minutes} 分钟",
                "secondary",
            ),
            self._stat_card(
                "上次命中",
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

        # 上次执行概况
        if last:
            scan_rows = [
                ["扫描任务", last.get("total", 0)],
                ["未完成任务", last.get("unfinished", 0)],
                ["豁免标签", last.get("exempt", 0)],
                ["保护期内", last.get("grace", 0)],
                ["持续无进度跟踪中", last.get("tracking", 0)],
                ["本次命中", last.get("hit", 0)],
                ["下载器", "、".join(last.get("downloaders") or []) or "无"],
            ]
            body: List[dict] = [self._table(["项目", "数值"], scan_rows)]
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

            # 上次命中明细
            items = last.get("items") or []
            if items:
                rows = [
                    [
                        (it.get("name") or "")[:60],
                        f"{it.get('stall', 0)} 分钟",
                        f"{it.get('speed', 0)} KB/s",
                        f"{it.get('progress', 0)}%",
                        it.get("result", "-"),
                    ]
                    for it in items[:30]
                ]
                blocks.append(
                    self._section(
                        "上次命中明细",
                        [self._table(["任务", "停滞", "速度", "进度", "处置结果"], rows)],
                        f"共 {len(items)} 条" + ("，仅显示前 30 条" if len(items) > 30 else ""),
                    )
                )

        # 当前跟踪名单：无进度但还没到阈值的任务
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
        tracking.sort(key=lambda x: -x[0])

        if tracking:
            rows = [
                [
                    (rec.get("name") or "")[:60],
                    f"{int(minutes)} 分钟",
                    f"{round(float(rec.get('progress') or 0) * 100, 1)}%",
                    f"{int(rec.get('speed') or 0)} KB/s",
                    "已达阈值" if minutes >= self._stall_minutes else "观察中",
                ]
                for minutes, rec in tracking[:15]
            ]
            blocks.append(
                self._section(
                    "当前无进度任务",
                    [self._table(["任务", "持续无进度", "进度", "速度", "状态"], rows)],
                    f"共 {len(tracking)} 个；达到 {self._stall_minutes} 分钟后按「{action_text}」处置"
                    + ("（演练模式不会真删）" if self._dryrun else ""),
                )
            )

        # 历史
        if history:
            rows = [
                [
                    h.get("time", "-"),
                    "演练" if h.get("dryrun") else "正式",
                    h.get("total", 0),
                    h.get("hit", 0),
                    ((h.get("items") or [{}])[0].get("name") or "")[:36] if h.get("hit") else "-",
                ]
                for h in history[:12]
            ]
            blocks.append(
                self._section(
                    "执行历史",
                    [self._table(["时间", "模式", "扫描", "命中", "最近一条命中任务"], rows)],
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
                    "id": "StallCleaner",
                    "name": "停滞下载清理服务",
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
            logger.error(f"停滞下载清理：停止服务出错 {err}")

    @property
    def service_infos(self) -> Optional[Dict[str, ServiceInfo]]:
        """已连接下载器实例。"""
        if not self._downloaders:
            logger.warning("停滞下载清理：尚未选择下载器")
            return None
        services = DownloaderHelper().get_services(name_filters=self._downloaders)
        if not services:
            logger.warning("停滞下载清理：获取下载器实例失败")
            return None
        active = {}
        for name, info in services.items():
            if not info.instance:
                continue
            if info.instance.is_inactive():
                logger.warning(f"停滞下载清理：下载器 {name} 未连接")
                continue
            active[name] = info
        return active or None

    def _save_run(self, record: dict):
        """保存本次结果，并追加进历史（最多保留 30 次）。"""
        try:
            self.save_data("last_run", record)
            history = self.get_data("history") or []
            if not isinstance(history, list):
                history = []
            history.insert(0, record)
            self.save_data("history", history[:30])
        except Exception as err:  # noqa: BLE001
            logger.error(f"停滞下载清理：保存执行记录失败 {err}")

    def check_stalled(self):
        """扫描所有下载器，处理停滞的下载任务，并记录结果供详情页展示。"""
        started_at = datetime.now()
        services = self.service_infos
        if not services:
            self._save_run(
                {
                    "time": started_at.strftime("%Y-%m-%d %H:%M:%S"),
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
        handle_total = 0
        messages: List[str] = []
        items: List[dict] = []
        errors: List[str] = []
        stats = {"total": 0, "unfinished": 0, "exempt": 0, "grace": 0, "tracking": 0}

        for dl_name, info in services.items():
            if info.type and str(info.type).lower() != "qbittorrent":
                logger.info(f"停滞下载清理：暂不支持下载器类型 {info.type}，跳过 {dl_name}")
                errors.append(f"{dl_name}：下载器类型 {info.type} 暂不支持")
                continue
            try:
                torrents, error = info.instance.get_torrents()
            except Exception as err:  # noqa: BLE001
                logger.error(f"停滞下载清理：读取 {dl_name} 种子失败 {err}")
                errors.append(f"{dl_name}：读取种子失败 {err}")
                continue
            if error:
                logger.warning(f"停滞下载清理：读取 {dl_name} 种子列表出错")
                errors.append(f"{dl_name}：读取种子列表出错")
                continue
            if not torrents:
                logger.info(f"停滞下载清理：{dl_name} 没有种子")
                continue
            logger.info(f"停滞下载清理：{dl_name} 共 {len(torrents)} 个任务，开始判定")

            for torrent in torrents:
                stats["total"] += 1
                try:
                    reason, result = self._inspect(torrent, state, new_state, exclude, now)
                except Exception as err:  # noqa: BLE001
                    logger.error(f"停滞下载清理：分析种子出错 {err}")
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
                    stats["tracking"] += 1
                    continue

                stall_minutes, speed_kb = result
                text = (
                    f"{torrent.name}｜停滞 {int(stall_minutes)} 分钟｜"
                    f"速度 {speed_kb:.0f} KB/s｜进度 {torrent.progress:.1%}"
                )
                if self._dryrun:
                    logger.info(f"停滞下载清理[演练]：{text}")
                    messages.append(f"[演练] {dl_name}：{text}")
                    handle_total += 1
                    items.append(
                        {
                            "name": torrent.name,
                            "stall": int(stall_minutes),
                            "speed": round(speed_kb),
                            "progress": round(float(torrent.progress) * 100, 1),
                            "result": "演练（未处理）",
                        }
                    )
                    continue
                ok = self._apply_action(info.instance, torrent.hash)
                if ok:
                    handle_total += 1
                    messages.append(f"{dl_name}：{text}")
                    logger.info(f"停滞下载清理：已处理 {text}")
                    new_state.pop(torrent.hash, None)
                    items.append(
                        {
                            "name": torrent.name,
                            "stall": int(stall_minutes),
                            "speed": round(speed_kb),
                            "progress": round(float(torrent.progress) * 100, 1),
                            "result": "已" + self.ACTION_TEXT.get(self._action, "处理"),
                        }
                    )

        self.save_data("stall_state", new_state)
        self._save_run(
            {
                "time": started_at.strftime("%Y-%m-%d %H:%M:%S"),
                "dryrun": self._dryrun,
                "action": self._action,
                "downloaders": list(services.keys()),
                "total": stats["total"],
                "unfinished": stats["unfinished"],
                "exempt": stats["exempt"],
                "grace": stats["grace"],
                "tracking": len(new_state),
                "hit": handle_total,
                "items": items,
                "error": "；".join(errors) if errors else "",
            }
        )

        if handle_total and self._notify:
            action_text = {"pause": "暂停", "delete": "删除任务", "deletefile": "删除任务及文件"}.get(
                self._action, self._action
            )
            self.post_message(
                mtype=NotificationType.SiteMessage,
                title=f"【停滞下载清理】已{action_text} {handle_total} 个任务",
                text="\n".join(messages[:30]),
            )
        elif handle_total:
            logger.info(f"停滞下载清理：本次处理 {handle_total} 个任务")
        else:
            logger.info("停滞下载清理：没有需要处理的任务")

    def _inspect(
        self,
        torrent: Any,
        state: Dict[str, dict],
        new_state: Dict[str, dict],
        exclude: set,
        now: float,
    ) -> Tuple[str, Optional[Tuple[float, float]]]:
        """判断单个任务状态。

        返回 (原因, 命中数据)：原因为 done/exempt/grace/tracking/hit，
        仅在 hit 时第二项为 (停滞分钟, 当前速度KB/s)。
        """
        # 只处理未完成任务
        if torrent.progress is None or torrent.progress >= 1:
            return "done", None
        # 豁免标签
        tags = {str(tag).strip() for tag in str(torrent.tags or "").split(",") if str(tag).strip()}
        if exclude and tags & exclude:
            return "exempt", None
        # 保护期：刚添加的任务不处理
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

    def _apply_action(self, downloader: Any, torrent_hash: str) -> bool:
        """按配置执行暂停或删除。"""
        try:
            if self._action == "pause":
                return bool(downloader.stop_torrents(ids=[torrent_hash]))
            if self._action == "deletefile":
                return bool(downloader.delete_torrents(delete_file=True, ids=[torrent_hash]))
            return bool(downloader.delete_torrents(delete_file=False, ids=[torrent_hash]))
        except Exception as err:  # noqa: BLE001
            logger.error(f"停滞下载清理：处理种子 {torrent_hash} 失败 {err}")
            return False

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
                                            "placeholder": "0 */1 * * *",
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
                                            "placeholder": "60",
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
                                        "component": "VSelect",
                                        "props": {
                                            "model": "action",
                                            "label": "处置动作",
                                            "items": [
                                                {"title": "删除任务（保留文件）", "value": "delete"},
                                                {"title": "删除任务及文件", "value": "deletefile"},
                                                {"title": "仅暂停", "value": "pause"},
                                            ],
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
                                            "model": "exclude_tags",
                                            "label": "豁免标签（逗号分隔）",
                                            "placeholder": "刷流,MOVIEPILOT",
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
                                            "text": "判定依据：同一任务进度连续 N 分钟无变化且速度低于阈值才处理；"
                                            "新任务在保护期内不处理；建议先用演练模式跑一轮确认名单。",
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
            "cron": "0 */1 * * *",
            "downloaders": [],
            "stall_minutes": 60,
            "min_speed": 10,
            "grace_minutes": 30,
            "action": "delete",
            "exclude_tags": "刷流",
        }
