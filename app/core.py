import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import exifread
import ffmpeg
import pandas as pd
from dateutil import parser
from dateutil.tz import gettz
from dotenv import load_dotenv
from loguru import logger
from tqdm import tqdm

load_dotenv()


def to_beijing_timestamp(time_str: str) -> str:
    """
    将任意格式时间字符串解析并转换为北京时间（YYYYmmddHHMMSS）。
    支持：EXIF (YYYY:MM:DD HH:MM:SS), 中文时间格式, ISO, Unix 秒/毫秒, 自然语言。

    2018:03:04 10:35:51上午
    2018:05:25 21:23:28下午
    """
    try:
        # ✅ 1. EXIF 相关格式
        if isinstance(time_str, str) and len(time_str) >= 19:
            # 检查基本格式 YYYY:MM:DD
            if time_str[4] == ":" and time_str[7] == ":" and time_str[10] == " ":
                # ✅ 1b. 优先检查中文 AM/PM 格式：2018:03:04 10:35:51上午
                if "上午" in time_str or "下午" in time_str:
                    try:
                        # 方法1：直接替换（可能失败的24小时制+下午格式）
                        time_clean = time_str.replace("上午", " AM").replace(
                            "下午", " PM"
                        )
                        dt = datetime.strptime(time_clean, "%Y:%m:%d %I:%M:%S %p")
                        dt = dt.replace(tzinfo=gettz("Asia/Shanghai"))
                        return dt.strftime("%Y%m%d%H%M%S")
                    except ValueError:
                        # 方法2：正则解析，处理错误的24小时制+AM/PM格式
                        match = re.match(
                            r'(\d{4}):(\d{2}):(\d{2}) (\d{2}):(\d{2}):(\d{2})(上午|下午)',
                            time_str,
                        )
                        if match:
                            year, month, day, hour, minute, second, ampm = (
                                match.groups()
                            )
                            hour = int(hour)

                            # ✅ 处理错误格式：21:23:28下午
                            if ampm == "下午":
                                if hour < 12:
                                    hour += 12  # 正常情况：1-11点下午 +12
                                # 12-23点下午保持不变（错误格式）
                            elif ampm == "上午":
                                if hour == 12:
                                    hour = 0  # 12点上午 = 0点
                                # 1-11点上午保持不变

                            dt = datetime(
                                int(year),
                                int(month),
                                int(day),
                                hour,
                                int(minute),
                                int(second),
                            )
                            dt = dt.replace(tzinfo=gettz("Asia/Shanghai"))
                            return dt.strftime("%Y%m%d%H%M%S")

                # ✅ 1a. 标准格式：2024:12:13 20:28:39
                else:
                    dt = datetime.strptime(time_str, "%Y:%m:%d %H:%M:%S")
                    dt = dt.replace(tzinfo=gettz("Asia/Shanghai"))
                    return dt.strftime("%Y%m%d%H%M%S")

        # ✅ 2. Unix 时间戳（秒或毫秒）
        if isinstance(time_str, str) and time_str.isdigit():
            if len(time_str) == 10:
                dt = datetime.fromtimestamp(int(time_str), tz=gettz("Asia/Shanghai"))
                return dt.strftime("%Y%m%d%H%M%S")
            elif len(time_str) == 13:
                dt = datetime.fromtimestamp(
                    int(time_str) / 1000, tz=gettz("Asia/Shanghai")
                )
                return dt.strftime("%Y%m%d%H%M%S")

        # ✅ 3. 其他格式（ISO、自然语言）使用 parser
        dt = parser.parse(time_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=gettz("Asia/Shanghai"))
        return dt.astimezone(gettz("Asia/Shanghai")).strftime("%Y%m%d%H%M%S")

    except Exception:
        return "ERROR"


class Pivor:
    TARGET_IMAGES = {".jpg", ".jpeg", ".png", ".cr2", ".arw"}  # , ".heic"
    TARGET_VIDEOS = {".mov", ".mp4", ".avi", ".mkv"}

    def __init__(self, root=None):
        root = root or os.getenv('PV_ROOT')
        self.root = Path(root)
        self.process_dir = self.root / "__process"
        self.process_dir.mkdir(parents=True, exist_ok=True)
        self.archive = self.root / 'archive'
        self.archive.mkdir(parents=True, exist_ok=True)
        self.duplicates_dir = self.root / "duplicates"
        self.duplicates_dir.mkdir(parents=True, exist_ok=True)
        self.snapshot_dir = self.root / "snapshot"
        self.snapshot_dir.mkdir(parents=True, exist_ok=True)

        self.logs_dir = Path(".logs")
        self.logs_dir.mkdir(exist_ok=True)

    def _setup_logger(self, log_name: str):
        """设置日志配置"""
        logger.remove()  # 清除之前的handler
        logger.add(
            log := self.logs_dir / f"{log_name}.log",
            level="INFO",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
            rotation="10 MB",
            encoding="utf-8",
        )
        return log

    def _iter_dir(self, work_dir: str | Path):
        if isinstance(work_dir, str):
            work_dir = Path(work_dir)
        if not work_dir.exists() or not work_dir.is_dir():
            raise ValueError(f"Invalid {work_dir=}")
        for f in work_dir.rglob("*"):
            if (
                f.suffix.lower() in self.TARGET_IMAGES | self.TARGET_VIDEOS
                and not f.name.startswith("._")
            ):
                yield f

    def stats(self):
        """
        统计文件信息并返回DataFrame
        Returns:
            pd.DataFrame: 包含月份、照片数量、视频数量、总计的DataFrame
                        最后两行是duplicates和snapshot的统计
        """
        data = []

        # 统计 archive 目录（按年月分组）
        if self.archive.exists():
            for year_month_dir in self.archive.iterdir():
                if (
                    year_month_dir.is_dir()
                    and year_month_dir.name.isdigit()
                    and len(year_month_dir.name) == 6
                ):
                    p_count = 0
                    v_count = 0
                    for pv_dir in year_month_dir.iterdir():
                        if pv_dir.is_dir():
                            if pv_dir.name == 'p':
                                p_count = len([
                                    f for f in pv_dir.iterdir() if f.is_file()
                                ])
                            elif pv_dir.name == 'v':
                                v_count = len([
                                    f for f in pv_dir.iterdir() if f.is_file()
                                ])

                    data.append([
                        year_month_dir.name,
                        p_count,
                        v_count,
                        p_count + v_count,
                    ])

        # 统计 duplicates 目录
        dup_count = 0
        if self.duplicates_dir.exists():
            dup_count = len([f for f in self.duplicates_dir.iterdir() if f.is_file()])

        # 统计 snapshot 目录
        snap_count = 0
        if self.snapshot_dir.exists():
            for item in self.snapshot_dir.rglob('*'):
                if item.is_file():
                    snap_count += 1

        # 添加最后两行
        data.append(['dup', 0, 0, dup_count])
        data.append(['snap', 0, 0, snap_count])

        # 创建DataFrame
        df = pd.DataFrame(data, columns=['month', 'p', 'v', 'total'])

        # 按月份排序，最后两行保持原位置
        if len(data) > 2:
            archive_rows = df.iloc[:-2].sort_values('month')
            last_rows = df.iloc[-2:]
            df = pd.concat([archive_rows, last_rows], ignore_index=True)

        return df

    def preview(self, work_dir: str | Path) -> Dict[Path, Path]:
        return {f: self.rename(f) for f in self._iter_dir(work_dir)}

    def fit(self, work_dir: str | Path = None, handle_duplicate=True):
        if isinstance(work_dir, str):
            work_dir = Path(work_dir)
        if work_dir is None:
            work_dir = self.process_dir
        if not work_dir.exists():
            print(f'- Not exists: {str(work_dir)}')
            return

        log_file = self._setup_logger(work_dir.stem)

        info = '\n'.join([
            '\n',
            f"- 开始处理目录: {work_dir}",
            f"- 归档目录: {self.archive}",
            f"- 快照目录: {self.snapshot_dir}",
            f"- 重名目录: {self.duplicates_dir}",
            '\n',
        ])
        logger.info(info)
        print(info)

        files_to_process = list(self._iter_dir(work_dir))
        total_files = len(files_to_process)

        logger.info(f"- 找到 {total_files} 个待处理文件")

        success_count = 0
        duplicate_count = 0
        snapshot_count = 0
        error_count = 0

        with tqdm(files_to_process, desc="processing", unit="f") as pbar:
            for fp in pbar:
                try:
                    x = self.rename(fp)
                    ts, model, fn = x.stem.split("_")
                    pv = "p" if x.suffix.lower() in self.TARGET_IMAGES else "v"
                    new_fp = self.archive / ts[:6] / pv / x.name

                    # 检查目标文件是否已存在
                    if new_fp.exists():
                        logger.warning(f"已存在: {fp.name} -> {new_fp}")
                        if handle_duplicate:
                            duplicate_count += 1
                            dup_1 = self.duplicates_dir / new_fp.name
                            dup_2 = self.duplicates_dir / fp.name
                            new_fp.rename(dup_1)
                            new_fp = dup_2
                            logger.warning(f"{dup_1=}")
                            logger.warning(f"{dup_2=}")
                        else:
                            logger.warning(f"pass: {str(fp)}")
                            continue
                    elif model == "UNKNOWN":
                        logger.warning(f"snapshot: {fp.name} -> {new_fp}")
                        new_fp = self.snapshot_dir / new_fp.name
                        snapshot_count += 1

                    # 移动文件
                    new_fp.parent.mkdir(parents=True, exist_ok=True)
                    fp.rename(new_fp)
                    success_count += 1

                    logger.info(f"✓ 成功处理: {fp.name} -> {new_fp}")

                except Exception as e:
                    error_count += 1
                    logger.error(f"✗ 处理失败: {fp.name} - {str(e)}")

                pbar.set_postfix({
                    "成功": success_count,
                    "快照": snapshot_count,
                    "重名": duplicate_count,
                    "错误": error_count,
                })

        summary = '\n'.join([
            '\n',
            '# 处理完成汇总/统计:\n',
            f'- 成功处理/总文件数: {success_count} / {total_files}',
            f'- 归档文件: {success_count - snapshot_count}',
            f'- 快照文件: {snapshot_count}',
            f'- 重名文件: {duplicate_count}',
            f'- 处理失败: {error_count}',
            f'- 日志文件: {log_file}',
            '\n',
        ])

        logger.info(summary)
        print(summary)

    def check(self):
        for f in self._iter_dir(self.archive):
            ...

    def recover(self):
        for f in self.duplicates_dir.rglob('*'):
            if len(parts := f.stem.split('_')) == 4:
                f.rename(f.parent / f'{parts[2]}{f.suffix}')

    def rename(self, file: str | Path, mv: bool = False) -> Path:
        if isinstance(file, str):
            file = Path(file)
        this_name = file.stem.split("_")
        if len(this_name) == 3 and re.fullmatch(r"\d{14}", this_name[0]):
            return file
        meta = self._extract_metadata(file)
        time_str = to_beijing_timestamp(meta["time"])
        model_clean = meta["model"].replace(" ", "-")
        fname_clean = file.stem.replace("_", "-").replace(" ", "-")
        new_name = file.parent / f"{time_str}_{model_clean}_{fname_clean}{file.suffix}"
        return file.rename(new_name) if mv else new_name

    def _extract_metadata(self, file: str | Path) -> Optional[Dict[str, Any]]:
        if isinstance(file, str):
            file = Path(file)
        meta = {"time": None, "model": "UNKNOWN"}
        suffix = file.suffix.lower()

        # 📷 照片：读取 EXIF，返回原始时间字符串
        if suffix in self.TARGET_IMAGES:
            try:
                with open(file, "rb") as f:
                    tags = exifread.process_file(f, details=False)
                if _time := tags.get("EXIF DateTimeOriginal") or tags.get(
                    "Image DateTime"
                ):
                    meta["time"] = str(_time)
                if _model := tags.get("Image Model"):
                    meta["model"] = str(_model).strip()
            except Exception:
                pass

        # 🎥 视频：读取 FFmpeg，返回原始时间字符串
        elif suffix in self.TARGET_VIDEOS:
            try:
                probe = ffmpeg.probe(file)
                tags = probe.get("format", {}).get("tags", {})

                if _time := tags.get("creation_time"):
                    meta["time"] = str(_time)
                if _model := (
                    tags.get("com.apple.quicktime.model") or tags.get("major_brand")
                ):
                    meta["model"] = str(_model).strip()
            except Exception:
                pass

        if not meta["time"]:
            meta["time"] = datetime.fromtimestamp(file.stat().st_mtime).strftime(
                "%Y%m%d%H%M%S"
            )
        return meta


def compare_stats(stats_before, stats_after):
    """使用箭头标记显示变化"""
    data = []

    # 合并数据
    before_dict = stats_before.set_index('month').to_dict('index')
    after_dict = stats_after.set_index('month').to_dict('index')
    all_months = set(before_dict.keys()) | set(after_dict.keys())

    for month in sorted(all_months):
        before = before_dict.get(month, {'p': 0, 'v': 0, 'total': 0})
        after = after_dict.get(month, {'p': 0, 'v': 0, 'total': 0})

        p_change = after['p'] - before['p']
        v_change = after['v'] - before['v']
        total_change = after['total'] - before['total']

        # 创建标记字符串
        p_str = str(after['p'])
        if p_change > 0:
            p_str += f" ↑{p_change}"
        elif p_change < 0:
            p_str += f" ↓{abs(p_change)}"

        v_str = str(after['v'])
        if v_change > 0:
            v_str += f" ↑{v_change}"
        elif v_change < 0:
            v_str += f" ↓{abs(v_change)}"

        total_str = str(after['total'])
        if total_change > 0:
            total_str += f" ↑{total_change}"
        elif total_change < 0:
            total_str += f" ↓{abs(total_change)}"

        # 只包含有变化的行
        if p_change != 0 or v_change != 0 or total_change != 0:
            data.append([month, p_str, v_str, total_str])

    return pd.DataFrame(data, columns=['month', 'p', 'v', 'total'])
