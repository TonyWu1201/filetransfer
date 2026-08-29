"""局域网点对点文件传输工具。"""

import time
from pathlib import Path

__version__ = "1.1.2"

DEFAULT_PORT = 54546
DISCOVERY_PORT = 54545
CHUNK_SIZE = 1024 * 1024


def default_out_dir() -> Path:
    """默认接收保存目录: receive/<YYYYMMDD>。"""
    return Path("receive") / time.strftime("%Y%m%d")

_SIZE_UNITS = ("B", "KB", "MB", "GB", "TB")


def format_size(num: float) -> str:
    """将字节数格式化为自适应单位（B/KB/MB/GB/TB）。"""
    num = float(num)
    for unit in _SIZE_UNITS:
        if num < 1024 or unit == _SIZE_UNITS[-1]:
            return f"{int(num)} {unit}" if unit == "B" else f"{num:.1f} {unit}"
        num /= 1024
    return f"{num:.1f} TB"


def format_size_pair(sent: int, total: int) -> str:
    """按 total 的自适应单位格式化 已发送/总大小。"""
    n = float(total)
    for i, unit in enumerate(_SIZE_UNITS):
        if n < 1024 or unit == _SIZE_UNITS[-1]:
            if i == 0:
                return f"{int(sent)}/{int(total)} B"
            scale = 1024**i
            return f"{sent / scale:.1f}/{total / scale:.1f} {unit}"
        n /= 1024
    return f"{sent:.1f}/{total:.1f} TB"


def format_speed(bytes_per_sec: float) -> str:
    """将字节/秒格式化为自适应速度单位（B/s、KB/s、MB/s...）。"""
    return format_size(bytes_per_sec) + "/s"
