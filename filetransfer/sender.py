import os
import socket
from pathlib import Path

from . import CHUNK_SIZE, DEFAULT_PORT, format_size
from .protocol import PROTOCOL_VERSION, ProtocolError, recv_frame, send_frame


class TransferError(Exception):
    pass


def _iter_entries(paths: list[Path]):
    for p in paths:
        p = Path(p)
        if p.is_dir():
            root = p.parent
            yield root, p, True
            for walk_root, dirs, files in os.walk(p):
                walk_path = Path(walk_root)
                for d in sorted(dirs):
                    yield root, walk_path / d, True
                for f in sorted(files):
                    yield root, walk_path / f, False
        elif p.is_file():
            yield p.parent, p, False
        else:
            raise TransferError(f"路径不存在或无法访问: {p}")


def send_transfer(
    host: str,
    paths: list[Path],
    port: int = DEFAULT_PORT,
    name: str | None = None,
    progress=None,
    log=None,
    timeout: float = 10.0,
):
    name = name or socket.gethostname()
    total_size = 0
    entries = []
    for root, file, is_dir in _iter_entries(paths):
        rel = str(file.relative_to(root))
        if is_dir:
            entries.append(("dir", rel, 0))
        else:
            size = file.stat().st_size
            entries.append(("file", file, rel, size))
            total_size += size

    if log:
        log(f"待发送: {len(entries)} 项, 共 {format_size(total_size)}")

    with socket.create_connection((host, port), timeout=timeout) as sock:
        sock.settimeout(None)
        send_frame(sock, {"type": "hello", "name": name, "version": PROTOCOL_VERSION})
        reply = recv_frame(sock)
        if reply.get("type") != "accept":
            raise TransferError(f"对方拒绝接收: {reply.get('reason', '未知原因')}")

        sent = 0
        for entry in entries:
            if entry[0] == "dir":
                send_frame(sock, {"type": "dir", "path": entry[1]})
                continue
            _, file, rel, size = entry
            send_frame(
                sock,
                {"type": "file", "path": rel, "size": size, "mtime": file.stat().st_mtime},
            )
            with open(file, "rb") as fh:
                remaining = size
                while remaining > 0:
                    chunk = fh.read(min(CHUNK_SIZE, remaining))
                    if not chunk:
                        raise TransferError(f"文件读取不完整: {file}")
                    sock.sendall(chunk)
                    sent += len(chunk)
                    remaining -= len(chunk)
                    if progress:
                        progress(sent, total_size, rel, size - remaining, size)
        send_frame(sock, {"type": "done"})
        reply = recv_frame(sock)
        if reply.get("type") != "ack":
            raise TransferError("接收方未确认传输完成")
        if log:
            log(f"发送完成: 共 {format_size(total_size)}")
    return total_size
