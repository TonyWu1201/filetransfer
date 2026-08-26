import os
import queue
import socket
import threading
from pathlib import Path

from . import CHUNK_SIZE, DEFAULT_PORT, format_size
from .protocol import PROTOCOL_VERSION, ProtocolError, recv_frame, send_frame

SPLIT_THRESHOLD = 32 * 1024 * 1024


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


def _build_tasks(files: list, n_streams: int) -> list:
    tasks = []
    for _, file, rel, size in files:
        if n_streams > 1 and size >= SPLIT_THRESHOLD:
            part = (size + n_streams - 1) // n_streams
            part = ((part + CHUNK_SIZE - 1) // CHUNK_SIZE) * CHUNK_SIZE
            offset = 0
            while offset < size:
                tasks.append((file, rel, size, offset, min(part, size - offset)))
                offset += part
        else:
            tasks.append((file, rel, size, 0, size))
    return tasks


def _send_stream(sock: socket.socket, tasks: list, total_size: int, progress, log) -> int:
    sent = 0
    with sock:
        for file, rel, size, offset, length in tasks:
            send_frame(
                sock,
                {
                    "type": "file",
                    "path": rel,
                    "size": length,
                    "total_size": size,
                    "offset": offset,
                    "mtime": file.stat().st_mtime,
                },
            )
            with open(file, "rb") as fh:
                fh.seek(offset)
                remaining = length
                while remaining > 0:
                    chunk = fh.read(min(CHUNK_SIZE, remaining))
                    if not chunk:
                        raise TransferError(f"文件读取不完整: {file}")
                    sock.sendall(chunk)
                    sent += len(chunk)
                    remaining -= len(chunk)
                    if progress:
                        progress(sent, total_size, rel, offset + length - remaining, size)
        send_frame(sock, {"type": "done"})
        reply = recv_frame(sock)
        if reply.get("type") != "ack":
            raise TransferError("接收方未确认传输完成")
    if log:
        log(f"发送完成: 共 {format_size(total_size)}")
    return total_size


def _send_parallel(conns: list, tasks: list, total_size: int, progress, log) -> int:
    q: queue.Queue = queue.Queue()
    for task in tasks:
        q.put(task)
    for _ in conns:
        q.put(None)

    lock = threading.Lock()
    sent = [0]
    errors: list = []

    def worker(conn: socket.socket) -> None:
        try:
            while True:
                task = q.get()
                if task is None:
                    send_frame(conn, {"type": "done"})
                    reply = recv_frame(conn)
                    if reply.get("type") != "ack":
                        raise TransferError("接收方未确认传输完成")
                    return
                file, rel, size, offset, length = task
                send_frame(
                    conn,
                    {
                        "type": "file",
                        "path": rel,
                        "size": length,
                        "total_size": size,
                        "offset": offset,
                        "mtime": file.stat().st_mtime,
                    },
                )
                with open(file, "rb") as fh:
                    fh.seek(offset)
                    remaining = length
                    while remaining > 0:
                        chunk = fh.read(min(CHUNK_SIZE, remaining))
                        if not chunk:
                            raise TransferError(f"文件读取不完整: {file}")
                        conn.sendall(chunk)
                        with lock:
                            sent[0] += len(chunk)
                        remaining -= len(chunk)
                        if progress:
                            progress(sent[0], total_size, rel, offset + length - remaining, size)
        except BaseException as exc:
            errors.append(exc)
            for c in conns:
                try:
                    c.close()
                except OSError:
                    pass

    threads = [threading.Thread(target=worker, args=(c,), daemon=True) for c in conns]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    for c in conns:
        try:
            c.close()
        except OSError:
            pass
    if errors:
        exc = errors[0]
        raise exc if isinstance(exc, TransferError) else TransferError(str(exc))
    if log:
        log(f"发送完成: 共 {format_size(total_size)}")
    return total_size


def send_transfer(
    host: str,
    paths: list[Path],
    port: int = DEFAULT_PORT,
    name: str | None = None,
    progress=None,
    log=None,
    timeout: float = 10.0,
    streams: int = 4,
):
    name = name or socket.gethostname()
    entries = []
    total_size = 0
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

    sock = socket.create_connection((host, port), timeout=timeout)
    sock.settimeout(None)
    conns = [sock]
    try:
        send_frame(
            sock,
            {"type": "hello", "name": name, "version": PROTOCOL_VERSION, "streams": streams},
        )
        reply = recv_frame(sock)
        if reply.get("type") != "accept":
            raise TransferError(f"对方拒绝接收: {reply.get('reason', '未知原因')}")

        dirs = [e for e in entries if e[0] == "dir"]
        files = [e for e in entries if e[0] == "file"]

        n_streams = max(1, min(int(reply.get("streams", 1)), streams))
        tasks = _build_tasks(files, n_streams)
        n_conns = min(n_streams, max(1, len(tasks)))
        token = reply.get("token") if n_conns > 1 else None
        if token:
            try:
                for _ in range(n_conns - 1):
                    c = socket.create_connection((host, port), timeout=timeout)
                    c.settimeout(None)
                    send_frame(c, {"type": "join", "token": token})
                    r = recv_frame(c)
                    if r.get("type") != "joined":
                        c.close()
                        raise TransferError(f"接收方不接受并行连接: {r.get('reason', '')}")
                    conns.append(c)
            except BaseException:
                for c in conns:
                    try:
                        c.close()
                    except OSError:
                        pass
                raise
        else:
            n_conns = 1

        for d in dirs:
            send_frame(sock, {"type": "dir", "path": d[1]})

        if n_conns == 1:
            return _send_stream(sock, tasks, total_size, progress, log)
        return _send_parallel(conns, tasks, total_size, progress, log)
    except BaseException:
        for c in conns:
            try:
                c.close()
            except OSError:
                pass
        raise
