import socket
import threading
import uuid
from pathlib import Path

from . import CHUNK_SIZE, DEFAULT_PORT, default_out_dir, format_size
from .protocol import PROTOCOL_VERSION, ProtocolError, recv_exact, recv_frame, send_frame


def _safe_join(out_dir: Path, rel: str) -> Path:
    rel = rel.replace("\\", "/")
    p = Path(rel)
    if p.is_absolute() or p.drive or ".." in p.parts:
        raise ProtocolError(f"非法路径: {rel}")
    return out_dir / p


def _dedup_path(path: Path) -> Path:
    if not path.exists():
        return path
    stem, suffix = path.stem, path.suffix
    for i in range(1, 10000):
        candidate = path.with_name(f"{stem} ({i}){suffix}")
        if not candidate.exists():
            return candidate
    raise ProtocolError(f"无法生成不重名的目标路径: {path}")


class TransferSession:
    def __init__(self, request_id: int, sender_name: str, sender_ip: str):
        self.id = request_id
        self.sender_name = sender_name
        self.sender_ip = sender_ip
        self.token = uuid.uuid4().hex
        self.accept_event = threading.Event()
        self.accepted = False
        self.lock = threading.Lock()
        self.files: dict[str, dict] = {}
        self.received = 0
        self.conn_count = 1
        self.alive = 1
        self.finished = 0


class FileTransferServer:
    def __init__(
        self,
        port: int = DEFAULT_PORT,
        out_dir: Path | str | None = None,
        auto_accept: bool = False,
        on_request=None,
        on_progress=None,
        on_done=None,
        on_error=None,
        log=None,
        max_streams: int = 4,
    ):
        self.port = port
        self.out_dir = Path(out_dir if out_dir is not None else default_out_dir()).resolve()
        self.auto_accept = auto_accept
        self.on_request = on_request
        self.on_progress = on_progress
        self.on_done = on_done
        self.on_error = on_error
        self.log = log
        self.max_streams = max(1, max_streams)
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._lock = threading.Lock()
        self._next_id = 1
        self._pending: dict[int, TransferSession] = {}
        self._sessions: dict[str, TransferSession] = {}
        self._threads: list[threading.Thread] = []
        self._running = False

    def start(self) -> None:
        self._sock.bind(("0.0.0.0", self.port))
        self._sock.listen(16)
        self._sock.settimeout(1.0)
        self._running = True
        t = threading.Thread(target=self._accept_loop, daemon=True)
        t.start()
        self._threads.append(t)
        if self.log:
            self.log(f"接收服务已启动: 端口 {self.port}, 保存目录 {self.out_dir}")

    def close(self) -> None:
        self._running = False
        try:
            self._sock.close()
        except OSError:
            pass

    def respond(self, request_id: int, accept: bool) -> None:
        session = self._pending.pop(request_id, None)
        if session is None:
            return
        session.accepted = accept
        session.accept_event.set()

    @property
    def pending_count(self) -> int:
        return len(self._pending)

    def _accept_loop(self) -> None:
        while self._running:
            try:
                conn, addr = self._sock.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            t = threading.Thread(
                target=self._handle, args=(conn, addr), daemon=True
            )
            t.start()
            self._threads.append(t)

    def _handle(self, conn: socket.socket, addr) -> None:
        conn.settimeout(None)
        session = None
        with conn:
            try:
                hello = recv_frame(conn)
                ftype = hello.get("type")
                if ftype == "join":
                    session = self._join_conn(conn, addr, hello)
                elif ftype == "hello":
                    session = self._hello_conn(conn, addr, hello)
                else:
                    raise ProtocolError("握手消息无效")
                if session is not None:
                    self._receive_loop(conn, session)
            except (ProtocolError, OSError) as exc:
                if self.on_error:
                    self.on_error(addr[0], str(exc))
                elif self.log:
                    self.log(f"连接 {addr[0]} 出错: {exc}")
            finally:
                if session is not None:
                    with session.lock:
                        session.alive -= 1
                        last = session.alive <= 0
                    if last:
                        self._sessions.pop(session.token, None)
                    self._pending.pop(session.id, None)

    def _hello_conn(self, conn: socket.socket, addr, hello: dict):
        sender_name = hello.get("name", "未知")
        with self._lock:
            request_id = self._next_id
            self._next_id += 1
            session = TransferSession(request_id, sender_name, addr[0])
            self._pending[request_id] = session
            self._sessions[session.token] = session
        if self.auto_accept:
            self.respond(request_id, True)
        elif self.on_request:
            self.on_request(session)
        else:
            self.respond(request_id, True)
        session.accept_event.wait()
        if not session.accepted:
            self._sessions.pop(session.token, None)
            send_frame(conn, {"type": "decline", "reason": "用户拒绝"})
            return None
        requested = int(hello.get("streams") or 1)
        streams = min(max(1, requested), self.max_streams)
        reply = {"type": "accept", "version": PROTOCOL_VERSION}
        if streams > 1:
            reply["streams"] = streams
            reply["token"] = session.token
        send_frame(conn, reply)
        if self.log:
            self.log(f"来自 {sender_name} ({addr[0]}) 的传输开始")
        return session

    def _join_conn(self, conn: socket.socket, addr, hello: dict):
        token = hello.get("token")
        session = self._sessions.get(token) if token else None
        if session is None or session.sender_ip != addr[0]:
            send_frame(conn, {"type": "decline", "reason": "无效的并行连接"})
            return None
        session.accept_event.wait()
        with session.lock:
            if not session.accepted:
                send_frame(conn, {"type": "decline", "reason": "用户拒绝"})
                return None
            session.conn_count += 1
            session.alive += 1
        send_frame(conn, {"type": "joined"})
        return session

    def _prepare_target(self, session: TransferSession, rel: str, total_size: int) -> Path:
        with session.lock:
            entry = session.files.get(rel)
            if entry is None:
                target = _dedup_path(_safe_join(self.out_dir, rel))
                target.parent.mkdir(parents=True, exist_ok=True)
                entry = {"target": target, "received": 0, "total": total_size, "logged": False}
                session.files[rel] = entry
                with open(target, "wb"):
                    pass
        return entry["target"]

    def _receive_loop(self, conn: socket.socket, session: TransferSession) -> None:
        out_dir = self.out_dir
        out_dir.mkdir(parents=True, exist_ok=True)
        while True:
            frame = recv_frame(conn)
            ftype = frame.get("type")
            if ftype == "dir":
                dedup_dir = _dedup_path(_safe_join(out_dir, frame["path"]))
                dedup_dir.mkdir(parents=True, exist_ok=True)
            elif ftype == "file":
                rel = frame["path"]
                size = frame["size"]
                offset = frame.get("offset", 0)
                total_size = frame.get("total_size", size)
                target = self._prepare_target(session, rel, total_size)
                with open(target, "r+b") as fh:
                    fh.seek(offset)
                    remaining = size
                    while remaining > 0:
                        chunk = recv_exact(conn, min(CHUNK_SIZE, remaining))
                        fh.write(chunk)
                        with session.lock:
                            session.received += len(chunk)
                            received = session.received
                            entry = session.files.get(rel)
                            if entry is not None:
                                entry["received"] += len(chunk)
                                file_done = entry["received"]
                            else:
                                file_done = offset + size - remaining
                            if self.on_progress:
                                self.on_progress(
                                    session, received, rel, file_done, total_size
                                )
                        remaining -= len(chunk)
                if self.log:
                    with session.lock:
                        entry = session.files.get(rel)
                        if (
                            entry is not None
                            and not entry["logged"]
                            and entry["received"] >= entry["total"]
                        ):
                            entry["logged"] = True
                            complete = True
                        else:
                            complete = False
                    if complete:
                        self.log(f"已保存: {target.relative_to(out_dir)} ({format_size(total_size)})")
            elif ftype == "done":
                send_frame(conn, {"type": "ack"})
                with session.lock:
                    session.finished += 1
                    complete = session.finished >= session.conn_count
                    received = session.received
                if complete:
                    if self.on_done:
                        self.on_done(session)
                    if self.log:
                        self.log(f"传输完成, 共接收 {format_size(received)}")
                return
            else:
                raise ProtocolError(f"未知消息类型: {ftype}")
