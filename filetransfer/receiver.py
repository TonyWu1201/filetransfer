import socket
import threading
from pathlib import Path

from . import CHUNK_SIZE, DEFAULT_PORT, format_size
from .protocol import PROTOCOL_VERSION, ProtocolError, recv_exact, recv_frame, send_frame


def _safe_join(out_dir: Path, rel: str) -> Path:
    target = (out_dir / rel).resolve()
    if not target.is_relative_to(out_dir.resolve()):
        raise ProtocolError(f"非法路径: {rel}")
    return target


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
        self.accept_event = threading.Event()
        self.accepted = False


class FileTransferServer:
    def __init__(
        self,
        port: int = DEFAULT_PORT,
        out_dir: Path | str = ".",
        auto_accept: bool = False,
        on_request=None,
        on_progress=None,
        on_done=None,
        on_error=None,
        log=None,
    ):
        self.port = port
        self.out_dir = Path(out_dir).resolve()
        self.auto_accept = auto_accept
        self.on_request = on_request
        self.on_progress = on_progress
        self.on_done = on_done
        self.on_error = on_error
        self.log = log
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._lock = threading.Lock()
        self._next_id = 1
        self._pending: dict[int, TransferSession] = {}
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
        request_id = None
        with conn:
            try:
                hello = recv_frame(conn)
                if hello.get("type") != "hello":
                    raise ProtocolError("握手消息无效")
                sender_name = hello.get("name", "未知")
                with self._lock:
                    request_id = self._next_id
                    self._next_id += 1
                    session = TransferSession(request_id, sender_name, addr[0])
                    self._pending[request_id] = session
                if self.auto_accept:
                    self.respond(request_id, True)
                elif self.on_request:
                    self.on_request(session)
                else:
                    self.respond(request_id, True)
                session.accept_event.wait()
                if not session.accepted:
                    send_frame(conn, {"type": "decline", "reason": "用户拒绝"})
                    return
                send_frame(conn, {"type": "accept", "version": PROTOCOL_VERSION})
                if self.log:
                    self.log(f"来自 {sender_name} ({addr[0]}) 的传输开始")
                self._receive_loop(conn, session)
            except (ProtocolError, OSError) as exc:
                if self.on_error:
                    self.on_error(addr[0], str(exc))
                elif self.log:
                    self.log(f"连接 {addr[0]} 出错: {exc}")
            finally:
                if request_id is not None:
                    self._pending.pop(request_id, None)

    def _receive_loop(self, conn: socket.socket, session: TransferSession) -> None:
        out_dir = self.out_dir
        out_dir.mkdir(parents=True, exist_ok=True)
        received = 0
        current = None
        while True:
            frame = recv_frame(conn)
            ftype = frame.get("type")
            if ftype == "dir":
                dedup_dir = _dedup_path(_safe_join(out_dir, frame["path"]))
                dedup_dir.mkdir(parents=True, exist_ok=True)
            elif ftype == "file":
                size = frame["size"]
                target = _dedup_path(_safe_join(out_dir, frame["path"]))
                target.parent.mkdir(parents=True, exist_ok=True)
                current = frame["path"]
                with open(target, "wb") as fh:
                    remaining = size
                    while remaining > 0:
                        chunk = recv_exact(conn, min(CHUNK_SIZE, remaining))
                        fh.write(chunk)
                        received += len(chunk)
                        remaining -= len(chunk)
                        if self.on_progress:
                            self.on_progress(session, received, current, size - remaining, size)
                if self.log:
                    self.log(f"已保存: {target.relative_to(out_dir)} ({format_size(size)})")
            elif ftype == "done":
                send_frame(conn, {"type": "ack"})
                if self.on_done:
                    self.on_done(session)
                if self.log:
                    self.log(f"传输完成, 共接收 {format_size(received)}")
                return
            else:
                raise ProtocolError(f"未知消息类型: {ftype}")
