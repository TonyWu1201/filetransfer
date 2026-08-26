import json
import socket
import struct

PROTOCOL_VERSION = 2
MAX_FRAME = 64 * 1024

PREFIX = struct.Struct("!I")


class ProtocolError(Exception):
    pass


def send_frame(sock: socket.socket, obj: dict) -> None:
    data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
    if len(data) > MAX_FRAME:
        raise ProtocolError("控制帧过大")
    sock.sendall(PREFIX.pack(len(data)) + data)


def recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(min(n - len(buf), 65536))
        if not chunk:
            raise ProtocolError("连接意外断开")
        buf.extend(chunk)
    return bytes(buf)


def recv_frame(sock: socket.socket) -> dict:
    head = recv_exact(sock, PREFIX.size)
    (length,) = PREFIX.unpack(head)
    if length > MAX_FRAME:
        raise ProtocolError("控制帧过大")
    return json.loads(recv_exact(sock, length).decode("utf-8"))
