import json
import socket
import sys
import threading
import time

from . import DISCOVERY_PORT, __version__
from .protocol import PROTOCOL_VERSION

PROBE = b"FT-PROBE-1"
RESPONSE = b"FT-RESP-1"

_SCAN_RESEND_INTERVAL = 0.4


def get_local_ip() -> str:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    except OSError:
        return "127.0.0.1"
    finally:
        s.close()


def _broadcast_of(ip: str, mask: str) -> str:
    ip_b = socket.inet_aton(ip)
    mask_b = socket.inet_aton(mask)
    return socket.inet_ntoa(bytes((a | (~b)) & 0xFF for a, b in zip(ip_b, mask_b)))


def _interface_addrs() -> list[tuple[str, str]]:
    """返回本机各网卡的 (IP, 子网掩码) 列表, 不含回环。"""
    addrs: list[tuple[str, str]] = []
    if sys.platform.startswith("linux"):
        try:
            import fcntl
            import struct

            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                for _idx, name in socket.if_nameindex():
                    ifreq = struct.pack("256s", name.encode("utf-8")[:15])
                    try:
                        ip = socket.inet_ntoa(fcntl.ioctl(sock, 0x8915, ifreq)[20:24])
                        mask = socket.inet_ntoa(fcntl.ioctl(sock, 0x891B, ifreq)[20:24])
                    except OSError:
                        continue
                    if ip.startswith("127."):
                        continue
                    addrs.append((ip, mask))
            finally:
                sock.close()
            if addrs:
                return addrs
        except (ImportError, OSError):
            pass
    try:
        for info in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET):
            ip = info[4][0]
            if ip.startswith("127."):
                continue
            addrs.append((ip, "255.255.255.0"))
    except OSError:
        pass
    return addrs


def _broadcast_targets() -> list[tuple[str, int]]:
    targets = {(bc, DISCOVERY_PORT) for bc in ("255.255.255.255",)}
    for ip, mask in _interface_addrs():
        try:
            targets.add((_broadcast_of(ip, mask), DISCOVERY_PORT))
        except OSError:
            continue
    return sorted(targets)


def _unicast_targets(hosts: list[str]) -> list[tuple[str, int]]:
    """把 "ip[:端口]" 主机列表转换为单播探测目标(探测端口固定为 DISCOVERY_PORT)。

    单播探测不依赖广播, 可以跨路由器找到不同子网上的设备。
    """
    targets: list[tuple[str, int]] = []
    for host in hosts:
        host = host.strip()
        if not host:
            continue
        addr = host.rpartition(":")[0] if ":" in host else host
        try:
            targets.append((socket.gethostbyname(addr), DISCOVERY_PORT))
        except OSError:
            continue
    return targets


class DiscoveryServer:
    def __init__(self, name: str, port: int, ip: str | None = None):
        self.name = name
        self.port = port
        self.ip = ip or get_local_ip()
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self._sock.bind(("", DISCOVERY_PORT))
        self._thread = threading.Thread(target=self._loop, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def close(self) -> None:
        try:
            self._sock.close()
        except OSError:
            pass

    def _loop(self) -> None:
        while True:
            try:
                data, addr = self._sock.recvfrom(4096)
            except OSError:
                return
            if data == PROBE:
                reply = json.dumps(
                    {
                        "name": self.name,
                        "ip": self.ip,
                        "port": self.port,
                        "version": PROTOCOL_VERSION,
                        "app": __version__,
                    },
                    ensure_ascii=False,
                ).encode("utf-8")
                try:
                    self._sock.sendto(RESPONSE + reply, addr)
                except OSError:
                    pass


def scan(timeout: float = 2.0, extra_hosts: list[str] | None = None) -> list[dict]:
    """扫描局域网在线设备。

    向每个网卡的定向广播地址和 255.255.255.255 周期性发送探测包;
    同时对 extra_hosts 中的主机做单播探测(可跨路由器, 适用于不同子网)。
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sock.settimeout(0.2)
    targets = _broadcast_targets() + _unicast_targets(extra_hosts or [])
    results: dict[str, dict] = {}
    deadline = time.monotonic() + timeout
    next_probe = 0.0
    while time.monotonic() < deadline:
        now = time.monotonic()
        if now >= next_probe:
            for target in targets:
                try:
                    sock.sendto(PROBE, target)
                except OSError:
                    continue
            next_probe = now + _SCAN_RESEND_INTERVAL
        try:
            data, addr = sock.recvfrom(8192)
        except socket.timeout:
            continue
        except OSError:
            break
        if not data.startswith(RESPONSE):
            continue
        try:
            info = json.loads(data[len(RESPONSE) :].decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            continue
        info["source"] = addr[0]
        key = (info.get("ip", addr[0]), info.get("port"))
        results[key] = info
    sock.close()
    return sorted(results.values(), key=lambda i: (i.get("ip", ""), i.get("port", 0)))
