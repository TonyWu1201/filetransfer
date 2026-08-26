import json
import socket
import threading
from pathlib import Path

import pytest

from filetransfer import discovery
from filetransfer.protocol import recv_frame, send_frame
from filetransfer.receiver import FileTransferServer
from filetransfer.sender import send_transfer


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@pytest.fixture
def server(tmp_path):
    srv = FileTransferServer(
        port=free_port(), out_dir=tmp_path / "out", auto_accept=True
    )
    srv.start()
    yield srv
    srv.close()


def make_tree(root: Path) -> None:
    (root / "sub" / "nested").mkdir(parents=True)
    (root / "a.txt").write_text("hello")
    (root / "sub" / "b.txt").write_text("world" * 100)
    (root / "sub" / "nested" / "c.bin").write_bytes(bytes(range(256)) * 4096)


def test_frame_roundtrip():
    a, b = socket.socketpair()
    send_frame(a, {"type": "hello", "name": "测试"})
    assert recv_frame(b) == {"type": "hello", "name": "测试"}
    a.close()
    b.close()


def test_send_single_file(server, tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    f = src / "a.txt"
    f.write_text("hello world" * 1000)
    total = send_transfer("127.0.0.1", [f], port=server.port)
    out = server.out_dir / "a.txt"
    assert total == f.stat().st_size
    assert out.read_text() == f.read_text()


def test_send_folder_tree(server, tmp_path):
    src = tmp_path / "src"
    make_tree(src)
    send_transfer("127.0.0.1", [src], port=server.port)
    out = server.out_dir / "src"
    assert (out / "a.txt").read_text() == "hello"
    assert (out / "sub" / "b.txt").read_text() == "world" * 100
    assert (out / "sub" / "nested" / "c.bin").read_bytes() == bytes(range(256)) * 4096


def test_multiple_paths_and_name_clash(server, tmp_path):
    a = tmp_path / "a.txt"
    a.write_text("first")
    b = tmp_path / "b.txt"
    b.write_text("second")
    send_transfer("127.0.0.1", [a, b], port=server.port)
    send_transfer("127.0.0.1", [a], port=server.port)
    assert (server.out_dir / "a.txt").read_text() == "first"
    assert (server.out_dir / "a (1).txt").read_text() == "first"
    assert (server.out_dir / "b.txt").read_text() == "second"


def test_large_file_progress(server, tmp_path):
    f = tmp_path / "big.bin"
    f.write_bytes(bytes(1024) * (2 * 1024 * 1024))
    events = []

    def progress(sent, total, rel, done, size):
        events.append(sent)

    send_transfer("127.0.0.1", [f], port=server.port, progress=progress)
    assert events[-1] == f.stat().st_size
    assert (server.out_dir / "big.bin").read_bytes() == f.read_bytes()


def test_decline(tmp_path):
    srv = FileTransferServer(port=free_port(), out_dir=tmp_path / "out")
    srv.start()
    f = tmp_path / "x.txt"
    f.write_text("data")
    declined = threading.Event()

    def on_request(session):
        srv.respond(session.id, False)
        declined.set()

    srv.on_request = on_request
    with pytest.raises(Exception):
        send_transfer("127.0.0.1", [f], port=srv.port, timeout=5)
    assert declined.wait(5)
    assert not (srv.out_dir / "x.txt").exists()
    srv.close()


def test_path_traversal_blocked(server):
    with socket.create_connection(("127.0.0.1", server.port)) as sock:
        send_frame(sock, {"type": "hello", "name": "evil", "version": 1})
        assert recv_frame(sock)["type"] == "accept"
        send_frame(sock, {"type": "file", "path": "../escape.txt", "size": 4})
        sock.sendall(b"evil")
        send_frame(sock, {"type": "done"})
    assert not (server.out_dir.parent / "escape.txt").exists()


def test_discovery_responds(monkeypatch):
    name, port = "test-host", free_port()
    srv = discovery.DiscoveryServer(name, port, ip="127.0.0.1")
    srv.start()
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(2)
    sock.sendto(discovery.PROBE, ("127.0.0.1", discovery.DISCOVERY_PORT))
    data, _ = sock.recvfrom(8192)
    sock.close()
    srv.close()
    assert data.startswith(discovery.RESPONSE)
    info = json.loads(data[len(discovery.RESPONSE):])
    assert info["name"] == name
    assert info["port"] == port


def test_broadcast_of():
    assert discovery._broadcast_of("10.0.0.1", "255.255.255.0") == "10.0.0.255"
    assert discovery._broadcast_of("10.203.236.206", "255.255.254.0") == "10.203.237.255"
    assert discovery._broadcast_of("192.168.1.7", "255.255.0.0") == "192.168.255.255"


def test_broadcast_targets_include_limited():
    targets = discovery._broadcast_targets()
    assert ("255.255.255.255", discovery.DISCOVERY_PORT) in targets


def test_unicast_targets_use_discovery_port():
    targets = discovery._unicast_targets(["10.0.0.2:54546", "10.0.0.3", ""])
    assert ("10.0.0.2", discovery.DISCOVERY_PORT) in targets
    assert ("10.0.0.3", discovery.DISCOVERY_PORT) in targets


def test_unicast_scan_finds_server():
    name, port = "uni-host", free_port()
    srv = discovery.DiscoveryServer(name, port, ip="127.0.0.1")
    srv.start()
    peers = discovery.scan(timeout=1.0, extra_hosts=["127.0.0.1"])
    srv.close()
    assert any(p["name"] == name and p["port"] == port for p in peers)
