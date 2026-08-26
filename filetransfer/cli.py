import argparse
import sys
import time
from pathlib import Path

from . import DEFAULT_PORT, __version__, format_size, format_size_pair, format_speed
from .discovery import DiscoveryServer, get_local_ip, scan
from .receiver import FileTransferServer
from .sender import TransferError, send_transfer


def _progress_line(sent: int, total: int, rel: str, done: int, size: int) -> None:
    pct = (sent / total * 100) if total else 0
    bar_len = 30
    filled = int(bar_len * (sent / total)) if total else bar_len
    bar = "#" * filled + "-" * (bar_len - filled)
    name = rel if len(rel) <= 40 else "..." + rel[-37:]
    sys.stderr.write(f"\r[{bar}] {pct:5.1f}%  {format_size_pair(sent, total)}  {name}")
    sys.stderr.flush()


def cmd_receive(args) -> int:
    def log(msg: str) -> None:
        print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    server = FileTransferServer(
        port=args.port, out_dir=out, auto_accept=True, log=log
    )
    server.start()
    discovery = DiscoveryServer("receive", args.port)
    discovery.start()
    try:
        print(f"接收中... 本机地址: {get_local_ip()}:{args.port}", flush=True)
        print(f"文件保存至: {out.resolve()}", flush=True)
        print("按 Ctrl+C 退出", flush=True)
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n已停止接收", flush=True)
    finally:
        server.close()
        discovery.close()
    return 0


def cmd_send(args) -> int:
    paths = [Path(p) for p in args.path]
    host = args.to
    started = time.monotonic()

    def progress(sent, total, rel, done, size):
        if args.quiet:
            return
        _progress_line(sent, total, rel, done, size)

    def log(msg: str) -> None:
        if not args.quiet:
            print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

    try:
        total = send_transfer(host, paths, port=args.port, progress=progress, log=log)
    except (TransferError, OSError) as exc:
        print(f"\n发送失败: {exc}", file=sys.stderr)
        return 1
    elapsed = time.monotonic() - started
    if not args.quiet:
        speed = (total / elapsed) if elapsed else 0
        print(f"\n发送完成: {format_size(total)}, 用时 {elapsed:.1f}s, 平均 {format_speed(speed)}")
    return 0


def cmd_list(args) -> int:
    print(f"扫描局域网 ({args.timeout}s)...", flush=True)
    peers = scan(timeout=args.timeout, extra_hosts=args.host)
    if not peers:
        print("未发现任何在线设备。请确认对方已启动接收或 GUI。")
        return 0
    print(f"{'名称':<20} {'IP地址':<16} {'端口':<8} {'版本'}")
    print("-" * 56)
    for p in peers:
        print(f"{p.get('name', '?'):<20} {p.get('ip', p.get('source', '?')):<16} {p.get('port', '?'):<8} {p.get('app', '?')}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ft",
        description="局域网点对点文件/文件夹传输工具",
    )
    parser.add_argument("--version", action="version", version=f"ft {__version__}")
    sub = parser.add_subparsers(dest="command")

    p_send = sub.add_parser("send", help="发送文件或文件夹")
    p_send.add_argument("path", nargs="+", help="要发送的文件或文件夹路径（可多个）")
    p_send.add_argument("--to", required=True, help="目标主机 IP 或主机名")
    p_send.add_argument("--port", type=int, default=DEFAULT_PORT, help=f"目标端口（默认 {DEFAULT_PORT}）")
    p_send.add_argument("-q", "--quiet", action="store_true", help="不显示进度和日志")
    p_send.set_defaults(func=cmd_send)

    p_recv = sub.add_parser("receive", help="启动接收服务")
    p_recv.add_argument("--port", type=int, default=DEFAULT_PORT, help=f"监听端口（默认 {DEFAULT_PORT}）")
    p_recv.add_argument("--out", default=".", help="保存目录（默认当前目录）")
    p_recv.set_defaults(func=cmd_receive)

    p_list = sub.add_parser("list", help="扫描局域网内的在线设备")
    p_list.add_argument("--timeout", type=float, default=2.0, help="扫描时长秒（默认 2.0）")
    p_list.add_argument(
        "--host",
        nargs="*",
        default=[],
        help="额外单播探测的主机，可跨路由器（如 10.0.0.2 或 10.0.0.2:54546）",
    )
    p_list.set_defaults(func=cmd_list)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.command:
        parser.print_help()
        return 1
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
