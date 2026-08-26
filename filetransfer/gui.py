import queue
import sys
import threading
import time
from pathlib import Path

from PyQt6.QtCore import QSettings, QTimer, Qt
from PyQt6.QtGui import QCloseEvent
from PyQt6.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QFileDialog,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMainWindow,
    QMessageBox,
    QPlainTextEdit,
    QProgressBar,
    QPushButton,
    QSpinBox,
    QTabWidget,
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
    QWidget,
)

from . import DEFAULT_PORT, format_size, format_size_pair
from .discovery import DiscoveryServer, get_local_ip, scan
from .receiver import FileTransferServer, TransferSession
from .sender import TransferError, send_transfer

SCAN_INTERVAL = 3.0
SETTINGS_FILE = Path(__file__).resolve().parent.parent / "settings.ini"


class App(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("局域网文件传输")
        self.resize(780, 620)
        self.setMinimumSize(700, 520)

        self.queue: queue.Queue = queue.Queue()
        self.server: FileTransferServer | None = None
        self.discovery: DiscoveryServer | None = None
        self.transfers: dict[int, str] = {}
        self.pending_items: dict[int, QTreeWidgetItem] = {}
        self.active_items: dict[int, QTreeWidgetItem] = {}
        self._scan_stop = threading.Event()
        self._hosts_lock = threading.Lock()
        self.settings = QSettings(str(SETTINGS_FILE), QSettings.Format.IniFormat)
        self._recent_hosts: list[str] = self.settings.value("recentHosts", []) or []
        self._scan_hosts: list[str] = list(self._recent_hosts)

        self._build_ui()

        self._queue_timer = QTimer(self)
        self._queue_timer.timeout.connect(self._poll_queue)
        self._queue_timer.start(100)

        self._scan_thread = threading.Thread(target=self._scan_loop, daemon=True)
        self._scan_thread.start()

    def _build_ui(self) -> None:
        central = QWidget(self)
        self.setCentralWidget(central)
        layout = QVBoxLayout(central)

        self.notebook = QTabWidget()
        layout.addWidget(self.notebook)
        self.tab_send = QWidget()
        self.tab_recv = QWidget()
        self.notebook.addTab(self.tab_send, "发送")
        self.notebook.addTab(self.tab_recv, "接收")

        self._build_send_tab()
        self._build_recv_tab()

        log_frame = QGroupBox("日志")
        log_layout = QVBoxLayout(log_frame)
        self.log_text = QPlainTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setMaximumBlockCount(500)
        self.log_text.setFixedHeight(110)
        log_layout.addWidget(self.log_text)
        layout.addWidget(log_frame)

        self.status_label = QLabel()
        self.statusBar().addWidget(self.status_label)
        self._refresh_status()

    def _build_send_tab(self) -> None:
        layout = QVBoxLayout(self.tab_send)

        pick_frame = QGroupBox("要发送的文件/文件夹")
        pick_layout = QHBoxLayout(pick_frame)
        self.paths_list = QListWidget()
        self.paths_list.setSelectionMode(QListWidget.SelectionMode.ExtendedSelection)
        pick_layout.addWidget(self.paths_list, 1)
        btn_col = QVBoxLayout()
        add_files = QPushButton("添加文件")
        add_files.clicked.connect(self._pick_files)
        add_dir = QPushButton("添加文件夹")
        add_dir.clicked.connect(self._pick_dir)
        remove = QPushButton("移除选中")
        remove.clicked.connect(self._remove_paths)
        clear = QPushButton("清空")
        clear.clicked.connect(self.paths_list.clear)
        for btn in (add_files, add_dir, remove, clear):
            btn_col.addWidget(btn)
        btn_col.addStretch()
        pick_layout.addLayout(btn_col)
        layout.addWidget(pick_frame, 1)

        target_frame = QGroupBox("目标主机")
        target_layout = QHBoxLayout(target_frame)
        target_layout.addWidget(QLabel("主机:"))
        self.host_combo = QComboBox()
        self.host_combo.setEditable(True)
        self.host_combo.addItems(self._recent_hosts)
        self.host_combo.editTextChanged.connect(self._on_host_edited)
        target_layout.addWidget(self.host_combo, 1)
        refresh = QPushButton("刷新设备列表")
        refresh.setMinimumWidth(96)
        refresh.clicked.connect(self._scan_once)
        target_layout.addWidget(refresh)
        layout.addWidget(target_frame)

        send_frame = QHBoxLayout()
        self.send_button = QPushButton("发送")
        self.send_button.setMinimumWidth(96)
        self.send_button.clicked.connect(self._do_send)
        send_frame.addWidget(self.send_button)
        self.send_progress = QProgressBar()
        self.send_progress.setRange(0, 100)
        self.send_progress.setValue(0)
        send_frame.addWidget(self.send_progress, 1)
        self.send_label = QLabel("")
        self.send_label.setMinimumWidth(320)
        send_frame.addWidget(self.send_label)
        layout.addLayout(send_frame)

    def _build_recv_tab(self) -> None:
        layout = QVBoxLayout(self.tab_recv)

        ctrl = QGroupBox("接收服务")
        ctrl_layout = QGridLayout(ctrl)
        ctrl_layout.addWidget(QLabel("端口:"), 0, 0)
        self.port_spin = QSpinBox()
        self.port_spin.setRange(1024, 65535)
        self.port_spin.setValue(DEFAULT_PORT)
        self.port_spin.valueChanged.connect(lambda _: self._refresh_status())
        ctrl_layout.addWidget(self.port_spin, 0, 1)
        ctrl_layout.addWidget(QLabel("保存到:"), 0, 2)
        self.out_edit = QLineEdit(str(Path.cwd()))
        ctrl_layout.addWidget(self.out_edit, 0, 3)
        browse = QPushButton("浏览...")
        browse.setMinimumWidth(88)
        browse.clicked.connect(self._pick_out_dir)
        ctrl_layout.addWidget(browse, 0, 4)
        self.auto_accept_check = QCheckBox("自动接受所有传输")
        ctrl_layout.addWidget(self.auto_accept_check, 0, 5)
        self.recv_button = QPushButton("启动接收")
        self.recv_button.setMinimumWidth(96)
        self.recv_button.clicked.connect(self._toggle_server)
        ctrl_layout.addWidget(self.recv_button, 0, 6)
        ctrl_layout.setColumnStretch(3, 1)
        layout.addWidget(ctrl)

        pending = QGroupBox("待处理的传输请求")
        pending_layout = QHBoxLayout(pending)
        self.pending_tree = QTreeWidget()
        self.pending_tree.setColumnCount(3)
        self.pending_tree.setHeaderLabels(["发送方", "IP 地址", "请求ID"])
        self.pending_tree.setColumnWidth(0, 200)
        self.pending_tree.setColumnWidth(1, 160)
        self.pending_tree.setColumnWidth(2, 60)
        self.pending_tree.setFixedHeight(120)
        pending_layout.addWidget(self.pending_tree, 1)
        pbtns = QVBoxLayout()
        accept_btn = QPushButton("接受")
        accept_btn.setMinimumWidth(88)
        accept_btn.clicked.connect(lambda: self._respond_pending(True))
        decline_btn = QPushButton("拒绝")
        decline_btn.setMinimumWidth(88)
        decline_btn.clicked.connect(lambda: self._respond_pending(False))
        pbtns.addWidget(accept_btn)
        pbtns.addWidget(decline_btn)
        pbtns.addStretch()
        pending_layout.addLayout(pbtns)
        layout.addWidget(pending)

        active = QGroupBox("进行中的传输")
        active_layout = QVBoxLayout(active)
        self.active_tree = QTreeWidget()
        self.active_tree.setColumnCount(4)
        self.active_tree.setHeaderLabels(["发送方", "当前文件", "大小", "进度", "状态"])
        self.active_tree.setColumnWidth(0, 150)
        self.active_tree.setColumnWidth(1, 260)
        self.active_tree.setColumnWidth(2, 140)
        self.active_tree.setColumnWidth(3, 90)
        active_layout.addWidget(self.active_tree)
        layout.addWidget(active, 1)

    def _log(self, msg: str) -> None:
        self.queue.put(("log", msg))

    def _write_log(self, msg: str) -> None:
        self.log_text.appendPlainText(f"[{time.strftime('%H:%M:%S')}] {msg}")

    def _refresh_status(self) -> None:
        ip = get_local_ip()
        state = "已启动" if self.server else "未启动"
        self.status_label.setText(
            f"本机地址: {ip}  |  接收端口: {self.port_spin.value()}  |  接收服务: {state}"
        )

    def _pick_files(self) -> None:
        files, _ = QFileDialog.getOpenFileNames(self, "选择要发送的文件")
        existing = {self.paths_list.item(i).text() for i in range(self.paths_list.count())}
        for f in files:
            if f not in existing:
                self.paths_list.addItem(f)
                existing.add(f)

    def _pick_dir(self) -> None:
        d = QFileDialog.getExistingDirectory(self, "选择要发送的文件夹")
        if d:
            existing = {self.paths_list.item(i).text() for i in range(self.paths_list.count())}
            if d not in existing:
                self.paths_list.addItem(d)

    def _remove_paths(self) -> None:
        for item in self.paths_list.selectedItems():
            self.paths_list.takeItem(self.paths_list.row(item))

    def _pick_out_dir(self) -> None:
        d = QFileDialog.getExistingDirectory(self, "选择接收保存目录")
        if d:
            self.out_edit.setText(d)

    def _toggle_server(self) -> None:
        if self.server:
            self._stop_server()
        else:
            self._start_server()

    def _start_server(self) -> None:
        out_dir = Path(self.out_edit.text())
        out_dir.mkdir(parents=True, exist_ok=True)
        auto = self.auto_accept_check.isChecked()
        self.server = FileTransferServer(
            port=self.port_spin.value(),
            out_dir=out_dir,
            auto_accept=auto,
            on_request=lambda s: self.queue.put(("request", s)),
            on_progress=lambda s, r, c, d, z: self.queue.put(("rprogress", s.id, r, c, d, z)),
            on_done=lambda s: self.queue.put(("rdone", s.id)),
            on_error=lambda ip, e: self.queue.put(("error", f"{ip}: {e}")),
            log=self._log,
        )
        try:
            self.server.start()
        except OSError as exc:
            self.server = None
            QMessageBox.critical(self, "启动失败", f"无法启动接收服务:\n{exc}")
            return
        self.discovery = DiscoveryServer("GUI", self.port_spin.value())
        self.discovery.start()
        self.recv_button.setText("停止接收")
        self._log(f"接收服务已启动: {get_local_ip()}:{self.port_spin.value()}")
        self._refresh_status()

    def _stop_server(self) -> None:
        if self.discovery:
            self.discovery.close()
            self.discovery = None
        if self.server:
            self.server.close()
            self.server = None
        self.recv_button.setText("启动接收")
        self.pending_tree.clear()
        self.pending_items.clear()
        self._log("接收服务已停止")
        self._refresh_status()

    def _respond_pending(self, accept: bool) -> None:
        for item in self.pending_tree.selectedItems():
            request_id = item.data(0, Qt.ItemDataRole.UserRole)
            if request_id is None:
                continue
            if self.server:
                self.server.respond(int(request_id), accept)
            self.pending_items.pop(request_id, None)
            self.pending_tree.takeTopLevelItem(self.pending_tree.indexOfTopLevelItem(item))
            if accept:
                self._log(f"已接受请求 #{request_id}")
            else:
                self._log(f"已拒绝请求 #{request_id}")

    def _on_host_edited(self, text: str) -> None:
        text = text.strip()
        if not text:
            return
        with self._hosts_lock:
            if text in self._scan_hosts:
                self._scan_hosts.remove(text)
            self._scan_hosts.insert(0, text)

    def _remember_host(self, host: str) -> None:
        if not host:
            return
        if host in self._recent_hosts:
            self._recent_hosts.remove(host)
        self._recent_hosts.insert(0, host)
        del self._recent_hosts[20:]
        self.settings.setValue("recentHosts", self._recent_hosts)
        with self._hosts_lock:
            if host in self._scan_hosts:
                self._scan_hosts.remove(host)
            self._scan_hosts.insert(0, host)

    def _scan_targets(self) -> list[str]:
        with self._hosts_lock:
            return list(self._scan_hosts)

    def _scan_once(self) -> None:
        threading.Thread(target=self._scan_worker, daemon=True).start()

    def _scan_worker(self) -> None:
        peers = scan(timeout=1.5, extra_hosts=self._scan_targets())
        self.queue.put(("peers", peers))

    def _scan_loop(self) -> None:
        while not self._scan_stop.is_set():
            peers = scan(timeout=1.0, extra_hosts=self._scan_targets())
            if peers:
                self.queue.put(("peers", peers))
            self._scan_stop.wait(SCAN_INTERVAL)

    def _do_send(self) -> None:
        paths = [self.paths_list.item(i).text() for i in range(self.paths_list.count())]
        if not paths:
            QMessageBox.warning(self, "提示", "请先添加要发送的文件或文件夹")
            return
        host_text = self.host_combo.currentText().strip()
        if not host_text:
            QMessageBox.warning(self, "提示", "请选择或输入目标主机")
            return
        if ":" in host_text:
            host, _, port_s = host_text.rpartition(":")
            try:
                port = int(port_s)
            except ValueError:
                port = DEFAULT_PORT
        else:
            host, port = host_text, DEFAULT_PORT

        self._remember_host(host_text)

        self.send_button.setEnabled(False)
        self.send_progress.setValue(0)
        self.send_label.setText("正在计算...")
        self._log(f"开始发送 {len(paths)} 个路径到 {host}:{port}")

        def worker() -> None:
            def progress(sent, total, rel, done, size):
                self.queue.put(("sprogress", sent, total, rel))

            try:
                send_transfer(host, [Path(p) for p in paths], port=port, progress=progress, log=self._log)
                self.queue.put(("sdone",))
            except (TransferError, OSError) as exc:
                self.queue.put(("send_error", str(exc)))

        threading.Thread(target=worker, daemon=True).start()

    def _poll_queue(self) -> None:
        try:
            while True:
                msg = self.queue.get_nowait()
                kind = msg[0]
                if kind == "peers":
                    values = [f"{p.get('ip')}:{p.get('port')}" for p in msg[1]]
                    current = self.host_combo.currentText()
                    self.host_combo.clear()
                    self.host_combo.addItems(values)
                    if current:
                        self.host_combo.setEditText(current)
                    elif values:
                        self.host_combo.setCurrentText(values[0])
                elif kind == "request":
                    session: TransferSession = msg[1]
                    if session.id not in self.pending_items:
                        item = QTreeWidgetItem(
                            [session.sender_name, session.sender_ip, str(session.id)]
                        )
                        item.setData(0, Qt.ItemDataRole.UserRole, session.id)
                        self.pending_tree.addTopLevelItem(item)
                        self.pending_items[session.id] = item
                        self._log(f"收到来自 {session.sender_name} ({session.sender_ip}) 的传输请求")
                elif kind == "rprogress":
                    _, sid, received, current, done, size = msg
                    pct = (done / size * 100) if size else 100.0
                    item = self.active_items.get(sid)
                    if item is None:
                        self.transfers[sid] = current
                        item = QTreeWidgetItem(["", current, format_size(size), f"{pct:.0f}%", "接收中"])
                        self.active_tree.addTopLevelItem(item)
                        self.active_items[sid] = item
                    else:
                        item.setText(1, current)
                        item.setText(2, format_size(size))
                        item.setText(3, f"{pct:.0f}%")
                elif kind == "rdone":
                    sid = msg[1]
                    item = self.active_items.get(sid)
                    if item is not None:
                        item.setText(3, "100%")
                        item.setText(4, "完成")
                        QTimer.singleShot(4000, lambda i=item: self._remove_active_row(i))
                elif kind == "sprogress":
                    _, sent, total, rel = msg
                    pct = (sent / total * 100) if total else 100.0
                    self.send_progress.setValue(int(pct))
                    name = rel if len(rel) <= 40 else "..." + rel[-37:]
                    self.send_label.setText(f"{name}  {format_size_pair(sent, total)}")
                elif kind == "sdone":
                    self.send_progress.setValue(100)
                    self.send_label.setText("发送完成")
                    self.send_button.setEnabled(True)
                    self._log("发送完成")
                elif kind == "send_error":
                    self.send_button.setEnabled(True)
                    self.send_label.setText("发送失败")
                    self._log(f"发送失败: {msg[1]}")
                    QMessageBox.critical(self, "发送失败", msg[1])
                elif kind == "error":
                    self._log(f"错误: {msg[1]}")
                elif kind == "log":
                    self._write_log(msg[1])
        except queue.Empty:
            pass

    def _remove_active_row(self, item: QTreeWidgetItem) -> None:
        idx = self.active_tree.indexOfTopLevelItem(item)
        if idx >= 0:
            self.active_tree.takeTopLevelItem(idx)
        for sid, it in list(self.active_items.items()):
            if it is item:
                del self.active_items[sid]

    def closeEvent(self, event: QCloseEvent) -> None:
        self._scan_stop.set()
        self._stop_server()
        event.accept()


def main() -> None:
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    app.setApplicationName("局域网文件传输")
    app.setStyleSheet(
        """
        QPushButton {
            border: 1px solid #7a7a7a;
            background: #f2f2f2;
            border-radius: 4px;
            padding: 4px 12px;
        }
        QPushButton:hover { background: #e6e6e6; }
        QPushButton:pressed { background: #d9d9d9; }
        QLineEdit, QComboBox, QListWidget, QTreeWidget, QPlainTextEdit {
            border: 1px solid #7a7a7a;
        }
        QGroupBox {
            border: 1px solid #9a9a9a;
            margin-top: 8px;
        }
        QGroupBox::title {
            subcontrol-origin: margin;
            left: 8px;
            padding: 0 3px;
        }
        """
    )
    window = App()
    window.show()
    app.exec()


if __name__ == "__main__":
    main()
