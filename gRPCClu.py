import sys
import os
import socket
import threading
import time
import grpc
from concurrent import futures
from PyQt5.QtWidgets import *
from PyQt5.QtCore import pyqtSignal, QObject, QTimer

# --- gRPC 상세 로그 활성화 ---
os.environ['GRPC_TRACE'] = 'tcp,http,api'
os.environ['GRPC_VERBOSITY'] = 'DEBUG'

import messenger_pb2
import messenger_pb2_grpc

MCAST_GRP = '224.1.1.1'
MCAST_PORT = 5007
GRPC_PORT = 50051

class Communicate(QObject):
    log_signal = pyqtSignal(str)
    refresh_peer_signal = pyqtSignal()

class MessengerServicer(messenger_pb2_grpc.MessengerServicer):
    def __init__(self, comm):
        self.comm = comm

    def SendMessage(self, request, context):
        msg = f"[Packet/RX] Sender: {request.sender_id} | Content: {request.content}"
        self.comm.log_signal.emit(msg)
        return messenger_pb2.MsgResponse(success=True)

class gRPCClusterApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.comm = Communicate()
        self.comm.log_signal.connect(self.add_log)
        self.comm.refresh_peer_signal.connect(self.update_peer_display)
        
        # 피어 관리: { ip: last_seen_timestamp }
        self.peers = {} 
        self.my_ip = socket.gethostbyname(socket.gethostname())
        
        self.initUI()
        
        # 스레드 및 타이머 시작
        threading.Thread(target=self.start_grpc_server, daemon=True).start()
        threading.Thread(target=self.udp_advertiser, daemon=True).start()
        threading.Thread(target=self.udp_listener, daemon=True).start()
        
        # 주기적 노드 활성 체크 타이머 (1초마다 검사)
        self.health_check_timer = QTimer()
        self.health_check_timer.timeout.connect(self.check_node_liveness)
        self.health_check_timer.start(1000)

    def initUI(self):
        self.setWindowTitle(f"gRPC Cluster Manager - {self.my_ip}")
        self.setGeometry(100, 100, 1100, 750)

        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        layout = QVBoxLayout(central_widget)

        # 상단: 네트워크 상태 및 파라미터 설정 (수정됨)
        top_group = QGroupBox("Network Configuration & Health Check Settings")
        top_layout = QHBoxLayout()
        
        self.lbl_status = QLabel(f"● LOCAL IP: {self.my_ip} | PORT: {GRPC_PORT}")
        self.lbl_status.setStyleSheet("font-weight: bold; color: #2c3e50;")
        
        # 주기 설정 UI 추가
        top_layout.addWidget(self.lbl_status)
        top_layout.addStretch(1)
        top_layout.addWidget(QLabel("Node Timeout (sec):"))
        self.spin_timeout = QSpinBox()
        self.spin_timeout.setRange(5, 300)
        self.spin_timeout.setValue(15) # 기본값 15초
        top_layout.addWidget(self.spin_timeout)
        
        top_group.setLayout(top_layout)
        layout.addWidget(top_group)

        # 중앙: 검색 노드 리스트 및 메시지 전송
        mid_layout = QHBoxLayout()
        
        peer_group = QGroupBox("Active Cluster Nodes (Auto-refresh)")
        peer_vbox = QVBoxLayout()
        self.peer_list = QListWidget()
        self.peer_list.setFixedWidth(350)
        peer_vbox.addWidget(self.peer_list)
        peer_group.setLayout(peer_vbox)
        
        msg_group = QGroupBox("Message Transmission")
        msg_vbox = QVBoxLayout()
        self.msg_input = QLineEdit()
        self.msg_input.setPlaceholderText("메시지를 입력하세요...")
        self.btn_send = QPushButton("Send gRPC Request")
        self.btn_send.setMinimumHeight(40)
        
        msg_vbox.addWidget(QLabel("Message Content:"))
        msg_vbox.addWidget(self.msg_input)
        msg_vbox.addWidget(self.btn_send)
        msg_vbox.addStretch()
        msg_group.setLayout(msg_vbox)
        
        mid_layout.addWidget(peer_group)
        mid_layout.addWidget(msg_group)
        layout.addLayout(mid_layout)

        # 하단: 로그 창
        log_group = QGroupBox("Real-time System & Packet Logs")
        log_layout = QVBoxLayout()
        self.log_txt = QTextEdit()
        self.log_txt.setReadOnly(True)
        self.log_txt.setStyleSheet("background-color: #1e1e1e; color: #d4d4d4; font-family: Consolas;")
        log_layout.addWidget(self.log_txt)
        log_group.setLayout(log_layout)
        layout.addWidget(log_group)

        self.btn_send.clicked.connect(self.send_to_selected)

    def add_log(self, text):
        timestamp = time.strftime("[%H:%M:%S] ")
        self.log_txt.append(timestamp + text)

    # --- 노드 활성 상태 체크 로직 (추가됨) ---
    def check_node_liveness(self):
        current_time = time.time()
        timeout = self.spin_timeout.value()
        expired_peers = []

        for ip, last_seen in self.peers.items():
            if current_time - last_seen > timeout:
                expired_peers.append(ip)

        if expired_peers:
            for ip in expired_peers:
                del self.peers[ip]
                self.comm.log_signal.emit(f"[Cluster] Node Timed Out: {ip}")
            self.comm.refresh_peer_signal.emit()

    def update_peer_display(self):
        self.peer_list.clear()
        for ip in sorted(self.peers.keys()):
            self.peer_list.addItem(f"{ip} (Active)")

    # --- UDP: 주기적 신호 발신 (나의 생존 알림) ---
    def udp_advertiser(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 32)
        while True:
            msg = f"HELLO_NODE:{self.my_ip}"
            try:
                sock.sendto(msg.encode(), (MCAST_GRP, MCAST_PORT))
            except Exception as e:
                self.comm.log_signal.emit(f"[UDP Error] {e}")
            time.sleep(5) # 5초마다 자신의 생존을 알림

    # --- UDP: 상대방 노드 신호 수신 ---
    def udp_listener(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(('', MCAST_PORT))
        mreq = socket.inet_aton(MCAST_GRP) + socket.inet_aton('0.0.0.0')
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        
        while True:
            data, addr = sock.recvfrom(1024)
            msg = data.decode()
            if msg.startswith("HELLO_NODE"):
                peer_ip = msg.split(":")[1]
                if peer_ip != self.my_ip:
                    is_new = peer_ip not in self.peers
                    self.peers[peer_ip] = time.time() # 수신 시간 갱신 (Heartbeat)
                    if is_new:
                        self.comm.log_signal.emit(f"[Cluster] New Peer Detected: {peer_ip}")
                    self.comm.refresh_peer_signal.emit()

    def start_grpc_server(self):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        messenger_pb2_grpc.add_MessengerServicer_to_server(MessengerServicer(self.comm), server)
        server.add_insecure_port(f'[::]:{GRPC_PORT}')
        self.comm.log_signal.emit(f"[gRPC] Server listening on {GRPC_PORT}")
        server.start()
        server.wait_for_termination()

    def send_to_selected(self):
        selected = self.peer_list.currentItem()
        if not selected:
            return
        # "(Active)" 문자열 제거 후 IP만 추출
        target_ip = selected.text().split(" ")[0]
        content = self.msg_input.text()
        
        def _send():
            try:
                channel = grpc.insecure_channel(f'{target_ip}:{GRPC_PORT}')
                stub = messenger_pb2_grpc.MessengerStub(channel)
                response = stub.SendMessage(messenger_pb2.MsgRequest(sender_id=self.my_ip, content=content))
                if response.success:
                    self.comm.log_signal.emit(f"[TX] Message sent to {target_ip}")
            except Exception as e:
                self.comm.log_signal.emit(f"[TX/Error] Failed to connect {target_ip}: {e}")

        threading.Thread(target=_send).start()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    ex = gRPCClusterApp()
    ex.show()
    sys.exit(app.exec_())

