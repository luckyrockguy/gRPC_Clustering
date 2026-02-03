import sys
import os
import socket
import threading
import time
import grpc
from concurrent import futures
from PyQt5.QtWidgets import *
from PyQt5.QtCore import pyqtSignal, QObject

# --- gRPC 상세 로그 활성화 (패킷 레벨 디버깅) ---
os.environ['GRPC_TRACE'] = 'tcp,http,api'
os.environ['GRPC_VERBOSITY'] = 'DEBUG'

import messenger_pb2
import messenger_pb2_grpc

MCAST_GRP = '224.1.1.1'
MCAST_PORT = 5007
GRPC_PORT = 50051

class Communicate(QObject):
    log_signal = pyqtSignal(str)

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
        self.peers = set()
        self.my_ip = socket.gethostbyname(socket.gethostname())
        self.initUI()
        
        threading.Thread(target=self.start_grpc_server, daemon=True).start()
        threading.Thread(target=self.udp_advertiser, daemon=True).start()
        threading.Thread(target=self.udp_listener, daemon=True).start()

    def initUI(self):
        self.setWindowTitle(f"gRPC Cluster Monitoring Node - {self.my_ip}")
        self.setGeometry(100, 100, 1000, 700) # 전체 창 크기 확대

        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        layout = QVBoxLayout(central_widget)

        # 상단 상태바
        top_group = QGroupBox("Network Status")
        top_layout = QHBoxLayout()
        self.lbl_status = QLabel(f"● LOCAL IP: {self.my_ip} | gRPC PORT: {GRPC_PORT} | UDP MCAST: {MCAST_GRP}")
        self.lbl_status.setStyleSheet("font-weight: bold; color: #2c3e50;")
        top_layout.addWidget(self.lbl_status)
        top_group.setLayout(top_layout)
        layout.addWidget(top_group)

        # 중앙: 검색 노드 리스트 폭 넓힘 (350px)
        mid_layout = QHBoxLayout()
        
        peer_group = QGroupBox("Detected Nodes (Cluster)")
        peer_vbox = QVBoxLayout()
        self.peer_list = QListWidget()
        self.peer_list.setFixedWidth(350) # 요청사항: 리스트 폭 확대
        peer_vbox.addWidget(self.peer_list)
        peer_group.setLayout(peer_vbox)
        
        msg_group = QGroupBox("Message Transmission")
        msg_vbox = QVBoxLayout()
        self.msg_input = QLineEdit()
        self.msg_input.setPlaceholderText("보낼 메시지를 입력하세요...")
        self.btn_send = QPushButton("Send gRPC Request")
        self.btn_send.setHeight = 40
        msg_vbox.addWidget(QLabel("Message Content:"))
        msg_vbox.addWidget(self.msg_input)
        msg_vbox.addWidget(self.btn_send)
        msg_vbox.addStretch()
        msg_group.setLayout(msg_vbox)
        
        mid_layout.addWidget(peer_group)
        mid_layout.addWidget(msg_group)
        layout.addLayout(mid_layout)

        # 하단: 패킷 레벨 로그 모니터링
        log_group = QGroupBox("Packet & System Real-time Logs")
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

    def udp_advertiser(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 32)
        while True:
            msg = f"HELLO_NODE:{self.my_ip}"
            try:
                sock.sendto(msg.encode(), (MCAST_GRP, MCAST_PORT))
            except Exception as e:
                self.comm.log_signal.emit(f"[UDP Error] {e}")
            time.sleep(5)

    def udp_listener(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(('', MCAST_PORT))
        mreq = socket.inet_aton(MCAST_GRP) + socket.inet_aton('0.0.0.0')
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        
        self.comm.log_signal.emit("[System] UDP Discovery Listener Active")

        while True:
            data, addr = sock.recvfrom(1024)
            msg = data.decode()
            if msg.startswith("HELLO_NODE"):
                peer_ip = msg.split(":")[1]
                if peer_ip != self.my_ip and peer_ip not in self.peers:
                    self.peers.add(peer_ip)
                    self.peer_list.addItem(peer_ip)
                    self.comm.log_signal.emit(f"[Cluster] New Peer Detected: {peer_ip}")

    def start_grpc_server(self):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        messenger_pb2_grpc.add_MessengerServicer_to_server(MessengerServicer(self.comm), server)
        server.add_insecure_port(f'[::]:{GRPC_PORT}')
        self.comm.log_signal.emit(f"[gRPC] Server listening on port {GRPC_PORT}")
        server.start()
        server.wait_for_termination()

    def send_to_selected(self):
        selected = self.peer_list.currentItem()
        if not selected:
            self.comm.log_signal.emit("[Warning] Select a node from the list first.")
            return
        target_ip = selected.text()
        content = self.msg_input.text()
        
        def _send():
            try:
                # 패킷 레벨 로그가 활성화된 채널
                channel = grpc.insecure_channel(f'{target_ip}:{GRPC_PORT}')
                stub = messenger_pb2_grpc.MessengerStub(channel)
                self.comm.log_signal.emit(f"[Packet/TX] Attempting connection to {target_ip}...")
                response = stub.SendMessage(messenger_pb2.MsgRequest(sender_id=self.my_ip, content=content))
                if response.success:
                    self.comm.log_signal.emit(f"[Packet/TX] RPC Success to {target_ip}")
            except Exception as e:
                self.comm.log_signal.emit(f"[Packet/Error] Connection failed: {str(e)}")

        threading.Thread(target=_send).start()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    ex = gRPCClusterApp()
    ex.show()
    sys.exit(app.exec_())
