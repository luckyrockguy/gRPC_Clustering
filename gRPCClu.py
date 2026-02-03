import sys
import socket
import threading
import time
import grpc
from concurrent import futures
from PyQt5.QtWidgets import *
from PyQt5.QtCore import pyqtSignal, QObject

# gRPC 생성 파일 임포트 (미리 컴파일 필요: python -m grpc_tools.protoc ...)
import messenger_pb2
import messenger_pb2_grpc

# --- 설정 ---
MCAST_GRP = '224.1.1.1'
MCAST_PORT = 5007
GRPC_PORT = 50051

# --- GUI 로그 전달용 시그널 ---
class Communicate(QObject):
    log_signal = pyqtSignal(str)
    peer_signal = pyqtSignal(list)

# --- gRPC 서비스 구현 ---
class MessengerServicer(messenger_pb2_grpc.MessengerServicer):
    def __init__(self, comm):
        self.comm = comm

    def SendMessage(self, request, context):
        msg = f"[수신] {request.sender_id}: {request.content}"
        self.comm.log_signal.emit(msg)
        return messenger_pb2.MsgResponse(success=True)

# --- 메인 윈도우 클래스 ---
class gRPCClusterApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.comm = Communicate()
        self.comm.log_signal.connect(self.add_log)
        self.peers = set()
        self.my_ip = socket.gethostbyname(socket.gethostname())
        self.initUI()
        
        # 스레드 시작
        threading.Thread(target=self.start_grpc_server, daemon=True).start()
        threading.Thread(target=self.udp_advertiser, daemon=True).start()
        threading.Thread(target=self.udp_listener, daemon=True).start()

    def initUI(self):
        self.setWindowTitle(f"gRPC Cluster Node - {self.my_ip}")
        self.setGeometry(100, 100, 800, 600)

        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        layout = QVBoxLayout(central_widget)

        # 상단: 노드 정보 및 상태 (HSMSMaster 스타일)
        top_group = QGroupBox("Node Status")
        top_layout = QHBoxLayout()
        self.lbl_status = QLabel(f"IP: {self.my_ip} | Status: Running")
        top_layout.addWidget(self.lbl_status)
        top_group.setLayout(top_layout)
        layout.addWidget(top_group)

        # 중앙: 메시지 전송창
        mid_layout = QHBoxLayout()
        self.peer_list = QListWidget()
        self.peer_list.setMaximumWidth(200)
        self.msg_input = QLineEdit()
        self.btn_send = QPushButton("Send Message")
        self.btn_send.clicked.connect(self.send_to_selected)
        
        mid_layout.addWidget(self.peer_list)
        v_box = QVBoxLayout()
        v_box.addWidget(QLabel("Message Content:"))
        v_box.addWidget(self.msg_input)
        v_box.addWidget(self.btn_send)
        mid_layout.addLayout(v_box)
        layout.addLayout(mid_layout)

        # 하단: 로그 모니터링 창 (디버깅용)
        log_group = QGroupBox("System Logs")
        log_layout = QVBoxLayout()
        self.log_txt = QTextEdit()
        self.log_txt.setReadOnly(True)
        log_layout.addWidget(self.log_txt)
        log_group.setLayout(log_layout)
        layout.addWidget(log_group)

    def add_log(self, text):
        timestamp = time.strftime("[%H:%M:%S] ")
        self.log_txt.append(timestamp + text)

    # --- UDP Multicast: 나를 알림 ---
    def udp_advertiser(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
        while True:
            msg = f"HELLO_NODE:{self.my_ip}"
            sock.sendto(msg.encode(), (MCAST_GRP, MCAST_PORT))
            time.sleep(5)

    # --- UDP Multicast: 다른 노드 탐색 ---
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
                if peer_ip != self.my_ip and peer_ip not in self.peers:
                    self.peers.add(peer_ip)
                    self.peer_list.addItem(peer_ip)
                    self.comm.log_signal.emit(f"[발견] 새 노드 추가됨: {peer_ip}")

    # --- gRPC 서버 시작 ---
    def start_grpc_server(self):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        messenger_pb2_grpc.add_MessengerServicer_to_server(MessengerServicer(self.comm), server)
        server.add_insecure_port(f'[::]:{GRPC_PORT}')
        self.comm.log_signal.emit(f"[서버] gRPC 서버 시작됨 (Port: {GRPC_PORT})")
        server.start()
        server.wait_for_termination()

    # --- 클라이언트: 메시지 전송 로직 ---
    def send_to_selected(self):
        selected = self.peer_list.currentItem()
        if not selected:
            return
        target_ip = selected.text()
        content = self.msg_input.text()
        
        def _send():
            try:
                channel = grpc.insecure_channel(f'{target_ip}:{GRPC_PORT}')
                stub = messenger_pb2_grpc.MessengerStub(channel)
                response = stub.SendMessage(messenger_pb2.MsgRequest(sender_id=self.my_ip, content=content))
                if response.success:
                    self.comm.log_signal.emit(f"[전송성공] {target_ip}로 메시지 보냄")
            except Exception as e:
                self.comm.log_signal.emit(f"[오류] 전송 실패 ({target_ip}): {e}")

        threading.Thread(target=_send).start()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    ex = gRPCClusterApp()
    ex.show()
    sys.exit(app.exec_())


