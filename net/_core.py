import asyncio
import json
import os
from net._session import ClientSession, HectagonIPCSession

# UNIX socket path
IPC_SOCKET_PATH = "/tmp/hectagon.sock"

# Cleanup socket on startup
if os.path.exists(IPC_SOCKET_PATH):
    os.remove(IPC_SOCKET_PATH)


class HectagonServer:

    def __init__(self, host, port):
        self.host = host
        self.port = port

    async def start(self):
        server = await asyncio.start_server(
            self.handle_client,
            self.host,
            self.port
        )

        print(f"[HectagonServer] Listening on {self.host}:{self.port}")
        async with server:
            await server.serve_forever()

    async def handle_client(self, reader, writer):
        session = ClientSession(reader, writer)
        await session.start()


class HectagonIPC:
    """Local UNIX domain socket IPC server"""

    def __init__(self, client_layer):
        self.client_layer = client_layer
        self.sessions = {}
        self.session_counter = 0

    async def start(self):
        """Start UNIX socket server"""
        server = await asyncio.start_unix_server(
            self.handle_client,
            IPC_SOCKET_PATH
        )

        print(f"[HectagonIPC] Listening on {IPC_SOCKET_PATH}")
        async with server:
            await server.serve_forever()

    async def handle_client(self, reader, writer):
        """Handle new local app connection"""
        session_id = self.session_counter
        self.session_counter += 1

        session = HectagonIPCSession(reader, writer, self.client_layer)
        self.sessions[session_id] = session

        try:
            await session.start()
        finally:
            if session_id in self.sessions:
                del self.sessions[session_id]

    async def broadcast_to_ipc(self, data):
        """Broadcast packet to all IPC clients"""
        for session in list(self.sessions.values()):
            await session.send(data)


class HectagonClient:
    """Client layer: TCP connection manager + UNIX socket IPC bridge"""

    def __init__(self, server_host, server_port):
        self.server_host = server_host
        self.server_port = server_port

        # TCP connection state
        self.reader = None
        self.writer = None
        self.is_connected = False

        # IPC state
        self.ipc = HectagonIPC(self)
        self.pending_requests = {}  # Map request ID to IPCSession

        # Reconnect config
        self.reconnect_interval = 7  # seconds

    async def start(self):
        """Start client layer"""
        print("[HectagonClient] Starting...")

        # Start IPC server
        ipc_task = asyncio.create_task(self.ipc.start())

        # Start TCP connection manager
        tcp_task = asyncio.create_task(self.maintain_tcp_connection())

        # Wait for both (they run indefinitely)
        await asyncio.gather(ipc_task, tcp_task)

    async def maintain_tcp_connection(self):
        """Maintain persistent TCP connection with auto-reconnect"""
        while True:
            try:
                await self.connect_to_server()
                await self.tcp_receive_loop()
            except Exception as e:
                print(f"[HectagonClient] Error: {e}")

            # Reconnect after 7 seconds
            print(f"[HectagonClient] Reconnecting in {self.reconnect_interval}s...")
            await asyncio.sleep(self.reconnect_interval)

    async def connect_to_server(self):
        """Connect to Hectagon server"""
        print(f"[HectagonClient] Connecting to {self.server_host}:{self.server_port}...")

        self.reader, self.writer = await asyncio.open_connection(
            self.server_host,
            self.server_port
        )

        self.is_connected = True
        print("[HectagonClient] Connected to server")

        # Send register packet
        await self.send_to_server({
            "type": "register",
            "request_id": "hectagon_client"
        })

    async def tcp_receive_loop(self):
        """Receive packets from TCP server"""
        while self.is_connected:
            try:
                data = await asyncio.wait_for(
                    self.reader.readline(),
                    timeout=30
                )
            except asyncio.TimeoutError:
                print("[HectagonClient] TCP timeout")
                break
            except Exception as e:
                print(f"[HectagonClient] TCP error: {e}")
                break

            if not data:
                print("[HectagonClient] Server disconnected")
                break

            try:
                packet = json.loads(data.decode())
                await self.handle_server_packet(packet)
            except json.JSONDecodeError:
                print("[HectagonClient] Invalid JSON from server")

        self.is_connected = False
        await self.cleanup_tcp()

    async def handle_server_packet(self, packet):
        """Handle packet from server"""
        packet_type = packet.get("type")

        if packet_type == "register_response":
            print("[HectagonClient] Registered with server")
            return

        # Check if it's a response to a previous request
        reply_to = packet.get("reply_to")
        if reply_to and reply_to in self.pending_requests:
            # Route back to specific IPC client
            ipc_session = self.pending_requests.pop(reply_to)
            await ipc_session.send(packet)
        else:
            # Broadcast to all IPC clients
            await self.ipc.broadcast_to_ipc(packet)

    async def send_to_server(self, data):
        """Send packet to TCP server"""
        if not self.is_connected or not self.writer:
            print("[HectagonClient] Not connected to server")
            return

        try:
            payload = json.dumps(data) + "\n"
            self.writer.write(payload.encode())
            await self.writer.drain()
        except Exception as e:
            print(f"[HectagonClient] Send error: {e}")
            self.is_connected = False

    async def cleanup_tcp(self):
        """Cleanup TCP connection"""
        if self.writer:
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except Exception as e:
                print(f"[HectagonClient] Cleanup error: {e}")

        self.reader = None
        self.writer = None
        self.is_connected = False


def get_hectagon_client(server_host="127.0.0.1", server_port=9000):
    """Get HectagonClient instance"""
    return HectagonClient(server_host, server_port)
        