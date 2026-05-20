import asyncio, json, time, os
from net._session import HectaSession, HectagonIPCSession
from extras._utils import debug

# UNIX socket path
IPC_SOCKET_PATH = "/tmp/hectagon.sock"

# Cleanup socket on startup
if os.path.exists(IPC_SOCKET_PATH):
    os.remove(IPC_SOCKET_PATH)


class HectagonServer:

    def __init__(self, host, port, verbose=True):        
        self.host = host
        self.port = port
        self.verbose = verbose
        if self.verbose:
            debug("[HectagonServer] Initializing HectagonServer")

    async def start(self):
        server = await asyncio.start_server(
            self.handle_client,
            self.host,
            self.port
        )

        if self.verbose:
            debug(f"[HectagonServer] Listening on '{self.host}:{self.port}'")
        async with server:
            await server.serve_forever()

    async def handle_client(self, reader, writer):
        session = HectaSession(reader, writer)
        await session.start()


class HectagonIPC:
    """Local UNIX domain socket IPC server"""

    def __init__(self, client_layer):
        debug("[HectagonIPC] Initializing HectagonIPC")
        self.client_layer = client_layer
        self.sessions = {}
        self.session_counter = 0

    async def start(self):
        """Start UNIX socket server"""
        server = await asyncio.start_unix_server(
            self.handle_client,
            IPC_SOCKET_PATH
        )

        debug(f"[HectagonIPC] Listening on '{IPC_SOCKET_PATH}'")
        async with server:
            await server.serve_forever()

    async def handle_client(self, reader, writer):
        """Handle new local app connection"""
        session_id = self.session_counter
        self.session_counter += 1

        session = HectagonIPCSession(reader, writer, self.client_layer)
        # Set IPC session ID and store in sessions dict
        session.id = session_id
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
        debug(f"[HectagonClient] Initializing HectagonClient for '{server_host}:{server_port}'")
        # TCP connection state
        self.reader = None
        self.writer = None
        self.is_connected = False
        self.unique_id = f"client_{int(time.time())}"
        self.ping_time_counter = time.perf_counter()

        # IPC server
        self.ipc = HectagonIPC(self)
        self.packet_map = {}

        # Reconnect config
        self.reconnect_interval = 7  # seconds

    def _generate_unique_id(self):
        """Generate a unique client ID"""
        return int(time.time())

    async def start(self):
        """Start client layer"""
        debug("[HectagonClient] Waiting for starting...")
        # Start IPC server
        ipc_task = asyncio.create_task(self.ipc.start())
        # Start TCP connection manager
        tcp_task = asyncio.create_task(self.maintain_tcp_connection())
        # Wait for both (they run indefinitely)
        await asyncio.gather(ipc_task, tcp_task)
        
    async def ping_loop(self):
        """Send periodic ping to server to keep connection alive"""
        while self.is_connected:
            await asyncio.sleep(10)  # Ping every 30 seconds
            await self.send_to_server(json.dumps({
                "id": self._generate_unique_id(),
                "client_id": self.unique_id,
                "delegation": "ping",
                "command": "ping",
                "data": {}
            }))
            self.ping_time_counter = time.perf_counter()

    async def maintain_tcp_connection(self):
        """Maintain persistent TCP connection with auto-reconnect"""
        while True:
            try:
                await self.connect_to_server()
                await self.tcp_receive_loop()
            except Exception as e:
                debug(f"[HectagonClient] Error: {e}")

            # Reconnect after 7 seconds
            debug(f"[HectagonClient] Reconnecting in {self.reconnect_interval}s...")
            await asyncio.sleep(self.reconnect_interval)

    async def connect_to_server(self):
        """Connect to Hectagon server"""
        debug(f"[HectagonClient] Connecting to HectagonServer...")

        self.reader, self.writer = await asyncio.open_connection(
            self.server_host,
            self.server_port
        )

        self.is_connected = True
        debug("[HectagonClient] Connected to server")

        # Send register packet
        debug("[HectagonClient] Sending register packet to server")
        await self.send_to_server(json.dumps({
            "id": 1,
            "client_id": self.unique_id,
            "delegation": "register",
            "command": "register",
            "data": {}
        }))
        
        # Create task ping loop
        asyncio.create_task(self.ping_loop())   

    async def tcp_receive_loop(self):
        """Receive packets from TCP server"""
        while self.is_connected:
            try:
                data = await asyncio.wait_for(
                    self.reader.readline(), timeout=560
                )
            except asyncio.TimeoutError:
                debug("[HectagonClient] TCP timeout")
                break
            except Exception as e:
                debug(f"[HectagonClient] TCP error: {e}")
                break

            if not data:
                debug("[HectagonClient] Server disconnected")
                break

            try:
                packet = json.loads(data.decode())
                
                if not packet['delegation'] == 'register' and not packet['command'] == 'register':
                    # Send to spesific IPC client
                    await self.handle_server_packet(packet)
                else:
                    if packet['status'] == 'failed':
                        debug(f"[HectagonClient] Registration failed - {packet['data']['message']}")
                        
                    
            except json.JSONDecodeError:
                debug("[HectagonClient] Invalid JSON from server")

        self.is_connected = False
        await self.cleanup_tcp()

    async def handle_server_packet(self, packet):
        """Handle packet from server"""
        
        # Get if exists session_id from packet_map using packet ID
        packet_id = packet.get("id")
        session_id = self.packet_map.get(packet_id)
        if session_id is not None:
            debug(f"[HectagonClient] Found session ID {session_id} for packet ID {packet_id}")
            ipc_session = self.ipc.sessions.get(session_id)
            if ipc_session:
                debug(f"[HectagonClient] Sending packet to IPC session {session_id}")
                await ipc_session.send(packet)
                # delete packet ID from packet_map after sending response to IPC client
                del self.packet_map[packet_id]
            else:
                debug(f"[HectagonClient] IPC session {session_id} not found")
        else:
            # If not found session_id, broadcast to all IPC clients (if no PING packet)
            if packet.get("delegation") != "ping":
                debug("[HectagonClient] No session ID found for packet, broadcasting to all IPC clients")
                await self.ipc.broadcast_to_ipc(packet)
        

    async def send_to_server(self, data):
        """Send packet to TCP server"""
        if not self.is_connected or not self.writer:
            debug("[HectagonClient] Not connected to server")
            return

        try:
            
            # if data has a session_id field, save it to packet_map with session_id as key and session IPC as a value, so we can reply to specific IPC client later
            json_data = json.loads(data)
            get_session_id = json_data.get("session_id")          
            if get_session_id is not None:
                # Record packet id and session_id for replying to specific IPC client later
                packet_id = json_data["id"]
                session_id = json_data["session_id"]                
                # Connect packet ID to session ID 
                self.packet_map[packet_id] = session_id
                debug(f"[HectagonClient] Recorded packet ID {packet_id} for session ID {session_id}")
            
            payload = json.dumps(data) + "\n"
            self.writer.write(payload.encode())
            await self.writer.drain()
        except Exception as e:
            debug(f"[HectagonClient] Send error: {e}")
            self.is_connected = False

    async def cleanup_tcp(self):
        """Cleanup TCP connection"""
        if self.writer:
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except Exception as e:
                debug(f"[HectagonClient] Cleanup error: {e}")

        self.reader = None
        self.writer = None
        self.is_connected = False


def get_hectagon_client(server_host="127.0.0.1", server_port=9000):
    """Get HectagonClient instance"""
    return HectagonClient(server_host, server_port)
        