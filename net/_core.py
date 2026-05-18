"""
Core Hectagon Architecture:
- HectagonServer: Centralized TCP server
- HectagonClient: Daemon managing TCP connection + UNIX socket for tenants
- HectaSession: TCP session between HectagonServer and HectagonClient
"""
import asyncio
import json
import os
from net._session import HectaSession, TenantSession
from net._net_manager import get_manager

# UNIX socket path
TENANT_SOCKET_PATH = "/tmp/hectagon.sock"

# Cleanup socket on startup
if os.path.exists(TENANT_SOCKET_PATH):
    os.remove(TENANT_SOCKET_PATH)


class HectagonServer:
    """Centralized TCP server managing HectaSession connections"""

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
        """Handle HectagonClient connection"""
        connection_manager = get_manager()
        hecta_session = HectaSession(reader, writer, connection_manager)
        await self.receive_loop(hecta_session)

    async def receive_loop(self, hecta_session):
        """Receive packets from HectagonClient"""
        while hecta_session.is_connected:
            try:
                data = await asyncio.wait_for(
                    hecta_session.reader.readline(),
                    timeout=30
                )
            except asyncio.TimeoutError:
                print("[HectagonServer] HectaSession timeout")
                break
            except Exception as e:
                print(f"[HectagonServer] Error: {e}")
                break

            if not data:
                print("[HectagonServer] HectaSession disconnected")
                break

            try:
                packet = json.loads(data.decode())
                print(f"[HectagonServer] Received from tenant: {packet}")
                # Handle packet (route to handler)
            except json.JSONDecodeError:
                print("[HectagonServer] Invalid JSON")

        await hecta_session.close()


class TenantRegistry:
    """Manages UNIX socket connections for tenants"""

    def __init__(self, hectagon_client):
        self.hectagon_client = hectagon_client
        self.tenant_counter = 0

    async def start(self):
        """Start UNIX socket server for tenants"""
        server = await asyncio.start_unix_server(
            self.handle_tenant,
            TENANT_SOCKET_PATH
        )

        print(f"[TenantRegistry] Listening on {TENANT_SOCKET_PATH}")
        async with server:
            await server.serve_forever()

    async def handle_tenant(self, reader, writer):
        """Handle new tenant connection"""
        # Generate unique tenant_id
        self.tenant_counter += 1
        tenant_id = f"tenant_{self.tenant_counter}"

        tenant_session = TenantSession(reader, writer, tenant_id, self.hectagon_client)

        try:
            await tenant_session.start()
        finally:
            connection_manager = get_manager()
            await connection_manager.remove(tenant_id)


class HectagonClient:
    """
    Client daemon:
    - Maintains persistent TCP connection to HectagonServer (HectaSession)
    - Provides UNIX socket for tenants (TenantRegistry)
    - Bridges packet routing between TCP and UNIX socket
    """

    def __init__(self, server_host, server_port):
        self.server_host = server_host
        self.server_port = server_port

        # TCP connection state (HectaSession)
        self.hecta_reader = None
        self.hecta_writer = None
        self.is_connected = False

        # Tenant management
        self.tenant_registry = TenantRegistry(self)
        self.pending_requests = {}  # request_id → TenantSession

        # Reconnect config
        self.reconnect_interval = 7

    async def start(self):
        """Start client daemon"""
        print("[HectagonClient] Starting...")

        # Start UNIX socket for tenants
        tenant_task = asyncio.create_task(self.tenant_registry.start())

        # Start TCP connection to server
        tcp_task = asyncio.create_task(self.maintain_tcp_connection())

        # Run both concurrently
        await asyncio.gather(tenant_task, tcp_task)

    async def maintain_tcp_connection(self):
        """Maintain persistent TCP connection to server"""
        while True:
            try:
                await self.connect_to_server()
                await self.receive_from_server()
            except Exception as e:
                print(f"[HectagonClient] Error: {e}")

            print(f"[HectagonClient] Reconnecting in {self.reconnect_interval}s...")
            await asyncio.sleep(self.reconnect_interval)

    async def connect_to_server(self):
        """Connect to HectagonServer"""
        print(f"[HectagonClient] Connecting to {self.server_host}:{self.server_port}...")

        self.hecta_reader, self.hecta_writer = await asyncio.open_connection(
            self.server_host,
            self.server_port
        )

        self.is_connected = True
        print("[HectagonClient] Connected to server")

    async def receive_from_server(self):
        """Receive packets from server"""
        while self.is_connected:
            try:
                data = await asyncio.wait_for(
                    self.hecta_reader.readline(),
                    timeout=30
                )
            except asyncio.TimeoutError:
                print("[HectagonClient] Server timeout")
                break
            except Exception as e:
                print(f"[HectagonClient] Receive error: {e}")
                break

            if not data:
                print("[HectagonClient] Server disconnected")
                break

            try:
                packet = json.loads(data.decode())
                await self.route_to_tenant(packet)
            except json.JSONDecodeError:
                print("[HectagonClient] Invalid JSON from server")

        self.is_connected = False
        await self.close_tcp()

    async def route_to_tenant(self, packet):
        """Route server packet to tenant"""
        tenant_id = packet.get("tenant_id")
        request_id = packet.get("reply_to")

        if request_id and request_id in self.pending_requests:
            # Route to specific tenant
            tenant = self.pending_requests.pop(request_id)
            await tenant.send(packet)
        elif tenant_id:
            # Route to specific tenant by ID
            connection_manager = get_manager()
            await connection_manager.send_to_tenant(tenant_id, packet)
        else:
            # Broadcast to all tenants
            connection_manager = get_manager()
            await connection_manager.broadcast_to_tenants(packet)

    async def send_to_server(self, packet):
        """Send packet from tenant to server"""
        if not self.is_connected or not self.hecta_writer:
            print("[HectagonClient] Not connected to server")
            return

        try:
            payload = json.dumps(packet) + "\n"
            self.hecta_writer.write(payload.encode())
            await self.hecta_writer.drain()
        except Exception as e:
            print(f"[HectagonClient] Send error: {e}")
            self.is_connected = False

    async def close_tcp(self):
        """Close TCP connection to server"""
        if self.hecta_writer:
            try:
                self.hecta_writer.close()
                await self.hecta_writer.wait_closed()
            except Exception as e:
                print(f"[HectagonClient] Close error: {e}")

        self.hecta_reader = None
        self.hecta_writer = None
        self.is_connected = False


def get_hectagon_client(server_host="127.0.0.1", server_port=9000):
    """Create HectagonClient instance"""
    return HectagonClient(server_host, server_port)
