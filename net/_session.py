"""
HectaSession - Manages TCP session between HectagonServer and HectagonClient
"""
import json
import asyncio


class HectaSession:
    """
    TCP session between HectagonServer and HectagonClient.
    Handles persistent connection and packet transmission.
    """

    def __init__(self, reader, writer, connection_manager):
        self.reader = reader
        self.writer = writer
        self.connection_manager = connection_manager
        self.is_connected = True

    async def send(self, data):
        """Send JSON packet to server"""
        if not self.is_connected:
            return

        try:
            payload = json.dumps(data) + "\n"
            self.writer.write(payload.encode())
            await self.writer.drain()
        except Exception as e:
            print(f"[HectaSession] Send error: {e}")
            self.is_connected = False

    async def close(self):
        """Close TCP connection"""
        self.is_connected = False
        if self.writer:
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except Exception as e:
                print(f"[HectaSession] Close error: {e}")


class TenantSession:
    """
    UNIX socket session for one tenant application.
    Each tenant has unique_id and connects via UNIX socket to HectagonClient.
    """

    def __init__(self, reader, writer, tenant_id, hectagon_client):
        self.reader = reader
        self.writer = writer
        self.tenant_id = tenant_id
        self.hectagon_client = hectagon_client
        self.is_connected = True

    async def start(self):
        """Main loop for tenant session"""
        print(f"[TenantSession] Tenant {self.tenant_id} connected")

        try:
            while self.is_connected:
                try:
                    data = await asyncio.wait_for(self.reader.readline(), timeout=60)
                except asyncio.TimeoutError:
                    print(f"[TenantSession] {self.tenant_id} timeout")
                    break

                if not data:
                    print(f"[TenantSession] {self.tenant_id} disconnected")
                    break

                try:
                    packet = json.loads(data.decode())
                    await self.handle_packet(packet)
                except json.JSONDecodeError:
                    print(f"[TenantSession] {self.tenant_id} invalid JSON")

        except Exception as e:
            print(f"[TenantSession] {self.tenant_id} error: {e}")
        finally:
            await self.disconnect()

    async def handle_packet(self, packet):
        """Forward packet to server (or handle locally)"""
        # Add tenant_id to packet
        packet["tenant_id"] = self.tenant_id

        # Send to server via TCP
        await self.hectagon_client.send_to_server(packet)

        # If packet has request_id, store for response routing
        request_id = packet.get("id")
        if request_id:
            self.hectagon_client.pending_requests[request_id] = self

    async def send(self, data):
        """Send JSON packet to tenant"""
        if not self.is_connected:
            return

        try:
            payload = json.dumps(data) + "\n"
            self.writer.write(payload.encode())
            await self.writer.drain()
        except Exception as e:
            print(f"[TenantSession] Send error: {e}")
            self.is_connected = False

    async def disconnect(self):
        """Cleanup tenant session"""
        self.is_connected = False
        try:
            self.writer.close()
            await self.writer.wait_closed()
        except Exception as e:
            print(f"[TenantSession] Close error: {e}")
