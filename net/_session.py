import json
import asyncio
from net._router import PacketRouter
from net._net_manager import get_session_manager
from extras._utils import debug


class HectaSession:

    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer
        self.manager = get_session_manager()
        self.id = None
        self.is_connected = True
        self.registered = False

    async def start(self):
        addr = self.writer.get_extra_info("peername")
        debug(f"[HectaSession] New connection from {addr}")

        try:
            while self.is_connected:
                try:
                    data = await asyncio.wait_for(self.reader.readline(), timeout=120)
                except asyncio.TimeoutError:
                    debug(f"[{addr}] Timeout - no data received")
                    break

                if not data:
                    debug(f"[{addr}] Disconnected")
                    break

                try:
                    # convert json string to dict
                    packet = json.loads(
                        json.loads(data.decode())
                    )
                    await PacketRouter.handle(self, packet)
                except json.JSONDecodeError:
                    debug(f"[{addr}] Invalid JSON received")
                    
        except ConnectionResetError:
            debug(f"[{addr}] Connection reset by peer")
        except BrokenPipeError:
            debug(f"[{addr}] Broken pipe")
        except Exception as e:
            debug(f"[{addr}] Error: {type(e).__name__}: {e}")
        finally:
            await self.disconnect()

    async def handle_registration(self, packet, addr):
        """Handle registration before routing to handlers"""
        if packet.get("type") == "register":
            await PacketRouter.handle(self, packet)
        else:
            debug(f"[{addr}] Packet received before registration, ignoring")
            error_packet = {
                "id": packet.get("id"),
                "delegation": packet.get("delegation"),
                "command": packet.get("command"),
                "status": "failed",
                "data": {
                    "message": "Must register first"
                }
            }
            await self.send(error_packet)

    async def send(self, data):
        if not self.is_connected:
            return

        try:
            payload = json.dumps(data) + "\n"
            self.writer.write(payload.encode())
            await self.writer.drain()
        except (ConnectionResetError, BrokenPipeError):
            self.is_connected = False
            if self.id:
                await self.manager.remove(self.id)
        except Exception as e:
            debug(f"[{self.writer.get_extra_info('peername')}] Send error: {e}")
            self.is_connected = False

    async def disconnect(self):
        self.is_connected = False
        if self.id:
            await self.manager.remove(self.id)
        try:
            self.writer.close()
            await self.writer.wait_closed()
        except Exception as e:
            debug(f"Error closing connection: {e}")


class HectagonIPCSession:
    """Represents one local IPC client connection (via UNIX socket)"""

    def __init__(self, reader, writer, hecta_session):
        self.reader = reader
        self.writer = writer
        self.hecta_session = hecta_session
        self.is_connected = True
        self.id = None

    async def start(self):
        """Main loop for IPC session"""
        try:
            while self.is_connected:
                try:
                    data = await self.reader.readline()
                except asyncio.TimeoutError:
                    debug("[HectagonIPCSession] Timeout")
                    break

                if not data:
                    debug("[HectagonIPCSession] Local client disconnected")
                    break

                try:
                    # Extract JSON packet and forward to HectagonClient
                    packet = json.loads(data.decode())
                    # add session id to packet for routing
                    packet["session_id"] = self.id
                    await self.hecta_session.send_to_server(json.dumps(packet))
                except json.JSONDecodeError:
                    await self.send({
                        "type": "error",
                        "message": "Invalid JSON format"
                    })
                    debug("[HectagonIPCSession] Invalid JSON")

        except Exception as e:
            debug(f"[HectagonIPCSession] Error: {e}")
        finally:
            await self.disconnect()

    async def send(self, data):
        """Send JSON packet to local app"""
        if not self.is_connected:
            return
        try:
            payload = json.dumps(data) + "\n"
            self.writer.write(payload.encode())
            await self.writer.drain()
        except Exception as e:
            debug(f"[HectagonIPCSession] Send error: {e}")
            self.is_connected = False

    async def disconnect(self):
        """Cleanup IPC session"""
        self.is_connected = False
        try:
            self.writer.close()
            await self.writer.wait_closed()
        except Exception as e:
            debug(f"[HectagonIPCSession] Close error: {e}")
