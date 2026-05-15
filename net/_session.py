import json
import asyncio
from net._router import PacketRouter

class ClientSession:

    def __init__(self, reader, writer, connection_manager):
        self.reader = reader
        self.writer = writer
        self.connection_manager = connection_manager
        self.id = None
        self.is_connected = True
        self.registered = False
        self.remote_addr = None

    async def start(self):
        addr = self.writer.get_extra_info("peername")
        self.remote_addr = addr

        try:
            while self.is_connected:
                try:
                    data = await asyncio.wait_for(self.reader.readline(), timeout=15)
                except asyncio.TimeoutError:
                    print(f"[{addr}] Timeout - no data received")
                    break

                if not data:
                    print(f"[{addr}] Disconnected")
                    break

                try:
                    packet = json.loads(data.decode())

                    if not self.registered:
                        await self.handle_registration(packet, addr)
                    else:
                        await self.handle_packet(packet)

                except json.JSONDecodeError:
                    print(f"[{addr}] Invalid JSON received")
        except ConnectionResetError:
            print(f"[{addr}] Connection reset by peer")
        except BrokenPipeError:
            print(f"[{addr}] Broken pipe")
        except Exception as e:
            print(f"[{addr}] Error: {type(e).__name__}: {e}")
        finally:
            await self.disconnect()

    async def handle_registration(self, packet, addr):
        """Handle registration before routing to handlers"""
        if packet.get("type") == "register":
            await PacketRouter.handle(self, packet)
        else:
            print(f"[{addr}] Packet received before registration, ignoring")
            await self.send({
                "type": "register_response",
                "status": "failed",
                "message": "Must register first"
            })
            await self.disconnect()

    async def handle_packet(self, packet):
        """Route registered client packets to handlers"""
        await PacketRouter.handle(self, packet)

    async def send(self, data):
        if not self.is_connected:
            return

        try:
            payload = json.dumps(data) + "\n"
            self.writer.write(payload.encode())
            await self.writer.drain()
        except (ConnectionResetError, BrokenPipeError):
            self.is_connected = False
            await self.connection_manager.remove(self)
        except Exception as e:
            print(f"[{self.writer.get_extra_info('peername')}] Send error: {e}")
            self.is_connected = False

    async def disconnect(self):
        self.is_connected = False
        try:
            await self.connection_manager.remove(self)
            self.writer.close()
            await self.writer.wait_closed()
            print(f'Remove {self.id} from client_pool')
        except Exception as e:
            print(f"Error closing connection: {e}")