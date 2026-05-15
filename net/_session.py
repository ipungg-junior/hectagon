import json
import asyncio

class ClientSession:

    def __init__(self, reader, writer, connection_manager):
        self.reader = reader
        self.writer = writer
        self.connection_manager = connection_manager
        self.id = None
        self.is_connected = True

    async def start(self):
        addr = self.writer.get_extra_info("peername")
        await self.connection_manager.add(self)
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

    async def handle_packet(self, packet):
        print(packet)

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
        await self.connection_manager.remove(self)
        try:
            self.writer.close()
            await self.writer.wait_closed()
        except Exception as e:
            print(f"Error closing connection: {e}")