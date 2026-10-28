import json
import asyncio
from net._router import PacketRouter
from net._net_manager import get_manager
from logging_controller import get_logger

logger = get_logger(__name__)


class ClientSession:

    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer
        self.manager = get_manager()
        self.id = None
        self.is_connected = True
        self.registered = False

    async def start(self):
        addr = self.writer.get_extra_info("peername")
        logger.info("New connection from %s", addr)

        try:
            while self.is_connected:
                try:
                    data = await asyncio.wait_for(self.reader.readline(), timeout=15)
                except asyncio.TimeoutError:
                    logger.warning("[%s] Timeout - no data received", addr)
                    break

                if not data:
                    logger.info("[%s] Disconnected", addr)
                    break

                try:
                    packet = json.loads(data.decode())

                    if not self.registered:
                        await self.handle_registration(packet, addr)
                    else:
                        await self.handle_packet(packet)

                except json.JSONDecodeError:
                    logger.warning("[%s] Invalid JSON received", addr)
        except ConnectionResetError:
            logger.warning("[%s] Connection reset by peer", addr)
        except BrokenPipeError:
            logger.warning("[%s] Broken pipe", addr)
        except Exception as exc:
            logger.exception("[%s] Error: %s", addr, type(exc).__name__)
        finally:
            await self.disconnect()

    async def handle_registration(self, packet, addr):
        """Handle registration before routing to handlers"""
        if packet.get("type") == "register":
            await PacketRouter.handle(self, packet)
        else:
            logger.warning("[%s] Packet received before registration, ignoring", addr)
            await self.send({
                "type": "error",
                "message": "Must register first"
            })

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
            if self.id:
                await self.manager.remove(self.id)
        except Exception as exc:
            logger.exception("[%s] Send error: %s", self.writer.get_extra_info("peername"), exc)
            self.is_connected = False

    async def disconnect(self):
        self.is_connected = False
        if self.id:
            await self.manager.remove(self.id)
        try:
            self.writer.close()
            await self.writer.wait_closed()
        except Exception as exc:
            logger.exception("Error closing connection: %s", exc)
