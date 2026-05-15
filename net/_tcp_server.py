import asyncio
from net._session import ClientSession

class TCPServer:

    def __init__(self, host, port, connection_manager):
        self.host = host
        self.port = port
        self.connection_manager = connection_manager

    async def start(self):

        server = await asyncio.start_server(
            self.handle_client,
            self.host,
            self.port
        )

        async with server:
            await server.serve_forever()

    async def handle_client(self, reader, writer):

        session = ClientSession(
            reader,
            writer,
            self.connection_manager
        )

        await session.start()