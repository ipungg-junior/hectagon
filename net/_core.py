import asyncio
from net._session import ClientSession

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
