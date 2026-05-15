class ConnectionManager:

    def __init__(self):
        self.clients = {}

    async def add(self, session):
        addr = session.writer.get_extra_info("peername")

    async def remove(self, session):
        if session.id and session.id in self.clients:
            del self.clients[session.id]

    async def register(self, id, session):
        session.id = id
        self.clients[id] = session

    async def broadcast(self, data):
        for session in self.clients.values():
            await session.send(data)