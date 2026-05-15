class ConnectionManager:

    def __init__(self):
        self.clients = {}

    async def register(self, client_id, session):
        """Register authenticated client"""
        self.clients[client_id] = session
        print(f"[ConnectionManager] Registered: {client_id} ({len(self.clients)} clients)")

    async def remove(self, client_id):
        """Remove client on disconnect"""
        if client_id in self.clients:
            del self.clients[client_id]
            print(f"[ConnectionManager] Disconnected: {client_id} ({len(self.clients)} clients)")

    async def send_to_client(self, client_id, data):
        """Send to specific client"""
        session = self.clients.get(client_id)
        if session:
            await session.send(data)

    async def broadcast(self, data):
        """Send to all registered clients"""
        for session in self.clients.values():
            await session.send(data)

    def get_client(self, client_id):
        """Get session by client ID"""
        return self.clients.get(client_id)

    def is_registered(self, client_id):
        """Check if client is registered"""
        return client_id in self.clients


# Module-level singleton instance
_manager = ConnectionManager()


def get_manager():
    """Get the global ConnectionManager instance"""
    return _manager
