from extras._utils import debug

class HectaSessionManager:

    def __init__(self):
        self.sessions = {}

    async def register(self, session_id, session):
        """Register authenticated client"""
        self.sessions[session_id] = session
        debug(f"[HectaSessionManager] Registered: {session_id} ({len(self.sessions)} clients)")

    async def remove(self, session_id):
        """Remove client on disconnect"""
        if session_id in self.sessions:
            del self.sessions[session_id]
            debug(f"[HectaSessionManager] Disconnected: {session_id} ({len(self.sessions)} clients)")

    async def send_to_client(self, session_id, data):
        """Send to specific client"""
        session = self.sessions.get(session_id)
        if session:
            await session.send(data)

    async def broadcast(self, data):
        """Send to all registered clients"""
        for session in self.sessions.values():
            await session.send(data)

    def get_client(self, session_id):
        """Get session by client ID"""
        return self.sessions.get(session_id)

    def is_registered(self, session_id):
        """Check if client is registered"""
        return session_id in self.sessions


# Module-level singleton instance
_manager = HectaSessionManager()
def get_session_manager():
    """Get the global HectaSessionManager instance"""
    return _manager