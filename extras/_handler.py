
# Example handler class
class PingPoolingHandler:
    
    async def handle(self, session, packet):
        data = {'type': 'ping_response', 'status': 'ok', 'message':'pong'}
        await session.send(data)
        print(f"[PingPoolingJHandler] Ping from session {session.remote_addr}")
            
            
class RegisterHandler:
    """Handles client registration before session starts"""

    async def handle(self, session, packet):
        client_id = packet.get("request_id")

        if not client_id:
            await session.send({
                "type": "register_response",
                "status": "failed",
                "error": "Missing client ID"
            })
            return
        
        if client_id in session.connection_manager.clients:
            await session.send({
                "type": "register_response",
                "status": "failed",
                "error": "Duplicate ID"
            })            
            return

        session.id = client_id
        await session.connection_manager.register(client_id, session)

        await session.send({
            "type": "register_response",
            "status": "ok",
            "request_id": client_id
        })

        session.registered = True
        print(f"[RegisterHandler] Client {client_id} registered successfully")
