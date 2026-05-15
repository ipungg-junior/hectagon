from net._net_manager import get_manager


class PingPoolingHandler:

    async def handle(self, session, packet):
        data = {'type': 'ping_response', 'status': 'ok', 'message': 'pong'}
        await session.send(data)
        print(f"[PingPoolingHandler] Ping from {session.id}")


class RegisterHandler:
    """Handles client registration before session starts"""

    async def handle(self, session, packet):
        client_id = packet.get("request_id")
        manager = get_manager()

        if not client_id:
            await session.send({
                "type": "register_response",
                "status": "failed",
                "error": "Missing client ID"
            })
            return

        if manager.is_registered(client_id):
            await session.send({
                "type": "register_response",
                "status": "failed",
                "error": "Duplicate ID"
            })
            return

        session.id = client_id
        await manager.register(client_id, session)

        await session.send({
            "type": "register_response",
            "status": "ok",
            "request_id": client_id
        })

        session.registered = True
        print(f"[RegisterHandler] Client {client_id} registered successfully")
