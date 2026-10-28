from net._net_manager import get_session_manager
from extras._utils import debug

class PingPoolingHandler:

    async def handle(self, session, packet):
        success_packet = {
            "id": packet.get("id"),
            "delegation": packet.get("delegation"),
            "command": packet.get("command"),
            "status": "success",
            "data": {
                "message": "pong"
            }
        }
        await session.send(success_packet)


class RegisterHandler:
    """Handles client registration before session starts"""

    async def handle(self, session, packet):
        debug(f'[RegisterHandler] Got registration packet: {packet}')
        client_id = packet.get("client_id")
        manager = get_session_manager()

        if not client_id:
            await session.send({
                "id": packet.get("id"),
                "delegation": packet.get("delegation"),
                "command": packet.get("command"),
                "status": "failed",
                "data": {
                    "message": "Missing client ID"
                }
            })
            return

        if manager.is_registered(client_id):
            await session.send({
                "id": packet.get("id"),
                "delegation": packet.get("delegation"),
                "command": packet.get("command"),
                "status": "failed",
                "data": {
                    "message": "Duplicate ID"
                }
            })
            return

        session.id = client_id
        await manager.register(client_id, session)

        await session.send({
            "id": packet.get("id"),
            "delegation": packet.get("delegation"),
            "command": packet.get("command"),
            "status": "success",
            "data": {
                "message": "Registration successful"
            }
        })

        session.registered = True
        debug(f"[RegisterHandler] Client {client_id} registered successfully")


class MemberHandler:
    """Handles member-related commands (e.g. list members)"""

    async def handle(self, session, packet):
        # get command parameter
        command = packet.get("command")
        
        if command == "get_member":
            # Get member from controller MemberController.get_members(packet.get("data")["membercard_no"])
            some_data_member = {
                "name": "John Doe",
                "membercard_no": "1234567890",
                "membership_level": "Gold",
                }
            
            # return response to client
            await session.send({
                "id": packet.get("id"),
                "delegation": packet.get("delegation"),
                "command": packet.get("command"),
                "status": "success",
                "data": some_data_member
            })

        else:
            await session.send({
                "id": packet.get("id"),
                "delegation": packet.get("delegation"),
                "command": packet.get("command"),
                "status": "failed",
                "data": {
                    "message": f"Unknown member command: '{command}'"
                }
            })