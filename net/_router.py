
class PacketRouter:

    _routes = {}

    @classmethod
    def register(cls, packet_type, handler):
        cls._routes[packet_type] = handler
        print(f"Setup handler for packet type: '{packet_type}'")

    @classmethod
    async def handle(cls, session, packet):
        packet_type = packet.get("type")

        if not packet_type:
            print("Packet missing 'type' field")
            return

        handler = cls._routes.get(packet_type)

        if not handler:
            print(f"No handler for packet type: {packet_type}")
            return

        try:
            await handler.handle(session, packet)
        except Exception as e:
            print(f"Error handling packet type {packet_type}: {e}")