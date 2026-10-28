from extras._utils import debug

class PacketRouter:

    _routes = {}

    @classmethod
    def register(cls, delegation, handler):
        cls._routes[delegation] = handler
        debug(f"Setup handler for packet delegation: '{delegation}'")

    @classmethod
    async def handle(cls, session, packet):
        delegation = packet.get("delegation")

        if not delegation and not packet.get("de") == "register":
            debug("Packet missing 'delegation' field")
            debug(f"{packet}")
            return

        handler = cls._routes.get(delegation)

        if not handler:
            debug(f"No handler for packet delegation: {delegation}")
            debug(f"{packet}")
            return

        try:
            await handler.handle(session, packet)
        except Exception as e:
            debug(f"Error handling packet delegation {delegation}: {e}")
            debug(f"{packet}")
            