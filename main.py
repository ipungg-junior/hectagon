import asyncio
import json
from net._core import HectagonServer
from net._router import PacketRouter
from extras._handler import PingPoolingHandler, RegisterHandler


async def main():
    with open("config.json") as f:
        config = json.load(f)

    tcp_config = config["net"]["tcp_server"]
    host = tcp_config["server_ip"]
    port = tcp_config["server_port"]

    # Register handlers
    PacketRouter.register("register", RegisterHandler())
    PacketRouter.register("ping", PingPoolingHandler())

    # Create and start server
    server = HectagonServer(host=host, port=port)
    await server.start()


if __name__ == "__main__":
    asyncio.run(main())
