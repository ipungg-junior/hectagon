import asyncio
import json
from net._tcp_server import TCPServer
from net._net_manager import ConnectionManager
from net._router import PacketRouter
from extras._handler import PingPoolingHandler, RegisterHandler


async def main():
    with open("config.json") as f:
        config = json.load(f)

    tcp_config = config["net"]["tcp_server"]
    host = tcp_config["server_ip"]
    port = tcp_config["server_port"]

    PacketRouter.register("register", RegisterHandler())
    PacketRouter.register("ping", PingPoolingHandler())

    connection_manager = ConnectionManager()

    server = TCPServer(
        host=host,
        port=port,
        connection_manager=connection_manager
    )

    await server.start()


if __name__ == "__main__":
    asyncio.run(main())
