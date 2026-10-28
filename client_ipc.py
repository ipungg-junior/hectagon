#!/usr/bin/env python3
"""
Hectagon Client Daemon

This is a service that runs in the background and:
1. Maintains persistent TCP connection to Hectagon server
2. Provides local IPC via UNIX socket (/tmp/hectagon.sock)
3. Bridges TCP ↔ IPC communication

Usage:
    python3 hectagon_client.py
"""

import asyncio
import json
import sys
from net._core import HectagonClient


async def main():
    # Get server config from environment or defaults
    server_host = "127.0.0.1"
    server_port = 9000

    # Create and start client
    client = HectagonClient(server_host, server_port)

    try:
        await client.start()
    except KeyboardInterrupt:
        print("\n[HectagonClient] Shutting down...")
        sys.exit(0)


if __name__ == "__main__":
    asyncio.run(main())
