# Hectagon

A lightweight, high-performance async TCP server with client architecture designed for distributed caching and real-time communication. Built with Python's asyncio for non-blocking I/O and extensible packet routing.

## Features

- **Async-First Design** - Built on Python asyncio for high concurrency
- **TCP-Based Architecture** - Efficient binary-safe JSON packet communication
- **Packet Router** - Classmethod-based routing system for extensible handlers
- **Handler-Based Processing** - Easy-to-extend packet handler pattern

## Architecture

### Server Core Architecture

```
TCP-Server
  ├── ConnectionManager (manages client sessions)
  ├── PacketRouter (routes packets to handlers)
  └── ClientSession (handles individual client connections)
```

### Client Flow

```
1. Client connects to server
2. Must send "register" packet with client ID
3. RegisterHandler validates and registers client
4. Client can now send/receive other packet types
5. PacketRouter routes packets to appropriate handlers
```

## Quick Start

### Server

```bash
# Install dependencies (asyncio is built-in)
python3 main.py
```

Server listens on `127.0.0.1:9000` (configurable in `config.json`)

### Client

```python
import asyncio
import json

async def connect():
    reader, writer = await asyncio.open_connection("127.0.0.1", 9000)
    
    # Register first
    writer.write(json.dumps({
        "type": "register",
        "request_id": "client_1"
    }).encode() + b"\n")
    await writer.drain()
    
    # Read response
    response = await reader.readline()
    print(json.loads(response))
    
    # Send ping
    writer.write(json.dumps({
        "type": "ping"
    }).encode() + b"\n")
    await writer.drain()
    
    # Close
    writer.close()
    await writer.wait_closed()

asyncio.run(connect())
```

## Packet Format

All packets are JSON with a `type` field:

```json
{"type": "register", "request_id": "client_1"}
{"type": "ping"}
{"type": "custom_data", "payload": {...}}
```

## Creating Custom Handlers

1. Create a handler class with async `handle(session, packet)` method:

```python
class MyHandler:
    async def handle(self, session, packet):
        print(f"Received: {packet}")
        await session.send({
            "type": "response_type",
            "status": "ok"
        })
```

2. Register in `main.py`:

```python
from net._router import PacketRouter
PacketRouter.register("packet_type", MyHandler())
```

3. Clients can now send `{"type": "packet_type", ...}`

## Configuration

Edit `config.json`:

```json
{
  "net": {
    "tcp_server": {
      "server_ip": "your-ip-server",
      "server_port": <port>
    }
  }
}
```

## Project Structure

```
hectagon/
├── config.json              # Server configuration
├── main.py                  # Server entry point
├── net/
│   ├── _session.py         # ClientSession - handles individual clients
│   ├── _tcp_server.py      # TCPServer - listens for connections
│   ├── _net_manager.py     # ConnectionManager - manages registered clients
│   └── _router.py          # PacketRouter - routes packets to handlers
├── extras/
│   └── _handler.py         # Built-in handlers (Ping, Register)
└── example_client.py        # Example client implementation
```

## Key Classes

### ClientSession
Manages individual client connections, handles registration requirement, routes packets.
- **Constructor**: `ClientSession(reader, writer, connection_manager)`
- **Methods**:
  - `start()` - Main connection loop
  - `send(data)` - Send JSON packet to client
  - `disconnect()` - Cleanup connection

### PacketRouter
Routes incoming packets to registered handlers (classmethod-based singleton).
- **Methods**:
  - `register(packet_type, handler)` - Register handler for packet type
  - `handle(session, packet)` - Route packet to handler

### ConnectionManager
Manages all connected and registered clients.
- **Methods**:
  - `register(id, session)` - Register authenticated client
  - `remove(session)` - Remove client on disconnect
  - `broadcast(data)` - Send to all clients
  - `send_to_client(id, data)` - Send to specific client

### TCPServer
Async TCP server that listens for connections.
- **Constructor**: `TCPServer(host, port, connection_manager)`
- **Methods**:
  - `start()` - Start listening and serve forever


## Requirements

- Python 3.7+
- No external dependencies (uses built-in asyncio)
