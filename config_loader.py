import json
from pathlib import Path


def load_config(config_path="config.json"):
    """Load config.json safely and validate required keys."""
    path = Path(config_path)

    if not path.exists():
        raise FileNotFoundError(
            f"Missing config file: '{config_path}'. "
            "Copy config.example to config.json and adjust the values."
        )

    try:
        with path.open("r", encoding="utf-8") as file:
            config = json.load(file)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in '{config_path}': {exc}") from exc

    net_section = config.get("net")
    tcp_server = net_section.get("tcp_server") if isinstance(net_section, dict) else None

    if not isinstance(net_section, dict) or not isinstance(tcp_server, dict):
        raise ValueError(
            "Config error: expected structure {'net': {'tcp_server': {...}}}"
        )

    host = tcp_server.get("server_ip")
    port = tcp_server.get("server_port")

    if not isinstance(host, str) or not host.strip():
        raise ValueError("Config error: 'net.tcp_server.server_ip' must be a non-empty string")

    if not isinstance(port, int) or not (0 < port <= 65535):
        raise ValueError("Config error: 'net.tcp_server.server_port' must be an integer between 1 and 65535")

    return config
