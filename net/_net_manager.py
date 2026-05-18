"""
ConnectionManager - Manages tenant sessions at UNIX socket layer
Maps tenant_id → TenantSession for local UNIX socket connections
"""


class ConnectionManager:
    """
    Manages all tenant sessions connected via UNIX socket to HectagonClient.
    One ConnectionManager per HectagonClient daemon.
    """

    def __init__(self):
        self.tenants = {}  # tenant_id → TenantSession

    async def register(self, tenant_id, tenant_session):
        """Register new tenant"""
        self.tenants[tenant_id] = tenant_session
        print(f"[ConnectionManager] Tenant registered: {tenant_id} ({len(self.tenants)} total)")

    async def remove(self, tenant_id):
        """Remove tenant on disconnect"""
        if tenant_id in self.tenants:
            del self.tenants[tenant_id]
            print(f"[ConnectionManager] Tenant disconnected: {tenant_id} ({len(self.tenants)} total)")

    async def send_to_tenant(self, tenant_id, data):
        """Send packet to specific tenant"""
        tenant = self.tenants.get(tenant_id)
        if tenant:
            await tenant.send(data)

    async def broadcast_to_tenants(self, data):
        """Broadcast packet to all connected tenants"""
        for tenant in list(self.tenants.values()):
            await tenant.send(data)

    def get_tenant(self, tenant_id):
        """Get tenant session by ID"""
        return self.tenants.get(tenant_id)

    def is_tenant_connected(self, tenant_id):
        """Check if tenant is connected"""
        return tenant_id in self.tenants


# Module-level singleton instance
_manager = ConnectionManager()


def get_manager():
    """Get the global ConnectionManager instance"""
    return _manager
