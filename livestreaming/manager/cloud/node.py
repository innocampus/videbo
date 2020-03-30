class Node:
    def __init__(self, name, node_type, platform, deployment_status, ip, vm_status="unknown", vm_id="unknown",
                 provider="unknown"):
        self.name = name
        self.node_type = node_type
        self.platform = platform
        self.deployment_status = deployment_status
        self.vm_status = vm_status
        self.ip = ip
        self.id = vm_id
        self.provider = provider

    def __str__(self):
        return f"[Node name: {self.name}; node_type: {self.node_type}; platform: {self.platform};  " \
               f"deployment status: {self.deployment_status}; vm status: {self.vm_status}; " \
               f"ip: {self.ip}; id: {self.id}, provider: {self.provider}] "
