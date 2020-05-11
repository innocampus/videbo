from typing import Any
from videbo.manager.api.models import NodesStatusList, CreateNewDistributorNodeParams,\
    SetDistributorStatusParams, RemoveDistributorNodeParams
from videbo.web import HTTPClient


def get_manager_url(path: str) -> str:
    from videbo import settings
    port = settings.get_config("manager", "http_port")
    return f"http://localhost:{port}{path}"


async def get_all_nodes(args: Any):
    url = get_manager_url("/api/manager/admin/nodes")
    ret: NodesStatusList
    status, ret = await HTTPClient.internal_request_admin("GET", url, None, NodesStatusList)
    if status == 200:
        print("List of all nodes:\n")
        for node in ret.nodes:
            print(f"ID: {node.id}")
            print(f"Type: {node.type}")
            print(f"Name: {node.name}")
            print(f"Base URL: {node.base_url}")
            print(f"Operational: {node.operational}")
            print(f"Created manually: {node.created_manually}")
            print(f"Shutdown: {node.shutdown}")
            print(f"Enabled: {node.enabled}")
            print(f"\n")


async def create_distributor_node(args: Any):
    url = get_manager_url("/api/manager/admin/create_distributor_node")
    params = CreateNewDistributorNodeParams(definition_name=args.definition,
                                            bound_to_storage_node_base_url=args.bound_to_storage_url)
    status, ret = await HTTPClient.internal_request_admin("POST", url, params)
    print_success(status)


async def remove_distributor_node(args: Any):
    url = get_manager_url("/api/manager/admin/remove_distributor_node")
    params = RemoveDistributorNodeParams(node_name=args.node)
    status, ret = await HTTPClient.internal_request_admin("POST", url, params)
    print_success(status)


async def set_distributor_status(args: Any, enable: bool):
    url = get_manager_url("/api/manager/admin/set_distributor_node_status")
    params = SetDistributorStatusParams(node_name=args.node, enable=enable)
    status, ret = await HTTPClient.internal_request_admin("POST", url, params)
    print_success(status)


def print_success(status: int):
    if status == 200:
        print("Successful! Please check the output of the manager.")
    else:
        print("!!!ERROR!!! Please check the output of the manager.")
