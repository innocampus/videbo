from aiohttp.web import Request
from aiohttp.web import HTTPSeeOther
from aiohttp.web import HTTPForbidden
from aiohttp.web import HTTPNotFound
from aiohttp.web import HTTPConflict
from aiohttp.web import HTTPOk
from aiohttp.web import RouteTableDef
from livestreaming.auth import BaseJWTData
from livestreaming.auth import ensure_jwt_data_and_role
from livestreaming.auth import Role
from livestreaming.web import register_route_with_cors
from livestreaming.web import ensure_json_body
from livestreaming.broker import broker_logger
from livestreaming.broker.grid import BrokerGrid
from livestreaming.broker.api.models import BrokerGridModel
from livestreaming.broker.api.models import BrokerRedirectJWTData

routes = RouteTableDef()
grid = BrokerGrid()


@register_route_with_cors(routes, "GET", r"/api/broker/redirect/{stream_id:\d+}/{playlist:[a-z0-9]+}.m3u8")
@ensure_jwt_data_and_role(Role.client)
async def redirect(request: Request, data: BrokerRedirectJWTData):
    """
    :param request:
    :param data:
    :raises HTTPForbidden:
    :raises HTTPNotFound:
    :raises HTTPServiceUnavailable:
    :raises HTTPSeeOther:
    """
    playlist = request.match_info["playlist"]
    stream_id = int(request.match_info['stream_id'])

    if data.stream_id != stream_id:
        raise HTTPForbidden()

    next_stream_node = grid.get_next_stream_content_node(stream_id)
    if next_stream_node:
        next_node = grid.get_content_node(next_stream_node.node_id)
        if next_node:
            url = f"{next_node.base_url}/api/content/playlist/" \
                f"{stream_id}/{playlist}.m3u8?{request.query_string}"
            grid.increment_clients(node_id=next_stream_node.node_id)
            raise HTTPSeeOther(location=url)
        else:
            broker_logger.error(f"BrokerContentNode<{next_stream_node.node_id}> is not available anymore")
            raise HTTPConflict()
    else:
        raise HTTPNotFound()


@routes.get("/api/broker/state")
@ensure_jwt_data_and_role(Role.manager)
async def fetch_state(_: Request, __: BaseJWTData):
    """
    :param _: Mandatory for routing
    :param __: Mandatory for ensure_jwt_data_and_role()
    :raises HTTPOk:
    """
    raise HTTPOk(body=grid.json_model())


@routes.post("/api/broker/streams")
@ensure_jwt_data_and_role(Role.manager)
@ensure_json_body()
async def update_grid(_: Request, __: BaseJWTData, model: BrokerGridModel):
    """
    Updates grid via web request.
    :param _: Mandatory for routing
    :param __: Mandatory for ensure_jwt_data_and_role()
    :param model:
    :raises HTTPOk
    """
    grid.update(model)
    raise HTTPOk()
