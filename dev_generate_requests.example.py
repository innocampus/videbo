"""
This file is only meant for development purposes.
You may generate a JWT or do requests to the manager.
"""

import sys
from asyncio import get_event_loop
from videbo import load_general_settings, settings
from videbo.auth import internal_jwt_encode, external_jwt_encode, BaseJWTData
from videbo.web import HTTPClient

load_general_settings(True)

if not settings.general.dev_mode:
    print("This script can only be executed in dev mode!")
    sys.exit(1)

HTTPClient.create_client_session()
# ------------------------


jwt_data = BaseJWTData.construct(role="manager")
jwt = internal_jwt_encode(jwt_data, 24*3600)
#print(jwt)

# ------------------------


from videbo.broker.api.models import BrokerRedirectJWTData
jwt_data = BrokerRedirectJWTData.construct(role="manager", stream_id=2, rid="blub")
jwt = internal_jwt_encode(jwt_data, 24*3600)
print(jwt)


# ------------------------

from videbo.manager.api.models import LMSNewStreamParams, LMSNewStreamReturn

params = LMSNewStreamParams(ip_range=None, rtmps=False, lms_stream_instance_id=1)

jwt_data = BaseJWTData.construct(role='manager')
url = f'http://localhost:9030/api/manager/stream/new'
ret: LMSNewStreamReturn
future = HTTPClient.internal_request('POST', url, jwt_data, params, LMSNewStreamReturn)
status, ret = get_event_loop().run_until_complete(future)

print(status)
print(ret)

if not ret.error:
    jwt_data = BrokerRedirectJWTData.construct(role="manager", stream_id=ret.stream.stream_id, rid="blub")
    jwt = internal_jwt_encode(jwt_data, 24*3600)
    print(f"Broker URL: {ret.stream.viewer_broker_url}?jwt={jwt}")


# ------------------------
get_event_loop().run_until_complete(HTTPClient.close_all(None))
