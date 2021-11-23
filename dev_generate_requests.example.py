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
get_event_loop().run_until_complete(HTTPClient.close_all())
