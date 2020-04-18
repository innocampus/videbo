import asyncio
import logging
import sys
from livestreaming.network import NetworkInterfaces
from livestreaming.web import HTTPClient


async def main():
	ni: NetworkInterfaces = NetworkInterfaces.get_instance()
	log = logging.getLogger()
	log.setLevel(logging.INFO)
	ch = logging.StreamHandler(sys.stdout)
	log.addHandler(ch)
	HTTPClient.create_client_session()
	ni.start_fetching("http://localhost/server-status", logger=log)
	while True:
		print(f"is running: {ni.is_running}")
		print(f"interfaces: {ni.get_interface_names()}")
		try:
			print(f"{ni.get_server_type()}: {ni.get_server_status()} connections currently")
		except KeyError as e:
			print(e.__str__())
		await asyncio.sleep(3)

try:
	asyncio.run(main())
except KeyboardInterrupt:
	print("\rstop ")
