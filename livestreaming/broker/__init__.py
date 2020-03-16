from .settings import BrokerSettings
from livestreaming.web import start_web_server
from logging import getLogger

broker_settings = BrokerSettings()
broker_logger = getLogger("livestreaming-broker")


def start() -> None:
    from .api.routes import routes
    broker_settings.load()
    start_web_server(broker_settings.http_port, routes)
