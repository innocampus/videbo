from videbo.settings import SettingsSectionBase


class BrokerSettings(SettingsSectionBase):
    _section = "broker"
    http_port: int
    http_retry_after: str
