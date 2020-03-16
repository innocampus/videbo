from livestreaming.settings import SettingsSectionBase


class BrokerSettings(SettingsSectionBase):
    http_port: int
    http_retry_after: str
