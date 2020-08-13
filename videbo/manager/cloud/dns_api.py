import INWX
from asyncio import get_event_loop
from contextlib import contextmanager
from dataclasses import dataclass
from ipaddress import IPv4Address, IPv6Address
import logging
from threading import Lock
from typing import List, Optional, Union
from videbo.manager import manager_settings
from .definitions import INWXApiAuthDefinition, CloudInstanceDefsController


_GenericIPAddress = Union[IPv4Address, IPv6Address]

logger = logging.getLogger('videbo-dns')


@dataclass
class DNSRecord:
    type: str  # A, AAAA, MX, ...
    name: str  # including full domain
    content: str
    ttl: int = 3600
    prio: int = 0
    id: Optional[int] = None  # internal record id by provider

    @classmethod
    def create_dynamic_node_name_from_ipv4(cls, domain: str, ip: IPv4Address):
        name = f"{manager_settings.dynamic_node_name_prefix}{ip.packed.hex()}.{domain}"
        return DNSRecord(type="A", name=name, content=str(ip))


class DNSManager:
    """Manager for the DNS records of a domain."""
    def __init__(self, domain: str):
        self.domain: str = domain
        self.cached_records: List[DNSRecord] = []

    async def get_all_records(self) -> List[DNSRecord]:
        """Get all DNS records for the domain and caches results in object."""
        records = await self._get_all_records()
        self.cached_records = records
        return records

    async def _get_all_records(self) -> List[DNSRecord]:
        """Internal method that needs to be overridden by dns provider api."""
        raise NotImplementedError

    async def get_all_dynamic_records(self) -> List[DNSRecord]:
        """Get all records that belong to a dynamic node."""
        records = await self.get_all_records()
        return [record for record in records if record.name.startswith(manager_settings.dynamic_node_name_prefix)]

    async def add_record(self, record: DNSRecord):
        await self._add_record(record)
        self.cached_records.append(record)
        logger.info(f"Added record: {record.name} {record.type} {record.content}")

    async def _add_record(self, record: DNSRecord):
        """Internal method that needs to be overridden by dns provider api."""
        raise NotImplementedError

    async def remove_record(self, record: DNSRecord):
        if record.id is None:
            return

        await self._remove_record(record)
        logger.info(f"Removed record: {record.name} {record.type} {record.content}")
        try:
            self.cached_records.remove(record)
        except ValueError:
            pass

    async def _remove_record(self, record: DNSRecord):
        """Internal method that needs to be overridden by dns provider api."""
        raise NotImplementedError

    async def create_a_record(self, subdomain: str, ip: IPv4Address) -> DNSRecord:
        return await self._create_record(subdomain, ip, 'A')

    async def create_aaaa_record(self, subdomain: str, ip: IPv6Address) -> DNSRecord:
        return await self._create_record(subdomain, ip, 'AAAA')

    async def _create_record(self, subdomain: str, ip: _GenericIPAddress, rec_type: str) -> DNSRecord:
        """
        Checks `cached_records` for a record with `matching` name and `type`.
        If found and `content` matches, returns that record.
        If `content` doesn't match, the record's `content` is changed and `add_record` is called.
        (Assumes that `add_record` does an update on existing records.)
        If no match is found at all, a new record is initialized and `add_record` is called.
        """
        rec = None
        rec_name = f'{manager_settings.dynamic_node_name_prefix}{subdomain}.{self.domain}'
        for record in self.cached_records:
            if record.name == rec_name and record.type == rec_type:
                if record.content == str(ip):
                    return record
                rec = record
                rec.content = str(ip)
                break
        if rec is None:
            rec = DNSRecord(type=rec_type, name=rec_name, content=str(ip))
        await self.add_record(rec)
        return rec

    async def remove_all_records(self, subdomain: str) -> None:
        rec_name = f'{manager_settings.dynamic_node_name_prefix}{subdomain}.{self.domain}'
        for record in self.cached_records:
            if record.name == rec_name:
                await self.remove_record(record)

    async def add_dynamic_node_name_from_ipv4(self, ip: IPv4Address) -> DNSRecord:
        """Add new record for a dynamic node and return the full host name."""
        new_record = DNSRecord.create_dynamic_node_name_from_ipv4(self.domain, ip)

        # Check if a record with this name already exists. Then do not add again.
        for record in self.cached_records:
            if record.name == new_record.name:
                return record

        await self.add_record(new_record)
        return new_record


class DNSManagerINWX(DNSManager):
    """
    Documentation - return codes:
    https://www.inwx.de/en/help/apidoc/f/ch04.html
    """
    def __init__(self, domain: str, auth: INWXApiAuthDefinition):
        super().__init__(domain)
        self.username: str = auth.username
        self.password: str = auth.password
        self.lock = Lock()  # only one request at the same time allowed

    @contextmanager
    def context_manager_session(self):
        """Context manager to ensure session gets closed."""
        with self.lock:
            api = INWX.ApiClient(api_url=INWX.ApiClient.API_LIVE_URL, api_type=INWX.ApiType.JSON_RPC)
            try:
                api.login(self.username, self.password)
            except Exception:
                raise DNSAPILoginError()

            try:
                yield api
            except Exception:
                raise DNSAPIError()
            finally:
                try:
                    api.logout()
                except Exception:
                    raise DNSAPILogoutError()

    async def _get_all_records(self) -> List[DNSRecord]:
        return await get_event_loop().run_in_executor(None, self._get_all_records_sync)

    def _get_all_records_sync(self) -> List[DNSRecord]:
        """
        Documentation - method:
        https://www.inwx.de/en/help/apidoc/f/ch02s13.html#nameserver.info
        """
        found_records: List[DNSRecord] = []
        params = {'domain': self.domain}
        with self.context_manager_session() as api:
            try:
                ret: dict = api.call_api(api_method="nameserver.info", method_params=params)
            except Exception:
                raise DNSAPICallError()
            if ret['code'] == 1000 and ret['resData']['domain'] == self.domain:
                for record in ret['resData']['record']:
                    new_record = DNSRecord(type=record['type'], name=record['name'], content=record['content'],
                                           ttl=record['ttl'], prio=record['prio'], id=record['id'])
                    found_records.append(new_record)

                return found_records
            raise DNSAPIError()

    async def _add_record(self, record: DNSRecord) -> None:
        """
        First fetches all existing records.
        If an existing record with the requested `type` and `name` exists, the record is updated.
        Otherwise, a new record is added.
        """
        existing_records = await get_event_loop().run_in_executor(None, self._get_all_records_sync)
        for ex_rec in existing_records:
            if ex_rec.type == record.type and ex_rec.name == record.name:
                record.id = ex_rec.id
                return await self._update_record(record)
        await get_event_loop().run_in_executor(None, self._add_record_sync, record)

    def _add_record_sync(self, record: DNSRecord) -> None:
        """
        Documentation - method:
        https://www.inwx.de/en/help/apidoc/f/ch02s13.html#nameserver.createRecord
        """
        params = {
            'domain': self.domain,
            'type': record.type,
            'content': record.content,
            'name': record.name,
            'ttl': record.ttl,
            'prio': record.prio,
        }
        with self.context_manager_session() as api:
            try:
                ret: dict = api.call_api(api_method="nameserver.createRecord", method_params=params)
            except Exception:
                raise DNSAPICallError()
            if ret['code'] == 1000:
                record.id = ret['resData']['id']
            else:
                raise DNSAPIError()

    async def _update_record(self, record: DNSRecord) -> None:
        await get_event_loop().run_in_executor(None, self._update_record_sync, record)

    def _update_record_sync(self, record: DNSRecord) -> None:
        """
        Documentation - method:
        https://www.inwx.de/en/help/apidoc/f/ch02s13.html#nameserver.updateRecord
        """
        params = {
            'id': record.id,
            'type': record.type,
            'name': record.name,
            'content': record.content,
            'ttl': record.ttl,
            'prio': record.prio,
        }
        with self.context_manager_session() as api:
            try:
                ret: dict = api.call_api(api_method="nameserver.updateRecord", method_params=params)
            except Exception:
                raise DNSAPICallError()
            if ret['code'] != 1000:
                raise DNSAPIError()

    async def _remove_record(self, record: DNSRecord) -> None:
        await get_event_loop().run_in_executor(None, self._remove_record_sync, record)

    def _remove_record_sync(self, record: DNSRecord) -> None:
        params = {'id': record.id}
        with self.context_manager_session() as api:
            try:
                ret: dict = api.call_api(api_method="nameserver.deleteRecord", method_params=params)
            except Exception:
                raise DNSAPICallError()
            if ret['code'] not in (1000, 2303):  # 2303 means record does not exist, don't treat this as an error
                raise DNSAPIError()
            record.id = None
            return


def get_dns_api_by_provider(definitions: CloudInstanceDefsController) -> Optional[DNSManager]:
    if definitions.domain is None:
        return None

    dns_def = definitions.dns_provider_definition
    if isinstance(dns_def, INWXApiAuthDefinition):
        return DNSManagerINWX(definitions.domain, dns_def)
    else:
        return None


class DNSAPIError(Exception):
    pass


class DNSAPILoginError(DNSAPIError):
    pass


class DNSAPILogoutError(DNSAPIError):
    pass


class DNSAPICallError(DNSAPIError):
    pass
