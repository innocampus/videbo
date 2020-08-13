import logging
import unittest
from unittest.mock import patch, MagicMock, call
from ipaddress import IPv4Address, IPv6Address
from time import time

from tests.base import BaseTestCase, load_basic_config_from, async_test, AsyncMock
from videbo.manager.cloud import dns_api
from videbo import settings


logger = logging.getLogger(__name__)


TESTED_MODULE_PATH = 'videbo.manager.cloud.dns_api'
MANAGER_SETTINGS_PATH = TESTED_MODULE_PATH + '.manager_settings'


class DNSManagerTestCase(BaseTestCase):

    MOCK_DOMAIN = 'test-domain-00000.xyz'
    MOCK_DYN_NAME_PREFIX = 'dyn-'

    def setUp(self) -> None:
        super().setUp()
        self.settings_patcher = patch(MANAGER_SETTINGS_PATH)
        self.mock_settings = self.settings_patcher.start()
        self.mock_settings.dynamic_node_name_prefix = self.MOCK_DYN_NAME_PREFIX

        self.mgr = dns_api.DNSManager(self.MOCK_DOMAIN)

    def tearDown(self) -> None:
        super().tearDown()
        self.settings_patcher.stop()

    @async_test
    @patch.object(dns_api.DNSManager, '_get_all_records', new_callable=AsyncMock)
    async def test_get_all_records(self, mock__get_all_records):
        mock_records = [MagicMock()]
        mock__get_all_records.return_value = mock_records

        output = await self.mgr.get_all_records()

        self.assertEqual(output, mock_records)
        mock__get_all_records.assert_awaited_once()
        self.assertEqual(self.mgr.cached_records, mock_records)

    @async_test
    async def test__get_all_records(self):
        with self.assertRaises(NotImplementedError):
            await self.mgr._get_all_records()

    @async_test
    @patch.object(dns_api.DNSManager, 'get_all_records', new_callable=AsyncMock)
    async def test_get_all_dynamic_records(self, mock_get_all_records):
        # Mocking dynamic node record:
        mock_dyn_record, mock_dyn_name = MagicMock(), self.MOCK_DYN_NAME_PREFIX + 'something'
        mock_dyn_record.name = mock_dyn_name
        # Mocking other records:
        mock_other_name = 'something-else'
        mock_other_record1, mock_other_record2 = MagicMock(), MagicMock()
        mock_other_record1.name, mock_other_record2.name = mock_other_name, mock_other_name
        # Set mocked records up as return value:
        mock_records = [mock_other_record1, mock_dyn_record, mock_other_record2]
        mock_get_all_records.return_value = mock_records
        # Initialize and call:
        output = await self.mgr.get_all_dynamic_records()
        # Expect only `mock_dyn_record` to be in the returned list:
        self.assertEqual(output, [mock_dyn_record])
        mock_get_all_records.assert_awaited_once()

    @async_test
    @patch.object(dns_api, 'DNSRecord')
    @patch.object(dns_api.DNSManager, '_add_record', new_callable=AsyncMock)
    async def test_add_record(self, mock__add_record, mock_record):
        with self.assertLogs():
            await self.mgr.add_record(mock_record)
        mock__add_record.assert_awaited_once_with(mock_record)
        self.assertIn(mock_record, self.mgr.cached_records)

    @async_test
    @patch.object(dns_api, 'DNSRecord')
    async def test__add_record(self, mock_record):
        with self.assertRaises(NotImplementedError):
            await self.mgr._add_record(mock_record)

    @async_test
    @patch.object(dns_api, 'DNSRecord')
    @patch.object(dns_api.DNSManager, '_remove_record', new_callable=AsyncMock)
    async def test_remove_record(self, mock__remove_record, mock_record):
        self.mgr.cached_records.append(mock_record)
        # Call once and make sure record is removed from cache:
        with self.assertLogs():
            await self.mgr.remove_record(mock_record)
        mock__remove_record.assert_awaited_once_with(mock_record)
        self.assertNotIn(mock_record, self.mgr.cached_records)
        # Call again to make sure it passes removal ValueError is caught without consequences:
        mock__remove_record.reset_mock()
        await self.mgr.remove_record(mock_record)
        mock__remove_record.assert_awaited_once_with(mock_record)
        # Call with a record without id and make sure nothing is done:
        mock__remove_record.reset_mock()
        mock_record.id = None
        await self.mgr.remove_record(mock_record)
        mock__remove_record.assert_not_awaited()

    @async_test
    @patch.object(dns_api, 'DNSRecord')
    async def test__remove_record(self, mock_record):
        with self.assertRaises(NotImplementedError):
            await self.mgr._remove_record(mock_record)

    @async_test
    @patch.object(dns_api.DNSManager, '_create_record', new_callable=AsyncMock)
    async def test_create_a_record(self, mock__create_record):
        subdomain, ip, rec_type = 'test', IPv4Address('0.0.0.0'), 'A'
        await self.mgr.create_a_record(subdomain, ip)
        mock__create_record.assert_awaited_once_with(subdomain, ip, rec_type)

    @async_test
    @patch.object(dns_api.DNSManager, '_create_record', new_callable=AsyncMock)
    async def test_create_aaaa_record(self, mock__create_record):
        subdomain, ip, rec_type = 'test', IPv6Address('::'), 'AAAA'
        await self.mgr.create_aaaa_record(subdomain, ip)
        mock__create_record.assert_awaited_once_with(subdomain, ip, rec_type)

    @async_test
    @patch.object(dns_api, 'DNSRecord')
    @patch.object(dns_api.DNSManager, 'add_record', new_callable=AsyncMock)
    async def test__create_record(self, mock_add_record, mock_record):
        subdomain, ip, rec_type = 'test123', IPv4Address('0.0.0.0'), 'A'
        rec_name = f'{self.MOCK_DYN_NAME_PREFIX}{subdomain}.{self.mgr.domain}'
        mock_record.name = rec_name
        mock_record.type = rec_type
        mock_record.content = str(ip)
        self.mgr.cached_records.append(mock_record)
        # Check that the existing record is returned unchanged:
        output = await self.mgr._create_record(subdomain, ip, rec_type)
        self.assertEqual(output, mock_record)
        mock_add_record.assert_not_awaited()
        # Check that the existing record's content is updated:
        other_ip = IPv4Address('0.0.0.1')
        output = await self.mgr._create_record(subdomain, other_ip, rec_type)
        self.assertEqual(output, mock_record)
        self.assertEqual(mock_record.content, str(other_ip))
        mock_add_record.assert_awaited_once_with(mock_record)
        # Check that new record is created and `add_record` is called with it:
        mock_add_record.reset_mock()
        other_subdomain = 'test456'
        await self.mgr._create_record(other_subdomain, ip, rec_type)
        mock_add_record.assert_awaited_once()

    @async_test
    @patch.object(dns_api.DNSManager, 'remove_record', new_callable=AsyncMock)
    async def test_remove_all_records(self, mock_remove_record):
        subdomain, ip, rec_type = 'test123', IPv4Address('0.0.0.0'), 'A'
        rec_name = f'{self.MOCK_DYN_NAME_PREFIX}{subdomain}.{self.mgr.domain}'
        # Setup mock records, 1 and 3 to be removed, 2 to stay untouched:
        mock_rec1, mock_rec2, mock_rec3 = MagicMock(), MagicMock(), MagicMock()
        mock_rec2.name = 'something_different'
        mock_rec1.name = mock_rec3.name = rec_name
        mock_rec1.type = mock_rec2.type = mock_rec3.type = rec_type
        mock_rec1.content = mock_rec2.content = mock_rec3.content = str(ip)
        self.mgr.cached_records = [
            mock_rec1,
            mock_rec2,
            mock_rec3,
        ]
        expected_remove_calls = [
            call(mock_rec1),
            call(mock_rec3),
        ]
        await self.mgr.remove_all_records(subdomain)
        self.assertListEqual(mock_remove_record.await_args_list, expected_remove_calls)


class DNSManagerINWXTestCase(BaseTestCase):
    """
    All tests of methods that actually perform API calls will be skipped,
    unless the config file contains API credentials **and** a (base-)domain name.
    """
    CONFIG_SECTION = 'cloud-inwx'
    DYN_NAME_PREFIX = 'test-'

    # We need to load config here already, to allow testsuite to skip or continue depending on available arguments:
    load_basic_config_from(BaseTestCase.CONFIG_FILE_NAME)
    DOMAIN = settings.get_config('general', 'domain')
    USERNAME = settings.get_config(CONFIG_SECTION, 'username')
    PASSWORD = settings.get_config(CONFIG_SECTION, 'password')
    CRED_PROVIDED = bool(USERNAME and PASSWORD)

    SKIP_REASON_NO_CREDENTIALS = f"No INWX credentials provided in {BaseTestCase.CONFIG_FILE_NAME}."
    SKIP_REASON_NO_DOMAIN = f"No domain provided in {BaseTestCase.CONFIG_FILE_NAME}."

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()  # Reads config file into settings singleton
        cls.mock_auth = MagicMock()
        cls.mock_auth.username = cls.USERNAME
        cls.mock_auth.password = cls.PASSWORD

    def setUp(self) -> None:
        super().setUp()
        self.settings_patcher = patch(MANAGER_SETTINGS_PATH)
        self.mock_settings = self.settings_patcher.start()
        self.mock_settings.dynamic_node_name_prefix = self.DYN_NAME_PREFIX

        self.mgr = dns_api.DNSManagerINWX(self.DOMAIN, self.mock_auth)
        self.get_event_loop_patcher = patch(TESTED_MODULE_PATH + '.get_event_loop')
        self.mock_get_event_loop = self.get_event_loop_patcher.start()

    def tearDown(self) -> None:
        super().tearDown()
        self.settings_patcher.stop()
        self.get_event_loop_patcher.stop()

    @patch(TESTED_MODULE_PATH + '.INWX.ApiClient')
    def test_context_manager_session(self, mock_api_client_class):
        # Regular case, no exceptions during login or logout:
        with self.mgr.context_manager_session() as api:
            self.assertIsInstance(api, MagicMock)
            api.login.assert_called_once_with(self.mock_auth.username, self.mock_auth.password)
        mock_api_instance = mock_api_client_class.return_value
        # Exception during login:
        mock_api_instance.login = MagicMock(side_effect=Exception())
        with self.assertRaises(dns_api.DNSAPILoginError):
            with self.mgr.context_manager_session():
                pass
        mock_api_instance.login.side_effect = None
        # Exception during logout:
        mock_api_instance.logout = MagicMock(side_effect=Exception())
        with self.assertRaises(dns_api.DNSAPILogoutError):
            with self.mgr.context_manager_session():
                pass

    @async_test
    async def test__get_all_records(self):
        mock_records = [1, 2, 3]
        mock_run_in_executor = AsyncMock(return_value=mock_records)
        self.mock_get_event_loop.return_value.run_in_executor = mock_run_in_executor
        output = await self.mgr._get_all_records()
        self.assertListEqual(output, mock_records)
        mock_run_in_executor.assert_awaited_once_with(None, self.mgr._get_all_records_sync)

    @async_test
    async def test__add_record_update(self):
        mock_new_record = MagicMock()
        # Create mock existing records, the first one being different, the second having the same `name` and `type`
        # as the mocked new record:
        mock_ex_rec1, mock_ex_rec2 = MagicMock(), MagicMock()
        mock_ex_rec1.id, mock_ex_rec2.id = 1, 2
        mock_new_record.type = mock_ex_rec2.type = 'A'
        mock_new_record.name = mock_ex_rec2.name = 'test2'
        mock_ex_rec1.type, mock_ex_rec1.name = 'A', 'test1'
        mock_existing_records = [mock_ex_rec1, mock_ex_rec2]
        mock_run_in_executor = AsyncMock(return_value=mock_existing_records)
        self.mock_get_event_loop.return_value.run_in_executor = mock_run_in_executor
        with patch.object(dns_api.DNSManagerINWX, '_update_record', new_callable=AsyncMock) as mock__update_record:
            await self.mgr._add_record(mock_new_record)
            mock_run_in_executor.assert_awaited_once_with(None, self.mgr._get_all_records_sync)
            # Verify that the ID has been set to that of existing record 2:
            self.assertEqual(mock_new_record.id, mock_ex_rec2.id)
            mock__update_record.assert_awaited_once_with(mock_new_record)

    async def test_add_record_new(self):
        mock_new_record = MagicMock()
        mock_existing_records = []
        # Test that `_add_record_sync` is executed with new/different record:
        mock_run_in_executor = AsyncMock(return_value=mock_existing_records)
        self.mock_get_event_loop.return_value.run_in_executor = mock_run_in_executor
        mock_new_record.id = None
        mock_new_record.name = 'test_some_other_name'
        await self.mgr._add_record(mock_new_record)
        mock_run_in_executor.assert_awaited_with(None, self.mgr._add_record_sync, mock_new_record)

    @async_test
    async def test__update_record(self):
        mock_record = MagicMock()
        mock_run_in_executor = AsyncMock()
        self.mock_get_event_loop.return_value.run_in_executor = mock_run_in_executor
        output = await self.mgr._update_record(mock_record)
        self.assertIsNone(output)
        mock_run_in_executor.assert_awaited_once_with(None, self.mgr._update_record_sync, mock_record)

    @async_test
    async def test__remove_record(self):
        mock_record = MagicMock()
        mock_run_in_executor = AsyncMock()
        self.mock_get_event_loop.return_value.run_in_executor = mock_run_in_executor
        output = await self.mgr._remove_record(mock_record)
        self.assertIsNone(output)
        mock_run_in_executor.assert_awaited_once_with(None, self.mgr._remove_record_sync, mock_record)

    @unittest.skipUnless(bool(DOMAIN), SKIP_REASON_NO_DOMAIN)
    @unittest.skipUnless(CRED_PROVIDED, SKIP_REASON_NO_CREDENTIALS)
    def test_API_interaction(self):
        """
        Technically, not a unit test; more of a monolithic style test.
        Serves to verify that the API endpoints and the INWX package behave as expected.
        """
        logger.warning(f"Running API-bound test from {self.__class__.__name__}. "
                       f"An interruption may leave unwanted DNS records created during test.")
        # Retrieve existing records
        existing_records_start = self.mgr._get_all_records_sync()
        # Construct new test record
        subdomain_number = int(time())  # using current UNIX timestamp as subdomain part
        rec_name = f'{self.DYN_NAME_PREFIX}{subdomain_number}.{self.mgr.domain}'
        rec_type, rec_content = 'A', str(IPv4Address('0.0.0.0'))
        rec = dns_api.DNSRecord(rec_type, rec_name, rec_content)
        # Verify that it is not among already existing entries:
        for ex_rec in existing_records_start:
            self.assertNotEqual((ex_rec.name, ex_rec.type), (rec.name, rec.type))
        self.assertIsNone(rec.id)
        # Add it and verify that it has an ID now:
        self.mgr._add_record_sync(rec)
        rec_id = rec.id
        self.assertIsNotNone(rec_id)
        # Verify that it is now returned by the API as well:
        existing_records = self.mgr._get_all_records_sync()
        self.assertIn(rec, existing_records)
        # Change record IP and update:
        rec_new_content = str(IPv4Address('0.0.0.1'))
        rec.content = rec_new_content
        self.mgr._update_record_sync(rec)
        # Verify the change:
        existing_records = self.mgr._get_all_records_sync()
        for ex_rec in existing_records:
            if ex_rec.id == rec_id:
                self.assertEqual((ex_rec.name, ex_rec.type, ex_rec.content), (rec.name, rec.type, rec_new_content))
                break
        # Remove all records with the test prefix:
        for ex_rec in existing_records:
            if ex_rec.name.startswith(self.DYN_NAME_PREFIX):
                self.mgr._remove_record_sync(ex_rec)
                # Verify that record has no ID anymore:
                self.assertIsNone(ex_rec.id)
        # Verify that existing records' IDs are now the same as before:
        existing_records_end = self.mgr._get_all_records_sync()
        self.assertSetEqual({r.id for r in existing_records_start}, {r.id for r in existing_records_end})
        logger.info(f"Finished API-bound test.")


if __name__ == '__main__':
    unittest.main()
