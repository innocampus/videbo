import logging
from time import time
from typing import Optional
from unittest.mock import AsyncMock, MagicMock, patch

from aiohttp.client import ClientSession, ClientTimeout
from aiohttp.test_utils import AioHTTPTestCase
from aiohttp.web import Application, Request, Response

from videbo import client


main_log = logging.getLogger('videbo')

VideboClient = client.Client


class ClientTestCase(AioHTTPTestCase):
    log_lvl: int

    @classmethod
    def setUpClass(cls) -> None:
        cls.log_lvl = main_log.level
        main_log.setLevel(logging.CRITICAL)

    @classmethod
    def tearDownClass(cls) -> None:
        main_log.setLevel(cls.log_lvl)

    async def get_application(self):
        self.root_resp = Response()
        self.pseudo_file = b"123abc"
        self.file_resp = Response(body=self.pseudo_file)
        self.last_root_request: Optional[Request] = None
        self.last_file_request: Optional[Request] = None

        async def root_handler(request: Request) -> Response:
            self.last_root_request = request
            return self.root_resp

        async def file_handler(request: Request) -> Response:
            self.last_file_request = request
            return self.file_resp

        app = Application()
        app.router.add_get('/', root_handler)
        app.router.add_get('/file/', file_handler)
        return app

    async def tearDownAsync(self) -> None:
        for instance in VideboClient._instances:
            await instance._session.close()
        VideboClient._instances.clear()

        self.last_root_request = None
        self.last_file_request = None
        await super().tearDownAsync()

    async def test___init__(self) -> None:
        test_timeout = ClientTimeout(12345)
        test_kwargs = {"timeout": test_timeout}
        obj = VideboClient(**test_kwargs)
        self.assertDictEqual(test_kwargs, obj._session_kwargs)
        self.assertIsInstance(obj._session, ClientSession)
        self.assertDictEqual({}, obj._jwt_cache)
        self.assertIn(obj, VideboClient._instances)
        self.assertEqual(test_timeout, obj._session.timeout)

    async def test_renew_session(self) -> None:
        test_timeout = ClientTimeout(12345)
        initial_kwargs = {"timeout": test_timeout, "auto_decompress": False}
        obj = VideboClient(**initial_kwargs)
        initial_session = obj._session
        self.assertFalse(initial_session.closed)
        kwargs_overrides = {"auto_decompress": True}
        combined_kwargs = initial_kwargs | kwargs_overrides
        await obj.renew_session(**kwargs_overrides)
        self.assertIsNot(initial_session, obj._session)
        self.assertTrue(initial_session.closed)
        self.assertFalse(obj._session.closed)
        self.assertDictEqual(combined_kwargs, obj._session_kwargs)
        self.assertEqual(test_timeout, obj._session.timeout)
        self.assertTrue(obj._session.auto_decompress)

    async def test_close(self) -> None:
        obj = VideboClient()
        self.assertIsNone(await obj.close())
        self.assertTrue(obj._session.closed)

    async def test___aenter__(self) -> None:
        obj = VideboClient()
        self.assertIs(obj, await obj.__aenter__())

    async def test___aexit__(self) -> None:
        obj = VideboClient()
        exc_type = Exception
        exc_val = Exception("foo")
        exc_tb = MagicMock()
        self.assertIsNone(await obj.__aexit__(exc_type, exc_val, exc_tb))
        self.assertTrue(obj._session.closed)

    async def test_close_all(self) -> None:
        close1, close2 = AsyncMock(), AsyncMock()
        VideboClient._instances = [
            MagicMock(close=close1),
            MagicMock(close=close2),
        ]
        try:
            self.assertIsNone(await VideboClient.close_all())
            close1.assert_awaited_once_with()
            close2.assert_awaited_once_with()
        finally:
            VideboClient._instances.clear()

    @patch.object(client, "RequestJWTData")
    async def test_get_jwt(self, mock_jwt_data_cls: MagicMock) -> None:
        obj = VideboClient()
        test_role, test_issuer = client.Role.lms, client.TokenIssuer.external
        mock_token, expiration_time = "foobar", time() + 100_000
        obj._jwt_cache[(test_role, test_issuer)] = (mock_token, expiration_time)
        output = obj.get_jwt(test_role, issuer=test_issuer)
        self.assertIs(mock_token, output)
        mock_jwt_data_cls.assert_not_called()

        # Cached token expired:
        obj._jwt_cache[(test_role, test_issuer)] = (mock_token, -1)
        mock_token = "baz"
        mock_encode = MagicMock(return_value=mock_token)
        mock_jwt_data_cls.return_value = MagicMock(encode=mock_encode)
        mock_current_time = 0
        token_expiration = mock_current_time + client._4HOURS
        with patch.object(client, "time", return_value=mock_current_time):
            output = obj.get_jwt(test_role, issuer=test_issuer)
        self.assertIs(mock_token, output)
        mock_jwt_data_cls.assert_called_once_with(
            exp=int(token_expiration),
            iss=test_issuer,
            role=test_role,
        )
        mock_encode.assert_called_once_with()
        self.assertDictEqual(
            {(test_role, test_issuer): (mock_token, token_expiration)},
            obj._jwt_cache,
        )

    @patch.object(VideboClient, "get_jwt")
    async def test_get_jwt_node(self, mock_get_jwt: MagicMock) -> None:
        output = VideboClient().get_jwt_node()
        self.assertIs(mock_get_jwt.return_value, output)
        mock_get_jwt.assert_called_once_with(client.Role.node)

    @patch.object(VideboClient, "get_jwt")
    async def test_get_jwt_admin(self, mock_get_jwt: MagicMock) -> None:
        output = VideboClient().get_jwt_admin()
        self.assertIs(mock_get_jwt.return_value, output)
        mock_get_jwt.assert_called_once_with(client.Role.admin)

    def test_update_auth_header(self) -> None:
        initial_headers = {"foo": "bar"}
        headers = initial_headers.copy()
        VideboClient.update_auth_header(headers, None)
        self.assertDictEqual(initial_headers, headers)

        jwt = "baz"
        VideboClient.update_auth_header(headers, jwt)
        self.assertDictEqual(
            initial_headers | {"Authorization": f"Bearer {jwt}"},
            headers,
        )

        headers = initial_headers.copy()
        jwt_data = client.RequestJWTData(
            exp=1,
            iss=client.TokenIssuer.external,
            role=client.Role.lms,
        )
        jwt = "spam"
        with patch.object(client.RequestJWTData, "encode", return_value=jwt):
            VideboClient.update_auth_header(headers, jwt_data, external=True)
        self.assertDictEqual(
            initial_headers | {"X-Authorization": f"Bearer {jwt}"},
            headers,
        )

    async def test_handle_response(self) -> None:
        mock_json = AsyncMock()
        mock_read = AsyncMock()
        mock_response = MagicMock(
            content_type="application/json",
            json=mock_json,
            read=mock_read,
        )
        output = await VideboClient.handle_response(mock_response, expect_json=True)
        self.assertIs(mock_json.return_value, output)
        mock_json.assert_awaited_once_with()
        mock_read.assert_not_called()

        mock_json.reset_mock()
        mock_response.content_type = "something else"

        with self.assertRaises(client.HTTPClientError):
            await VideboClient.handle_response(mock_response, expect_json=True)
        mock_json.assert_not_called()
        mock_read.assert_not_called()

        output = await VideboClient.handle_response(mock_response, expect_json=False)
        self.assertIs(mock_read.return_value, output)
        mock_json.assert_not_called()
        mock_read.assert_awaited_once_with()

    @patch.object(client.BaseResponseModel, "parse_obj")
    @patch.object(VideboClient, "handle_response")
    @patch.object(VideboClient, "update_auth_header")
    async def test_request___with_return_model_no_payload(
        self,
        mock_update_auth_header: MagicMock,
        mock_handle_response: AsyncMock,
        mock_parse_obj: MagicMock,
    ) -> None:
        class ResponseModel(client.BaseResponseModel):
            pass

        mock_handle_response.return_value = mock_handled_data = object()
        mock_parse_obj.return_value = mock_parsed_data = object()

        obj = VideboClient()

        mock_jwt, mock_data = MagicMock(), None
        return_model = ResponseModel
        test_external = True
        test_headers = {"spam": "eggs"}
        test_kwargs = {"headers": test_headers}

        output = await obj.request(
            "GET",
            self.server.make_url("/"),
            jwt=mock_jwt,
            data=mock_data,
            return_model=return_model,
            external=test_external,
            **test_kwargs,
        )
        self.assertFalse(self.last_root_request.can_read_body)
        self.assertIn("spam", self.last_root_request.headers)
        self.assertEqual("eggs", self.last_root_request.headers["spam"])
        self.assertNotIn("Content-Type", self.last_root_request.headers)
        self.assertTupleEqual((200, mock_parsed_data), output)
        mock_update_auth_header.assert_called_once_with(
            test_headers,
            mock_jwt,
            external=test_external,
        )
        mock_handle_response.assert_awaited_once()
        mock_parse_obj.assert_called_once_with(mock_handled_data)

    @patch.object(client.BaseResponseModel, "parse_obj")
    @patch.object(VideboClient, "handle_response")
    @patch.object(VideboClient, "update_auth_header")
    async def test_request___with_payload_no_return_model(
        self,
        mock_update_auth_header: MagicMock,
        mock_handle_response: AsyncMock,
        mock_parse_obj: MagicMock,
    ) -> None:
        mock_handle_response.return_value = mock_handled_data = object()

        obj = VideboClient()

        mock_jwt = MagicMock()
        return_model = None
        json_data = "blabla"
        mock_data = MagicMock(json=MagicMock(return_value=json_data))
        test_external = True
        test_headers = {"spam": "eggs"}
        test_kwargs = {"headers": test_headers}

        output = await obj.request(
            "GET",
            self.server.make_url("/"),
            jwt=mock_jwt,
            data=mock_data,
            return_model=return_model,
            external=test_external,
            **test_kwargs,
        )
        self.assertTrue(self.last_root_request.can_read_body)
        self.assertIn("spam", self.last_root_request.headers)
        self.assertEqual("eggs", self.last_root_request.headers["spam"])
        self.assertIn("Content-Type", self.last_root_request.headers)
        self.assertEqual("application/json", self.last_root_request.headers["Content-Type"])
        self.assertTupleEqual((200, mock_handled_data), output)
        mock_update_auth_header.assert_called_once_with(
            test_headers,
            mock_jwt,
            external=test_external,
        )
        mock_handle_response.assert_awaited_once()
        mock_parse_obj.assert_not_called()

    @patch.object(client.BaseResponseModel, "parse_obj")
    @patch.object(VideboClient, "handle_response")
    @patch.object(VideboClient, "update_auth_header")
    async def test_request___error(
        self,
        mock_update_auth_header: MagicMock,
        mock_handle_response: AsyncMock,
        mock_parse_obj: MagicMock,
    ) -> None:
        obj = VideboClient()
        mock_jwt = MagicMock()
        test_external = True
        test_headers = {"spam": "eggs"}
        with self.assertRaises(client.HTTPClientError):
            await obj.request(
                "GET",
                "bad_url",
                jwt=mock_jwt,
                external=test_external,
                headers=test_headers,
            )
        mock_update_auth_header.assert_called_once_with(
            test_headers,
            mock_jwt,
            external=test_external,
        )
        mock_handle_response.assert_not_called()
        mock_parse_obj.assert_not_called()

        mock_update_auth_header.reset_mock()

        # Without log:
        with self.assertRaises(client.HTTPClientError):
            await obj.request(
                "GET",
                "bad_url",
                jwt=mock_jwt,
                external=test_external,
                log_connection_error=False,
                headers=test_headers,
            )
        mock_update_auth_header.assert_called_once_with(
            test_headers,
            mock_jwt,
            external=test_external,
        )
        mock_handle_response.assert_not_called()
        mock_parse_obj.assert_not_called()

    @patch.object(VideboClient, "update_auth_header")
    async def test_request_file_read(
        self,
        mock_update_auth_header: MagicMock,
    ) -> None:
        obj = VideboClient()
        mock_jwt = MagicMock()
        test_headers = {"spam": "eggs"}
        test_kwargs = {"headers": test_headers, "timeout": 1}

        list_output = [
            chunk async for chunk in obj.request_file_read(
                self.server.make_url("/file/"),
                mock_jwt,
                chunk_size=1,
                **test_kwargs,
            )
        ]
        self.assertEqual(len(self.pseudo_file), len(list_output))
        self.assertEqual(self.pseudo_file, b"".join(list_output))
        mock_update_auth_header.assert_called_once_with(
            test_headers,
            mock_jwt,
        )
        self.assertIn("spam", self.last_file_request.headers)
        self.assertEqual("eggs", self.last_file_request.headers["spam"])

        mock_update_auth_header.reset_mock()
        new_pseudo_file = b"foobar" * 10
        self.file_resp = Response(body=new_pseudo_file)

        # With other chunk size:
        chunk_size = 2
        test_headers = {"foo": "bar"}
        test_kwargs = {"headers": test_headers}

        list_output = [
            chunk async for chunk in obj.request_file_read(
                self.server.make_url("/file/"),
                mock_jwt,
                chunk_size=chunk_size,
                **test_kwargs,
            )
        ]
        self.assertEqual(
            len(new_pseudo_file) // chunk_size,
            len(list_output),
        )
        self.assertEqual(new_pseudo_file, b"".join(list_output))
        mock_update_auth_header.assert_called_once_with(
            test_headers,
            mock_jwt,
        )
        self.assertIn("foo", self.last_file_request.headers)
        self.assertEqual("bar", self.last_file_request.headers["foo"])

        mock_update_auth_header.reset_mock()
        self.file_resp = Response(body=b"doesnt matter", status=400)

        # Error:
        with self.assertRaises(client.HTTPClientError):
            _ = [
                chunk async for chunk in obj.request_file_read(
                    self.server.make_url("/file/"),
                    mock_jwt,
                    chunk_size=chunk_size,
                    **test_kwargs,
                )
            ]
