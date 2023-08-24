import logging
from pathlib import Path
from typing import cast
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from pydantic import BaseModel, ValidationError

from tests.silent_log import SilentLogMixin
from videbo import web


class WebTestCase(SilentLogMixin, IsolatedAsyncioTestCase):
    def test_get_application(self) -> None:
        mock_ctx1, mock_ctx2 = AsyncMock(), AsyncMock()
        mock_logger = MagicMock()
        app = web.get_application(
            cleanup_contexts=(mock_ctx1, mock_ctx2),
            logger=mock_logger,
        )
        self.assertIn(mock_ctx1, app.cleanup_ctx)
        self.assertIn(mock_ctx2, app.cleanup_ctx)
        self.assertIs(mock_logger, app.logger)
        self.assertEqual(web.REQUEST_BODY_MAX_SIZE, app._client_max_size)

    @patch.object(web, "run_app")
    @patch.object(web, "get_application")
    def test_start_web_server(
        self,
        mock_get_application: MagicMock,
        mock_run_app: MagicMock,
    ) -> None:
        mock_get_application.return_value = mock_app = MagicMock()

        mock_routes = MagicMock()
        address = "foo"
        port = 123
        mock_contexts = MagicMock()
        mock_logger = MagicMock()
        verbose = True
        app_kwargs = {"spam": "eggs"}
        self.assertIsNone(web.start_web_server(
            mock_routes,
            address,
            port,
            cleanup_contexts=mock_contexts,
            access_logger=mock_logger,
            verbose=verbose,
            **app_kwargs,
        ))
        mock_get_application.assert_called_once_with(
            cleanup_contexts=mock_contexts,
            **app_kwargs,
        )
        mock_app.add_routes.assert_called_once_with(mock_routes)
        mock_logger.setLevel.assert_not_called()
        mock_run_app.assert_called_once_with(
            mock_app,
            host=address,
            port=port,
            access_log=mock_logger,
        )

        mock_get_application.reset_mock()
        mock_app.add_routes.reset_mock()
        mock_run_app.reset_mock()

        #####################################
        # Same, but with log down-leveling: #

        verbose = False
        self.assertIsNone(web.start_web_server(
            mock_routes,
            address,
            port,
            cleanup_contexts=mock_contexts,
            access_logger=mock_logger,
            verbose=verbose,
            **app_kwargs,
        ))
        mock_get_application.assert_called_once_with(
            cleanup_contexts=mock_contexts,
            **app_kwargs,
        )
        mock_app.add_routes.assert_called_once_with(mock_routes)
        mock_logger.setLevel.assert_called_once_with(logging.ERROR)
        mock_run_app.assert_called_once_with(
            mock_app,
            host=address,
            port=port,
            access_log=mock_logger,
        )

    @patch.object(web, "get_route_model_param")
    async def test_ensure_json_body(
        self,
        mock_get_route_model_param: MagicMock,
    ) -> None:
        validated_payload = object()
        mock_param_name = "foo"
        mock_param_cls = MagicMock(parse_obj=MagicMock(return_value=validated_payload))
        mock_get_route_model_param.return_value = mock_param_name, mock_param_cls

        # Set up pseudo-route (coroutine function) to decorate:
        expected_output = object()
        mock_function = AsyncMock(return_value=expected_output)

        # Set up fake request object and args to pass to our pseudo-route:
        mock_request = AsyncMock(content_type="application/json")
        args, kwargs = (420, ), {"spam": "eggs"}

        ####################################
        # Simulate WITH-parentheses usage: #

        hdr_key, hdr_val = "bar", "123"
        mock_headers = {hdr_key: hdr_val}

        # Test top-level decorator:
        decorator = web.ensure_json_body(headers=mock_headers)
        self.assertTrue(callable(decorator))

        # Test inner decorator:
        wrapped_function = decorator(mock_function)
        self.assertTrue(callable(wrapped_function))
        # Check the expected function call inside:
        mock_get_route_model_param.assert_called_once_with(
            mock_function,
            web.BaseRequestModel,
        )

        # Test wrapper:
        output = await wrapped_function(mock_request, *args, **kwargs)
        # Check that it returns exactly what our pseudo-function returns:
        self.assertIs(expected_output, output)
        # Check that our pseudo-function was called with the same positional arguments and keyword-arguments
        # plus the data parameter as identified by the mocked `get_route_model_param` function:
        mock_function.assert_awaited_once_with(
            mock_request,
            *args,
            **kwargs,
            **{mock_param_name: validated_payload},
        )
        # Check the expected function call inside:
        mock_request.json.assert_awaited_once_with()
        mock_param_cls.parse_obj.assert_called_once_with(mock_request.json.return_value)

        mock_get_route_model_param.reset_mock()
        mock_function.reset_mock()
        mock_request.json.reset_mock()
        mock_param_cls.parse_obj.reset_mock()

        #######################
        # Wrong content type: #

        mock_request.content_type = "wrong"

        with self.assertRaises(web.HTTPBadRequest) as ctx:
            await wrapped_function(mock_request, *args, **kwargs)
            self.assertEqual(hdr_val, ctx.exception.headers[hdr_key])
        mock_request.json.assert_not_called()
        mock_param_cls.parse_obj.assert_not_called()
        mock_function.assert_not_called()

        #################
        # Invalid JSON: #

        mock_request.json.side_effect = web.JSONDecodeError("foo", "bar", 1)
        mock_request.content_type = "application/json"

        with self.assertRaises(web.HTTPBadRequest) as ctx:
            await wrapped_function(mock_request, *args, **kwargs)
            self.assertEqual(hdr_val, ctx.exception.headers[hdr_key])
        mock_request.json.assert_awaited_once_with()
        mock_param_cls.parse_obj.assert_not_called()
        mock_function.assert_not_called()

        mock_request.json.reset_mock()
        mock_request.json.side_effect = None

        #################
        # Invalid data: #

        mock_param_cls.parse_obj.side_effect = CustomValidationError

        with self.assertRaises(web.HTTPBadRequest) as ctx:
            await wrapped_function(mock_request, *args, **kwargs)
            self.assertEqual(hdr_val, ctx.exception.headers[hdr_key])
        mock_request.json.assert_awaited_once_with()
        mock_param_cls.parse_obj.assert_called_once_with(mock_request.json.return_value)
        mock_function.assert_not_called()

        mock_param_cls.parse_obj.reset_mock()
        mock_request.json.reset_mock()

        mock_param_cls.parse_obj.side_effect = None

        ##################################
        # Simulate NO-parentheses usage: #

        # Test top-level decorator AND inner decorator:
        wrapped_function = web.ensure_json_body(mock_function)
        self.assertTrue(callable(decorator))
        # Check the expected function call inside:
        mock_get_route_model_param.assert_called_once_with(
            mock_function,
            web.BaseRequestModel,
        )

        # Test wrapper:
        output = await wrapped_function(mock_request, *args, **kwargs)
        # Check that it returns exactly what our pseudo-function returns:
        self.assertIs(expected_output, output)

    @patch.object(web, "sanitize_filename")
    def test_file_serve_headers(self, mock_sanitize: MagicMock) -> None:
        mock_sanitize.return_value = "foo bar"
        download_filename = "foobar"
        expected_output = {
            "Cache-Control": f"private, max-age={web.CACHE_CONTROL_MAX_AGE}",
            "Content-Disposition": 'attachment; filename="foo%20bar"'
        }
        output = web.file_serve_headers(download_filename)
        self.assertDictEqual(expected_output, output)

        expected_output = {
            "Cache-Control": f"private, max-age={web.CACHE_CONTROL_MAX_AGE}",
        }
        output = web.file_serve_headers()
        self.assertDictEqual(expected_output, output)

    def test_x_accel_headers(self) -> None:
        redirect_uri = "foo"
        limit_rate_bytes = 123
        expected_output = {
            "X-Accel-Redirect": redirect_uri,
            "X-Accel-Limit-Rate": str(limit_rate_bytes),
        }
        output = web.x_accel_headers(redirect_uri, limit_rate_bytes)
        self.assertDictEqual(expected_output, output)

        expected_output = {
            "X-Accel-Redirect": redirect_uri,
        }
        output = web.x_accel_headers(redirect_uri)
        self.assertDictEqual(expected_output, output)

    @patch.object(web, "mime_type_from_file_name")
    @patch.object(web, "x_accel_headers")
    @patch.object(web, "file_serve_headers")
    def test_serve_file_via_x_accel(
        self,
        mock_file_serve_headers: MagicMock,
        mock_x_accel_headers: MagicMock,
        mock_mime_type_from_file_name: MagicMock,
    ) -> None:
        hdr1_key, hdr1_val = "spam", "eggs"
        hdr2_key, hdr2_val = "foo", "bar"
        mock_file_serve_headers.return_value = {hdr1_key: hdr1_val}
        mock_x_accel_headers.return_value = {hdr2_key: hdr2_val}
        mock_mime_type_from_file_name.return_value = content = "video/mp4"

        redirect_uri = Path("abc/def/file.tar.gz")
        limit_rate_bytes = 12345
        download_filename = "something"

        output = web.serve_file_via_x_accel(
            redirect_uri,
            limit_rate_bytes=limit_rate_bytes,
            download_filename=download_filename,
        )
        # Check that all expected headers are present:
        self.assertEqual(hdr1_val, output.headers[hdr1_key])
        self.assertEqual(hdr2_val, output.headers[hdr2_key])
        # Check that content type was set:
        self.assertEqual(content, output.content_type)
        # Check for expected function calls:
        mock_file_serve_headers.assert_called_once_with(download_filename)
        mock_x_accel_headers.assert_called_once_with(
            str(redirect_uri),
            limit_rate_bytes,
        )
        mock_mime_type_from_file_name.assert_called_once_with(redirect_uri)


class CustomValidationError(ValidationError):
    def __init__(self) -> None:
        super().__init__([MagicMock()], cast(type[BaseModel], MagicMock()))

    def __str__(self) -> str:
        return "foo"
