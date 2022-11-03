from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, call, patch

from videbo import route_def


class RouteTableDefTestCase(IsolatedAsyncioTestCase):
    @patch.object(route_def.AIOHTTPRouteTableDef, "options")
    @patch.object(route_def.AIOHTTPRouteTableDef, "route")
    async def test_route_with_cors(
        self,
        mock_route: MagicMock,
        mock_options: MagicMock,
    ) -> None:
        # Test instance:
        obj = route_def.RouteTableDef()
        # Test arguments:
        test_path = "foo/bar"
        test_method1, test_method2 = "GET", "SPAM"
        test_allow_headers = ["spam", "eggs"]

        expected_headers = {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET,SPAM",
            "Access-Control-Max-Age": str(route_def.ACCESS_CONTROL_MAX_AGE),
            "Access-Control-Allow-Headers": "spam,eggs"
        }

        # Set up pseudo-route (coroutine function) to decorate:
        fake_headers = MagicMock()
        expected_response = MagicMock(headers=fake_headers)
        mock_handler = AsyncMock(return_value=expected_response)

        # Set up fake request object to pass to our pseudo-route:
        mock_request = AsyncMock(content_type="application/json")

        # Test top-level decorator:
        decorator = obj.route_with_cors(
            test_path,
            test_method1,
            test_method2,
            allow_headers=test_allow_headers,
        )
        self.assertTrue(callable(decorator))
        # Grab the OPTIONS route handler:
        options_handler = getattr(decorator, "__options_handler__")
        self.assertTrue(callable(options_handler))

        # Test inner decorator:
        wrapped_function = decorator(mock_handler)
        self.assertTrue(callable(wrapped_function))
        # Check that routes were created:
        mock_route.assert_has_calls([
            call(test_method1, test_path),
            call()(wrapped_function),
            call(test_method2, test_path),
            call()(wrapped_function),
        ])
        mock_options.assert_has_calls([
            call(test_path),
            call()(options_handler)
        ])

        # Test wrapper:
        output = await wrapped_function(mock_request)
        # Check that it returns exactly what our pseudo-function returns:
        self.assertIs(expected_response, output)
        # Check that our pseudo-handler was called as expected:
        mock_handler.assert_awaited_once_with(mock_request)
        # Check the expected headers were added to the response:
        fake_headers.extend.assert_called_once_with(expected_headers)

        # Test OPTIONS route handler:
        options_response = await options_handler(mock_request)
        self.assertIsInstance(options_response, route_def.Response)
        for name, value in expected_headers.items():
            self.assertEqual(value, options_response.headers[name])

        # Test that headers are added in case of error as well:
        mock_handler.side_effect = test_exc = route_def.HTTPException()
        with self.assertRaises(route_def.HTTPException) as context:
            await wrapped_function(mock_request)
            self.assertIs(test_exc, context.exception)
            for name, value in expected_headers.items():
                self.assertEqual(value, test_exc.headers[name])

    @patch.object(route_def.RouteTableDef, "route_with_cors")
    def test_get_with_cors(self, mock_route_with_cors: MagicMock) -> None:
        # Test instance:
        obj = route_def.RouteTableDef()
        # Test arguments:
        test_path = "foo/bar"
        test_allow_hdr = ["spam", "eggs"]

        output = obj.get_with_cors(test_path, allow_headers=test_allow_hdr)
        self.assertIs(mock_route_with_cors.return_value, output)
        mock_route_with_cors.assert_called_once_with(
            test_path,
            "GET",
            allow_headers=test_allow_hdr,
        )

    @patch.object(route_def.RouteTableDef, "route_with_cors")
    def test_post_with_cors(self, mock_route_with_cors: MagicMock) -> None:
        # Test instance:
        obj = route_def.RouteTableDef()
        # Test arguments:
        test_path = "foo/bar"
        test_allow_hdr = ["spam", "eggs"]

        output = obj.post_with_cors(test_path, allow_headers=test_allow_hdr)
        self.assertIs(mock_route_with_cors.return_value, output)
        mock_route_with_cors.assert_called_once_with(
            test_path,
            "POST",
            allow_headers=test_allow_hdr,
        )
