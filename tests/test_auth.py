from typing import ClassVar, Optional
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

import jwt
from aiohttp.web_exceptions import HTTPBadRequest, HTTPUnauthorized, HTTPForbidden
from multidict import CIMultiDict
from pydantic import ValidationError

from videbo import auth
from videbo.exceptions import InvalidAuthData, NotAuthorized
from videbo.models import TokenIssuer


class AuthTestCase(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        super().setUp()
        # Settings mocking:
        self.settings_patcher = patch.object(auth, "settings")
        self.mock_settings = self.settings_patcher.start()

    def tearDown(self) -> None:
        super().tearDown()
        self.settings_patcher.stop()

    def test_extract_jwt_from_request(self) -> None:
        mock_jwt = 'abcde'

        # In Auth. header:
        request = MagicMock(headers=CIMultiDict({'foo': 'bar', 'Authorization': f'Bearer {mock_jwt}'}))
        output = auth.extract_jwt_from_request(request)
        self.assertEqual(mock_jwt, output)

        # Bad Auth. header:
        request.headers['Authorization'] = f'Foobar {mock_jwt}'
        with self.assertRaises(InvalidAuthData):
            auth.extract_jwt_from_request(request)

        # Not in header, but in query:
        del request.headers['Authorization']
        request.query = {'x': 'y', 'jwt': mock_jwt}
        output = auth.extract_jwt_from_request(request)
        self.assertEqual(mock_jwt, output)

        # Missing altogether:
        del request.query['jwt']
        with self.assertRaises(InvalidAuthData):
            auth.extract_jwt_from_request(request)

    def test_jwt_kid_internal(self) -> None:
        mock_secret = 'abc'

        # Missing key ID:
        token_headers: dict = {'x': 'y', 'ab': 'cd'}
        token = jwt.encode({'foo': 'bar'}, mock_secret, algorithm=auth.JWT_ALG, headers=token_headers)
        with self.assertRaises(InvalidAuthData):
            auth.jwt_kid_internal(token)

        # Invalid key ID:
        token_headers['kid'] = 'something wrong'
        token = jwt.encode({'foo': 'bar'}, mock_secret, algorithm=auth.JWT_ALG, headers=token_headers)
        with self.assertRaises(InvalidAuthData):
            auth.jwt_kid_internal(token)

        # Internal:
        token_headers['kid'] = TokenIssuer.internal.value
        token = jwt.encode({'foo': 'bar'}, mock_secret, algorithm=auth.JWT_ALG, headers=token_headers)
        self.assertTrue(auth.jwt_kid_internal(token))

        # External:
        token_headers['kid'] = TokenIssuer.external.value
        token = jwt.encode({'foo': 'bar'}, mock_secret, algorithm=auth.JWT_ALG, headers=token_headers)
        self.assertFalse(auth.jwt_kid_internal(token))

    @patch.object(auth, "Role")
    @patch.object(auth, "jwt_kid_internal")
    @patch.object(auth, "extract_jwt_from_request")
    def test_validate_jwt_data(
        self,
        mock_extract_jwt_from_request: MagicMock,
        mock_jwt_kid_internal: MagicMock,
        mock_role_cls: MagicMock,
    ) -> None:
        test_min_level = 42
        mock_data_obj = MagicMock(role=test_min_level)
        mock_decode = MagicMock()

        class MockJWTData(auth.RequestJWTData):
            error: ClassVar[Optional[BaseException]] = None

            @classmethod
            def decode(cls, *args, **kwargs) -> MagicMock:
                mock_decode(*args, **kwargs)
                if cls.error is None:
                    return mock_data_obj
                raise cls.error

        mock_request = MagicMock()

        ########################################################
        # Everything correct; key ID explicitly set to internal:
        mock_extract_jwt_from_request.return_value = mock_token = 'foo'
        mock_jwt_kid_internal.return_value = True
        mock_role_cls.node = test_min_level + 1

        # Test
        self.assertIs(
            mock_data_obj,
            auth.validate_jwt_data(mock_request, test_min_level, MockJWTData),
        )

        # Check expected function calls:
        mock_extract_jwt_from_request.assert_called_once_with(mock_request)
        mock_jwt_kid_internal.assert_called_once_with(mock_token)
        mock_decode.assert_called_once_with(mock_token, internal=True)

        mock_extract_jwt_from_request.reset_mock()
        mock_jwt_kid_internal.reset_mock()
        mock_decode.reset_mock()

        #####################################
        # JWT role below required min. level:
        mock_data_obj.role = test_min_level - 1

        # Test
        with self.assertRaises(NotAuthorized):
            auth.validate_jwt_data(mock_request, test_min_level, MockJWTData)

        # Check expected function calls:
        mock_extract_jwt_from_request.assert_called_once_with(mock_request)
        mock_jwt_kid_internal.assert_called_once_with(mock_token)
        mock_decode.assert_called_once_with(mock_token, internal=True)

        mock_extract_jwt_from_request.reset_mock()
        mock_jwt_kid_internal.reset_mock()
        mock_decode.reset_mock()

        ##############################
        # Invalid data encoded in JWT:
        MockJWTData.error = CustomValidationError()

        # Test
        with self.assertRaises(InvalidAuthData):
            auth.validate_jwt_data(mock_request, test_min_level, MockJWTData)

        # Check expected function calls:
        mock_extract_jwt_from_request.assert_called_once_with(mock_request)
        mock_jwt_kid_internal.assert_called_once_with(mock_token)
        mock_decode.assert_called_once_with(mock_token, internal=True)

        mock_extract_jwt_from_request.reset_mock()
        mock_jwt_kid_internal.reset_mock()
        mock_decode.reset_mock()

        #######################################
        # Invalid JWT; min. level at/above LMS:
        MockJWTData.error = jwt.InvalidTokenError
        mock_role_cls.lms = test_min_level

        # Test
        with self.assertRaises(jwt.InvalidTokenError):
            auth.validate_jwt_data(mock_request, test_min_level, MockJWTData)

        # Check expected function calls:
        mock_extract_jwt_from_request.assert_called_once_with(mock_request)
        mock_jwt_kid_internal.assert_called_once_with(mock_token)
        mock_decode.assert_called_once_with(mock_token, internal=True)

        mock_extract_jwt_from_request.reset_mock()
        mock_jwt_kid_internal.reset_mock()
        mock_decode.reset_mock()

        ####################################
        # Invalid JWT; min. level below LMS:
        mock_role_cls.lms = test_min_level + 1

        # Test
        with self.assertRaises(jwt.InvalidTokenError):
            auth.validate_jwt_data(mock_request, test_min_level, MockJWTData)

        # Check expected function calls:
        mock_extract_jwt_from_request.assert_called_once_with(mock_request)
        mock_jwt_kid_internal.assert_called_once_with(mock_token)
        mock_decode.assert_called_once_with(mock_token, internal=True)

    @patch.object(auth, "validate_jwt_data")
    @patch.object(auth, "get_route_model_param")
    @patch.object(auth, "Role")
    async def test_ensure_auth(
        self,
        mock_role_cls: MagicMock,
        mock_get_route_model_param: MagicMock,
        mock_validate_jwt_data: MagicMock,
    ) -> None:
        mock_role_cls.return_value = mock_role = 42
        mock_role_cls.admin = mock_role + 1
        mock_get_route_model_param.return_value = mock_param_name, mock_param_cls = 'foo', MagicMock()
        mock_validate_jwt_data.return_value = mock_jwt_data = object()

        # Set up pseudo-route (coroutine function) to decorate:
        expected_output = object()
        mock_function = AsyncMock(return_value=expected_output)

        # Set up fake request object and additional arguments to pass to our pseudo-route:
        mock_request = MagicMock()

        ################
        # Everything OK:
        min_level = mock_role
        hdr_key, hdr_val = 'bar', '123'
        mock_headers = {hdr_key: hdr_val}

        # Test top-level decorator:
        decorator = auth.ensure_auth(min_level, headers=mock_headers)
        self.assertTrue(callable(decorator))
        # Check the expected class init inside:
        mock_role_cls.assert_called_once_with(min_level)

        # Test inner decorator:
        wrapped_function = decorator(mock_function)
        self.assertTrue(callable(wrapped_function))
        # Check the expected function call inside:
        mock_get_route_model_param.assert_called_once_with(mock_function, auth.RequestJWTData)

        # Test wrapper:
        output = await wrapped_function(mock_request)
        # Check that it returns exactly what our pseudo-function returns:
        self.assertIs(expected_output, output)
        # Check that our pseudo-function was called with the same positional arguments and keyword-arguments
        # plus the JWT data parameter as identified by the mocked `get_route_model_param` function:
        mock_function.assert_awaited_once_with(mock_request, **{mock_param_name: mock_jwt_data})
        # Check the expected function call inside:
        mock_validate_jwt_data.assert_called_once_with(mock_request, min_level, mock_param_cls)

        mock_role_cls.reset_mock()
        mock_get_route_model_param.reset_mock()
        mock_validate_jwt_data.reset_mock()
        mock_function.reset_mock()

        ###########
        # HTTP 403:
        mock_role_cls.admin = mock_role
        self.mock_settings.forbid_admin_via_proxy = True
        mock_request.headers = {'X-Forwarded-For': 'foo'}

        # Test top-level decorator:
        decorator = auth.ensure_auth(min_level, headers=mock_headers)
        self.assertTrue(callable(decorator))
        # Check the expected class init inside:
        mock_role_cls.assert_called_once_with(min_level)

        # Test inner decorator:
        wrapped_function = decorator(mock_function)
        self.assertTrue(callable(wrapped_function))
        # Check the expected function call inside:
        mock_get_route_model_param.assert_called_once_with(mock_function, auth.RequestJWTData)

        # Test wrapper:
        with self.assertRaises(HTTPForbidden) as err403_context:
            await wrapped_function(mock_request)
        # Check that the specified headers are "transmitted":
        self.assertEqual(hdr_val, err403_context.exception.headers[hdr_key])
        # Check that our pseudo-function was never even called:
        mock_function.assert_not_called()
        # Check the expected function call inside:
        mock_validate_jwt_data.assert_called_once_with(mock_request, min_level, mock_param_cls)

        mock_role_cls.reset_mock()
        mock_get_route_model_param.reset_mock()
        mock_validate_jwt_data.reset_mock()

        ###########
        # HTTP 400:
        mock_validate_jwt_data.side_effect = InvalidAuthData

        # Test top-level decorator:
        decorator = auth.ensure_auth(min_level, headers=mock_headers)
        self.assertTrue(callable(decorator))
        # Check the expected class init inside:
        mock_role_cls.assert_called_once_with(min_level)

        # Test inner decorator:
        wrapped_function = decorator(mock_function)
        self.assertTrue(callable(wrapped_function))
        # Check the expected function call inside:
        mock_get_route_model_param.assert_called_once_with(mock_function, auth.RequestJWTData)

        # Test wrapper:
        with self.assertRaises(HTTPBadRequest) as err400_context:
            await wrapped_function(mock_request)
        # Check that the specified headers are "transmitted":
        self.assertEqual(hdr_val, err400_context.exception.headers[hdr_key])
        # Check that our pseudo-function was never even called:
        mock_function.assert_not_called()
        # Check the expected function call inside:
        mock_validate_jwt_data.assert_called_once_with(mock_request, min_level, mock_param_cls)

        mock_role_cls.reset_mock()
        mock_get_route_model_param.reset_mock()
        mock_validate_jwt_data.reset_mock()

        ###########
        # HTTP 401:
        mock_validate_jwt_data.side_effect = NotAuthorized

        # Test top-level decorator:
        decorator = auth.ensure_auth(min_level, headers=mock_headers)
        self.assertTrue(callable(decorator))
        # Check the expected class init inside:
        mock_role_cls.assert_called_once_with(min_level)

        # Test inner decorator:
        wrapped_function = decorator(mock_function)
        self.assertTrue(callable(wrapped_function))
        # Check the expected function call inside:
        mock_get_route_model_param.assert_called_once_with(mock_function, auth.RequestJWTData)

        # Test wrapper:
        with self.assertRaises(HTTPUnauthorized) as err401_context:
            await wrapped_function(mock_request)
        # Check that the specified headers are "transmitted":
        self.assertEqual(hdr_val, err401_context.exception.headers[hdr_key])
        # Check that our pseudo-function was never even called:
        mock_function.assert_not_called()
        # Check the expected function call inside:
        mock_validate_jwt_data.assert_called_once_with(mock_request, min_level, mock_param_cls)

        mock_role_cls.reset_mock()
        mock_get_route_model_param.reset_mock()
        mock_validate_jwt_data.reset_mock()


class MockJWTModel(MagicMock):

    def dict(self, **_kwargs) -> dict:
        """
        Simulates the behavior of the `BaseJWTData.dict` method.

        Returns any data that was used to initialize the mock object along with the string value of the `iss` field.
        """
        return {
            k: v for k, v in self.__dict__.items() if not k.startswith('_') and k != 'method_calls'
        } | {
            'iss': self.iss.value if hasattr(self.iss, 'value') else self.iss
        }

    def parse_obj(self, data: dict) -> 'MockJWTModel':
        """Mocks the Pydantic model's method with the same name."""
        return self.__class__(**data)


class CustomValidationError(ValidationError):
    def __init__(self) -> None:
        super().__init__([MagicMock()], MagicMock())  # type: ignore

    def __str__(self) -> str:
        return "foo"
