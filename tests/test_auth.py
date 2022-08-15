from unittest import TestCase
from unittest.mock import MagicMock, patch

import jwt
from multidict import CIMultiDict

from videbo import auth
from videbo.exceptions import InvalidAuthData
from videbo.models import TokenIssuer


TESTED_MODULE_PATH = 'videbo.auth'
SETTINGS_PATH = TESTED_MODULE_PATH + '.settings'


class AuthTestCase(TestCase):
    def setUp(self) -> None:
        super().setUp()
        # Settings mocking:
        self.settings_patcher = patch(SETTINGS_PATH)
        self.mock_settings = self.settings_patcher.start()
        self.internal_secret = 'secretA'
        self.external_secret = 'secretB'
        self.mock_settings.internal_api_secret = self.internal_secret
        self.mock_settings.external_api_secret = self.external_secret

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

        # In query:
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

    def test__get_jwt_params(self) -> None:
        output = auth._get_jwt_params(internal=True)
        self.assertTupleEqual((self.internal_secret, TokenIssuer.internal), output)
        output = auth._get_jwt_params(internal=False)
        self.assertTupleEqual((self.external_secret, TokenIssuer.external), output)

    @patch(TESTED_MODULE_PATH + '.BaseJWTData')
    @patch(TESTED_MODULE_PATH + '._get_jwt_params')
    def test_decode_jwt(self, mock__get_jwt_params: MagicMock, mock_base_jwt_data_cls) -> None:
        secret, issuer = 'secure', 'someone'
        mock__get_jwt_params.return_value = (secret, MagicMock(value=issuer))
        test_data = {'foo': 'bar', 'x': 1, 'iss': issuer}
        token = jwt.encode(test_data, secret, algorithm=auth.JWT_ALG)
        mock_internal_arg = MagicMock()

        # Mock model parsing that simply returns the input unchanged:
        mock_base_jwt_data_cls.parse_obj = lambda x: x

        output = auth.decode_jwt(token, model=mock_base_jwt_data_cls, internal=mock_internal_arg)
        self.assertEqual(test_data, output)
        mock__get_jwt_params.assert_called_once_with(internal=mock_internal_arg)
        mock__get_jwt_params.reset_mock()

    @patch(TESTED_MODULE_PATH + '.time', return_value=0)
    @patch(TESTED_MODULE_PATH + '._get_jwt_params')
    def test_encode_jwt(self, mock__get_jwt_params: MagicMock, mock_time: MagicMock) -> None:
        secret, issuer = 'secure', 'someone'
        issuer_enum = MagicMock(value=issuer)
        mock__get_jwt_params.return_value = (secret, issuer_enum)
        mock_data = MockModel()
        expiry = 1234
        # TODO: incomplete
        auth.encode_jwt(mock_data, expiry=expiry)
        self.assertEqual(issuer_enum, mock_data.iss)
        self.assertEqual(mock_time() + expiry, mock_data.exp)
        mock_data.parse_obj.assert_called_once_with(mock_data.dict())


class MockModel(MagicMock):

    def dict(self, **_kwargs):
        return {'iss': 'foo', 'exp': 'bar'}
