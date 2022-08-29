from time import time
from unittest import TestCase
from unittest.mock import patch

import jwt
from pydantic import ValidationError

from videbo import models


TESTED_MODULE_PATH = 'videbo.models'
SETTINGS_PATH = TESTED_MODULE_PATH + '.storage_settings'


class BaseJWTDataTestCase(TestCase):
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

    def test_iss_is_enum_member(self) -> None:
        kwargs: dict = {'exp': 1, 'iss': models.TokenIssuer.internal}
        obj = models.BaseJWTData(**kwargs)
        self.assertIs(models.TokenIssuer.internal, obj.iss)

        kwargs['iss'] = 'int'
        obj = models.BaseJWTData(**kwargs)
        self.assertIs(models.TokenIssuer.internal, obj.iss)

        kwargs['iss'] = 'internal'
        obj = models.BaseJWTData(**kwargs)
        self.assertIs(models.TokenIssuer.internal, obj.iss)

        kwargs['iss'] = 'foo'
        with self.assertRaises(ValidationError):
            models.BaseJWTData(**kwargs)

        kwargs['iss'] = 123
        with self.assertRaises(ValidationError):
            models.BaseJWTData(**kwargs)

    def test_dict(self) -> None:
        obj = models.BaseJWTData(exp=1, iss=models.TokenIssuer.external)
        output = obj.dict()
        self.assertEqual('ext', output['iss'])
        self.assertNotIsInstance(output['iss'], models.TokenIssuer)

    def test_encode(self) -> None:
        exp = int(time()) + 300
        iss = models.TokenIssuer.internal
        data = {'exp': exp, 'iss': iss.value}
        obj = models.BaseJWTData(exp=exp, iss=iss)
        token = obj.encode()
        expected = jwt.encode(data, self.internal_secret, algorithm=models.DEFAULT_JWT_ALG, headers={'kid': iss.value})
        self.assertEqual(expected, token)
        decoded = jwt.decode(token, self.internal_secret, algorithms=[models.DEFAULT_JWT_ALG], issuer=iss.value)
        self.assertDictEqual(data, decoded)

        iss = models.TokenIssuer.external
        data = {'exp': exp, 'iss': iss.value}
        obj = models.BaseJWTData(exp=exp, iss=iss)
        token = obj.encode()
        expected = jwt.encode(data, self.external_secret, algorithm=models.DEFAULT_JWT_ALG, headers={'kid': iss.value})
        self.assertEqual(expected, token)
        decoded = jwt.decode(token, self.external_secret, algorithms=[models.DEFAULT_JWT_ALG], issuer=iss.value)
        self.assertDictEqual(data, decoded)

    def test_decode(self) -> None:
        exp = int(time()) + 300
        iss = models.TokenIssuer.internal
        internal = True
        data = {'exp': exp, 'iss': iss.value}
        token = jwt.encode(data, self.internal_secret, algorithm=models.DEFAULT_JWT_ALG, headers={'kid': iss.value})
        obj = models.BaseJWTData.decode(token, internal=internal)
        self.assertIsInstance(obj, models.BaseJWTData)
        self.assertDictEqual(data, obj.dict())

        iss = models.TokenIssuer.external
        internal = False
        data = {'exp': exp, 'iss': iss.value}
        token = jwt.encode(data, self.external_secret, algorithm=models.DEFAULT_JWT_ALG, headers={'kid': iss.value})
        obj = models.BaseJWTData.decode(token, internal=internal)
        self.assertIsInstance(obj, models.BaseJWTData)
        self.assertDictEqual(data, obj.dict())


class RequestJWTDataTestCase(TestCase):

    def test_role_is_enum_member(self) -> None:
        kwargs: dict = {'exp': 1, 'iss': models.TokenIssuer.internal, 'role': models.Role.node}
        obj = models.RequestJWTData(**kwargs)
        self.assertIs(models.Role.node, obj.role)

        kwargs['role'] = models.Role.node.value
        obj = models.RequestJWTData(**kwargs)
        self.assertIs(models.Role.node, obj.role)

        kwargs['role'] = models.Role.node.name
        obj = models.RequestJWTData(**kwargs)
        self.assertIs(models.Role.node, obj.role)

        kwargs['role'] = 'spam'
        with self.assertRaises(ValidationError):
            models.RequestJWTData(**kwargs)

        kwargs['role'] = ('spam', 'eggs')
        with self.assertRaises(ValidationError):
            models.RequestJWTData(**kwargs)

    def test_role_appropriate_for_external(self) -> None:
        # Should not cause a problem:
        models.RequestJWTData(exp=1, iss=models.TokenIssuer.external, role=models.Role.lms)

        models.RequestJWTData(exp=1, iss=models.TokenIssuer.internal, role=models.Role.node)
        models.RequestJWTData(exp=1, iss=models.TokenIssuer.internal, role=models.Role.admin)

        with self.assertRaises(ValidationError):
            models.RequestJWTData(exp=1, iss=models.TokenIssuer.external, role=models.Role.node)

        with self.assertRaises(ValidationError):
            models.RequestJWTData(exp=1, iss=models.TokenIssuer.external, role=models.Role.node)
