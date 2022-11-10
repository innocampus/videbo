from unittest import TestCase
from unittest.mock import MagicMock, patch

from videbo.storage.api import models


class FileTypeTestCase(TestCase):

    def test_values(self) -> None:
        output = models.FileType.values()
        expected = frozenset(member.value for member in models.FileType.__members__.values())
        self.assertSetEqual(expected, output)


class RequestFileJWTDataTestCase(TestCase):

    @patch.object(models.BaseJWTData, "default_expiration_from_now")
    def test_client_default(self, mock_default_expiration_from_now: MagicMock) -> None:
        file_hash = "foo"
        file_ext = ".bar"
        expiration_time = 1234
        obj = models.RequestFileJWTData.client_default(
            file_hash,
            file_ext,
            temp=True,
            expiration_time=expiration_time,
        )
        self.assertEqual(
            models.RequestFileJWTData(
                exp=expiration_time,
                iss=models.TokenIssuer.external,
                role=models.Role.client,
                type=models.FileType.VIDEO_TEMP,
                hash=file_hash,
                file_ext=file_ext,
                rid="",
            ),
            obj,
        )
        mock_default_expiration_from_now.assert_not_called()

        mock_default_expiration_from_now.return_value = expiration_time

        thumb_id = 42
        obj = models.RequestFileJWTData.client_default(
            file_hash,
            file_ext,
            temp=False,
            thumb_id=thumb_id,
        )
        self.assertEqual(
            models.RequestFileJWTData(
                exp=expiration_time,
                iss=models.TokenIssuer.external,
                role=models.Role.client,
                type=models.FileType.THUMBNAIL,
                hash=file_hash,
                file_ext=file_ext,
                thumb_id=thumb_id,
                rid="",
            ),
            obj,
        )
        mock_default_expiration_from_now.assert_called_once_with()

    @patch.object(models.BaseJWTData, "encode")
    def test_encode_public_file_url(self, mock_encode: MagicMock) -> None:
        mock_encode.return_value = jwt_string = "foobarbaz"
        output = models.RequestFileJWTData(
            exp=123,
            iss=models.TokenIssuer.external,
            role=models.Role.client,
            type=models.FileType.THUMBNAIL,
            hash="a",
            file_ext="b",
            rid=""
        ).encode_public_file_url()
        self.assertEqual(
            f"{models.settings.public_base_url}/file?jwt={jwt_string}",
            output,
        )
        mock_encode.assert_called_once_with()


class StorageFileInfoTestCase(TestCase):

    def test___repr__(self) -> None:
        file_hash, file_ext = 'spam', '.eggs'
        obj = models.StorageFileInfo(hash=file_hash, file_ext=file_ext, file_size=12345)
        self.assertEqual(file_hash + file_ext, repr(obj))

    @patch.object(models.StorageFileInfo, '__repr__')
    def test___str__(self, mock_repr: MagicMock) -> None:
        mock_repr.return_value = string = 'foobar'
        obj = models.StorageFileInfo(hash='something', file_ext='.else', file_size=12345)
        self.assertEqual(string, str(obj))
