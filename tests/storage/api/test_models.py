from unittest import TestCase
from unittest.mock import MagicMock, call, patch

from pydantic import ValidationError

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

    @patch.object(models.RequestFileJWTData, "client_default", autospec=True)
    @patch.object(models.BaseJWTData, "default_expiration_from_now")
    def test_get_urls(
        self,
        mock_default_expiration_from_now: MagicMock,
        mock_client_default: MagicMock,
    ) -> None:
        mock_default_expiration_from_now.return_value = exp = 123
        expected_vid_url = "x"
        expected_thumb_url_1 = "y"
        expected_thumb_url_2 = "z"
        mock_encode_public_file_url = MagicMock(
            side_effect=(
                expected_vid_url,
                expected_thumb_url_1,
                expected_thumb_url_2,
            )
        )
        mock_jwt_obj = MagicMock(
            encode_public_file_url=mock_encode_public_file_url
        )
        mock_client_default.return_value = mock_jwt_obj

        file_hash = "foo"
        file_ext = ".bar"
        temp = MagicMock()
        thumb_count = 2
        output = models.RequestFileJWTData.get_urls(
            file_hash,
            file_ext,
            temp=temp,
            thumb_count=thumb_count,
        )
        self.assertEqual(expected_vid_url, output[0])
        self.assertEqual(expected_thumb_url_1, output[1][0])
        self.assertEqual(expected_thumb_url_2, output[1][1])
        mock_default_expiration_from_now.assert_called_once_with()
        self.assertListEqual(
            [
                call(
                    file_hash,
                    file_ext,
                    temp=temp,
                    expiration_time=exp,
                ),
                call(
                    file_hash,
                    file_ext,
                    temp=temp,
                    expiration_time=exp,
                    thumb_id=0,
                ),
                call(
                    file_hash,
                    file_ext,
                    temp=temp,
                    expiration_time=exp,
                    thumb_id=1,
                ),
            ],
            mock_client_default.call_args_list,
        )
        self.assertListEqual(
            [call()] * 3,
            mock_jwt_obj.encode_public_file_url.call_args_list,
        )


class MaxSizeMBTestCase(TestCase):
    def test_default_factory(self) -> None:
        obj = models.MaxSizeMB()
        self.assertEqual(models.settings.video.max_file_size_mb, obj.max_size)


class FileTooBigTestCase(TestCase):
    def test__log_response(self) -> None:
        mock_logger = MagicMock()
        obj = models.FileTooBig()
        self.assertIsNone(obj._log_response(mock_logger))
        mock_logger.warning.assert_called_once()


class InvalidFormatTestCase(TestCase):
    def test__log_response(self) -> None:
        mock_logger = MagicMock()
        obj = models.InvalidFormat()
        self.assertIsNone(obj._log_response(mock_logger))
        mock_logger.warning.assert_called_once()


class FileUploadedTestCase(TestCase):
    @patch.object(models.RequestFileJWTData, "get_urls")
    @patch.object(models, "FileUploadedResponseJWT")
    def test_from_video(
        self,
        mock_jwt_cls: MagicMock,
        mock_get_urls: MagicMock,
    ) -> None:
        mock_jwt_cls.return_value = mock_jwt_obj = MagicMock()
        mock_jwt_obj.encode.return_value = token = "abc"
        mock_jwt_cls.default_expiration_from_now.return_value = mock_exp = 123
        vid_url, thumb_urls = "wrong", ["wrong"]
        mock_get_urls.return_value = vid_url, thumb_urls

        file_hash, file_ext = "foo", ".bar"
        thumb_count, duration = 3, 3.14
        with self.assertRaises(ValidationError):
            models.FileUploaded.from_video(
                file_hash,
                file_ext,
                thumbnails_available=thumb_count,
                duration=duration,
            )
        mock_jwt_cls.assert_called_once_with(
            exp=mock_exp,
            iss=models.TokenIssuer.external,
            hash=file_hash,
            file_ext=file_ext,
            thumbnails_available=thumb_count,
            duration=duration,
        )
        mock_get_urls.assert_called_once_with(
            file_hash,
            file_ext,
            temp=True,
            thumb_count=thumb_count,
        )

        mock_jwt_cls.reset_mock()
        mock_get_urls.reset_mock()

        vid_url, thumb_urls = "http://localhost/foo", ["http://foo.bar/baz"]
        mock_get_urls.return_value = vid_url, thumb_urls
        output = models.FileUploaded.from_video(
            file_hash,
            file_ext,
            thumbnails_available=thumb_count,
            duration=duration,
        )
        self.assertIsInstance(output, models.FileUploaded)
        self.assertEqual("ok", output.result)
        self.assertEqual(token, output.jwt)
        self.assertEqual(vid_url, output.url)
        self.assertEqual(thumb_urls, output.thumbnails)


class FileDoesNotExistTestCase(TestCase):
    def test__log_response(self) -> None:
        mock_logger = MagicMock()
        obj = models.FileDoesNotExist(file_hash="foo")
        self.assertIsNone(obj._log_response(mock_logger))
        mock_logger.error.assert_called_once()
