import logging
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, call, create_autospec, patch

from aiohttp.multipart import BodyPartReader
from aiohttp.web_request import Request

from videbo.storage import http_util
from ..silent_log import SilentLogMixin


MAX_FILE_SIZE = http_util.settings.video.max_file_size_bytes


class HTTPUtilTestCase(SilentLogMixin, IsolatedAsyncioTestCase):
    @patch.object(http_util, "is_allowed_file_ending")
    @patch.object(http_util, "isinstance")
    async def test_get_video_payload(
            self,
            mock_isinstance: MagicMock,
            mock_is_allowed_file_ending: MagicMock,
    ) -> None:
        # Missing `video` field:
        mock_request = create_autospec(Request, instance=True)
        mock_request.multipart.return_value.next.return_value = None
        mock_isinstance.return_value = False
        with self.assertRaises(http_util.FormFieldMissing):
            await http_util.get_video_payload(mock_request)
        mock_isinstance.assert_called_once_with(None, BodyPartReader)
        mock_is_allowed_file_ending.assert_not_called()

        mock_isinstance.reset_mock()
        ######################################################################

        # Bad file extension
        mock_is_allowed_file_ending.return_value = False
        mock_isinstance.return_value = True
        mock_video_field = MagicMock()
        mock_video_field.name = "video"
        mock_video_field.filename = filename = None
        other_field = MagicMock()
        mock_fields = (other_field, mock_video_field)
        mock_request.multipart.return_value.next.side_effect = mock_fields
        expected_isinstance_calls = [
            call(other_field, BodyPartReader),
            call(mock_video_field, BodyPartReader),
        ]
        with self.assertRaises(http_util.BadFileExtension):
            await http_util.get_video_payload(mock_request)
        self.assertListEqual(
            expected_isinstance_calls,
            mock_isinstance.call_args_list,
        )
        mock_is_allowed_file_ending.assert_called_once_with(filename)

        mock_isinstance.reset_mock()
        mock_is_allowed_file_ending.reset_mock()
        mock_request.multipart.return_value.next.side_effect = mock_fields
        ######################################################################

        # File too big:
        mock_is_allowed_file_ending.return_value = True
        mock_request.content_length = MAX_FILE_SIZE + 1
        with self.assertRaises(http_util.FileTooBigError):
            await http_util.get_video_payload(mock_request)
        self.assertListEqual(
            expected_isinstance_calls,
            mock_isinstance.call_args_list,
        )
        mock_is_allowed_file_ending.assert_called_once_with(filename)

        mock_isinstance.reset_mock()
        mock_is_allowed_file_ending.reset_mock()
        mock_request.multipart.return_value.next.side_effect = mock_fields
        ######################################################################

        # Everything OK:
        mock_request.content_length = MAX_FILE_SIZE
        output = await http_util.get_video_payload(mock_request)
        self.assertIs(mock_video_field, output)
        self.assertListEqual(
            expected_isinstance_calls,
            mock_isinstance.call_args_list,
        )
        mock_is_allowed_file_ending.assert_called_once_with(filename)

    async def test_read_data(self) -> None:
        mock_data_chunks = (b"x", b"y", b"")
        mock_field = create_autospec(BodyPartReader, instance=True)
        mock_field.read_chunk.side_effect = mock_data_chunks
        mock_temp_file = create_autospec(http_util.TempFile, instance=True)
        mock_temp_file.size = MAX_FILE_SIZE
        chunk_size = 123
        with self.assertRaises(http_util.FileTooBigError):
            await http_util.read_data(mock_temp_file, mock_field, chunk_size)
        mock_field.read_chunk.assert_awaited_once_with(chunk_size)
        mock_temp_file.write.assert_not_called()

        mock_field.read_chunk.reset_mock()
        mock_field.read_chunk.side_effect = mock_data_chunks

        # The two data chunks should barely fit:
        mock_temp_file.size = MAX_FILE_SIZE - 2
        await http_util.read_data(mock_temp_file, mock_field, chunk_size)
        self.assertListEqual(
            [call(chunk_size)] * 3,
            mock_field.read_chunk.await_args_list,
        )
        self.assertListEqual(
            [call(b"x"), call(b"y")],
            mock_temp_file.write.await_args_list,
        )

    @patch.object(http_util, "get_video_info")
    @patch.object(http_util, "read_data")
    async def test_save_temp_video(
            self,
            mock_read_data: AsyncMock,
            mock_get_video_info: AsyncMock,
    ) -> None:
        # Mock `VideoInfo`:
        mock_get_video_info.return_value = mock_video_info = MagicMock()
        mock_video_info.get_consistent_file_ext.return_value = ext = ".foo"
        mock_video_info.get_duration.return_value = duration = 3.14
        # Mock `TempFile`:
        mock_file = create_autospec(http_util.TempFile, instance=True)
        mock_file.open.return_value = mock_file
        # Rest of the arguments:
        mock_form_field = MagicMock()
        mock_logger = MagicMock()

        output = await http_util.save_temp_video(
            mock_file,
            mock_form_field,
            log=mock_logger,
        )
        self.assertEqual(round(duration, 1), output)
        mock_file.open.assert_called_once_with()
        mock_file.__aenter__.assert_called_once_with()
        mock_read_data.assert_awaited_once_with(
            mock_file,
            mock_form_field,
            log=mock_logger,
        )
        mock_file.__aexit__.assert_called_once_with(None, None, None)
        mock_get_video_info.assert_awaited_once_with(
            mock_file.path,
            log=mock_logger,
        )
        mock_file.persist.assert_awaited_once_with(file_ext=ext)

    @patch.object(http_util.FileUploaded, "from_video")
    @patch.object(http_util.StorageFileController, "get_instance")
    @patch.object(http_util, "generate_thumbnails")
    @patch.object(http_util, "InvalidFormat")
    @patch.object(http_util, "FileTooBig")
    @patch.object(http_util, "save_temp_video")
    async def test_save_temp_and_get_response(
            self,
            mock_save_temp_video: AsyncMock,
            mock_file_too_big_cls: MagicMock,
            mock_invalid_format_cls: MagicMock,
            mock_generate_thumbnails: AsyncMock,
            mock_get_storage_instance: MagicMock,
            mock_get_response_data_from_video: MagicMock,
    ) -> None:
        # Mock `FileUploaded`:
        response = object()
        mock_data = MagicMock(json_response=MagicMock(return_value=response))
        mock_get_response_data_from_video.return_value = mock_data
        # Mock `TempFile`:
        mock_file = create_autospec(http_util.TempFile, instance=True)
        mock_file.open.return_value = mock_file
        # Rest of the arguments:
        mock_form_field = MagicMock()
        mock_logger = MagicMock()

        duration = 3.14
        # Test errors first:
        mock_save_temp_video.side_effect = (
            http_util.FileTooBigError,
            http_util.FFProbeError,
            http_util.VideoNotAllowed,
            duration,
        )

        # File too big:
        output = await http_util.save_temp_and_get_response(
            mock_file,
            mock_form_field,
            log=mock_logger,
        )
        self.assertIs(mock_file_too_big_cls().json_response(), output)
        mock_save_temp_video.assert_awaited_once_with(
            mock_file,
            mock_form_field,
            mock_logger,
        )
        mock_file.delete.assert_awaited_once_with()

        mock_save_temp_video.reset_mock()
        mock_file.delete.reset_mock()

        # Probe error:
        output = await http_util.save_temp_and_get_response(
            mock_file,
            mock_form_field,
            log=mock_logger,
        )
        self.assertIs(mock_invalid_format_cls().json_response(), output)
        mock_save_temp_video.assert_awaited_once_with(
            mock_file,
            mock_form_field,
            mock_logger,
        )
        mock_file.delete.assert_awaited_once_with()

        mock_save_temp_video.reset_mock()
        mock_file.delete.reset_mock()

        # Invalid format:
        output = await http_util.save_temp_and_get_response(
            mock_file,
            mock_form_field,
            log=mock_logger,
        )
        self.assertIs(mock_invalid_format_cls().json_response(), output)
        mock_file.delete.assert_awaited_once_with()

        mock_save_temp_video.reset_mock()
        mock_file.delete.reset_mock()

        mock_generate_thumbnails.assert_not_called()
        mock_get_storage_instance.assert_not_called()
        mock_get_response_data_from_video.assert_not_called()

        # Success:
        output = await http_util.save_temp_and_get_response(
            mock_file,
            mock_form_field,
            log=mock_logger,
        )
        self.assertIs(response, output)
        mock_file.delete.assert_not_called()
        mock_generate_thumbnails.assert_awaited_once_with(
            mock_file.path,
            duration,
            interim_dir=mock_get_storage_instance.return_value.temp_out_dir,
        )
        mock_get_response_data_from_video.assert_called_once_with(
            mock_file.digest,
            mock_file.path.suffix,
            thumbnails_available=mock_generate_thumbnails.return_value,
            duration=duration,
        )

    async def test_video_check_redirect(self) -> None:
        pass

    @patch.object(http_util, "run_in_default_executor")
    async def test_verify_file_exists(
            self,
            mock_run_in_default_executor: AsyncMock,
    ) -> None:
        mock_run_in_default_executor.return_value = True
        path = MagicMock()
        self.assertIsNone(await http_util.verify_file_exists(path))
        mock_run_in_default_executor.assert_awaited_once_with(path.is_file)
        mock_run_in_default_executor.reset_mock()

        mock_run_in_default_executor.return_value = False
        with self.assertRaises(http_util.HTTPNotFound):
            with self.assertLogs(http_util._log, logging.WARNING):
                await http_util.verify_file_exists(path)
        mock_run_in_default_executor.assert_awaited_once_with(path.is_file)

    def test_ensure_acceptable_load(self) -> None:
        # Should just pass:
        http_util.ensure_acceptable_load(0.89999, 0.9)

        with self.assertLogs(http_util._log, logging.WARNING):
            with self.assertRaises(http_util.HTTPServiceUnavailable):
                http_util.ensure_acceptable_load(0.90001, 0.9)

    @patch.object(http_util, "RedirectToDistributor")
    @patch.object(http_util, "ensure_acceptable_load")
    @patch.object(http_util.StorageFileController, "get_instance")
    @patch.object(http_util.NetworkInterfaces, "get_instance")
    async def test_handle_video_request(
        self,
        mock_get_ni_instance: MagicMock,
        mock_get_storage_instance: MagicMock,
        mock_ensure_acceptable_load: MagicMock,
        mock_redirect_exception_cls: MagicMock,
    ) -> None:
        # Set the load to the threshold value:
        load = http_util.settings.distribution.load_threshold_delayed_redirect
        mock_network_interfaces = MagicMock(
            get_tx_load=MagicMock(return_value=load)
        )
        mock_get_ni_instance.return_value = mock_network_interfaces
        mock_dist_controller = MagicMock()
        mock_get_storage_instance.return_value = MagicMock(
            distribution_controller=mock_dist_controller
        )
        test_file = MagicMock()

        # Should return early because the range start is > 0:
        test_request = MagicMock(http_range=MagicMock(start=1))
        await http_util.handle_video_request(test_request, test_file)
        mock_dist_controller.handle_distribution.assert_called_once_with(
            test_file,
            load,
        )
        mock_ensure_acceptable_load.assert_called_once_with(
            load,
            log=http_util._log,
        )
        mock_dist_controller.get_node_to_serve.assert_not_called()

        mock_dist_controller.handle_distribution.reset_mock()
        mock_ensure_acceptable_load.reset_mock()

        # Should return early because no distributor node was found:
        test_request.http_range.start = None
        mock_dist_controller.get_node_to_serve.return_value = None, False
        await http_util.handle_video_request(test_request, test_file)
        mock_dist_controller.handle_distribution.assert_called_once_with(
            test_file,
            load,
        )
        mock_ensure_acceptable_load.assert_called_once_with(
            load,
            log=http_util._log,
        )
        mock_dist_controller.get_node_to_serve.assert_called_once_with(
            test_file
        )

        mock_dist_controller.handle_distribution.reset_mock()
        mock_ensure_acceptable_load.reset_mock()
        mock_dist_controller.get_node_to_serve.reset_mock()

        # Should redirect because a distributor node has the file:
        node = MagicMock()
        mock_dist_controller.get_node_to_serve.return_value = node, True
        class FakeRedirect(Exception): ...
        mock_redirect_exception_cls.return_value = FakeRedirect()
        with self.assertRaises(FakeRedirect):
            await http_util.handle_video_request(test_request, test_file)
        mock_dist_controller.handle_distribution.assert_called_once_with(
            test_file,
            load,
        )
        mock_ensure_acceptable_load.assert_not_called()
        mock_dist_controller.get_node_to_serve.assert_called_once_with(
            test_file
        )
        mock_redirect_exception_cls.assert_called_once_with(
            test_request,
            node,
            test_file,
            log=http_util._log,
        )

        mock_dist_controller.handle_distribution.reset_mock()
        mock_dist_controller.get_node_to_serve.reset_mock()
        mock_redirect_exception_cls.reset_mock()

        # Should return because dist. node does not have the full file yet
        # and storage load is not above threshold:
        mock_dist_controller.get_node_to_serve.return_value = node, False
        await http_util.handle_video_request(test_request, test_file)
        mock_dist_controller.handle_distribution.assert_called_once_with(
            test_file,
            load,
        )
        mock_ensure_acceptable_load.assert_not_called()
        mock_dist_controller.get_node_to_serve.assert_called_once_with(
            test_file
        )
        mock_redirect_exception_cls.assert_not_called()

        mock_dist_controller.handle_distribution.reset_mock()
        mock_dist_controller.get_node_to_serve.reset_mock()

        # Should redirect because storage load is above threshold:
        load += 0.001
        mock_network_interfaces.get_tx_load.return_value = load
        with patch.object(http_util, "async_sleep") as mock_async_sleep:
            with self.assertRaises(FakeRedirect):
                await http_util.handle_video_request(test_request, test_file)
        mock_dist_controller.handle_distribution.assert_called_once_with(
            test_file,
            load,
        )
        mock_ensure_acceptable_load.assert_not_called()
        mock_dist_controller.get_node_to_serve.assert_called_once_with(
            test_file
        )
        mock_redirect_exception_cls.assert_called_once_with(
            test_request,
            node,
            test_file,
            log=http_util._log,
        )
        mock_async_sleep.assert_awaited_once_with(1)


    @patch.object(http_util, "file_serve_headers")
    @patch.object(http_util.StorageFileController, "get_instance")
    async def test_handle_thumbnail_request(
            self,
            mock_get_storage_instance: MagicMock,
            mock_file_serve_headers: MagicMock,
    ) -> None:
        mock_cache = MagicMock()
        mock_get_storage_instance.return_value = mock_storage = MagicMock(
            thumb_memory_cache=mock_cache
        )
        mock_file_serve_headers.return_value = mock_headers = {"foo": "bar"}
        mock_jwt_data = MagicMock(thumb_id=None)

        # Missing thumbnail ID:
        with self.assertRaises(http_util.HTTPBadRequest):
            with self.assertLogs(http_util._log, logging.WARNING):
                await http_util.handle_thumbnail_request(mock_jwt_data)
        mock_get_storage_instance.assert_not_called()
        mock_storage.get_perm_thumbnail_path.assert_not_called()
        mock_storage.get_temp_thumbnail_path.assert_not_called()
        mock_cache.get_and_update.assert_not_called()
        mock_file_serve_headers.assert_not_called()

        mock_jwt_data.thumb_id = num = 123
        mock_jwt_data.hash = hash_ = 123
        mock_jwt_data.file_ext = ext = 123
        mock_jwt_data.type = None  # invalid

        # This should be unreachable because of the file type enum:
        with self.assertRaises(RuntimeError):
            await http_util.handle_thumbnail_request(mock_jwt_data)
        mock_get_storage_instance.assert_called_once_with()
        mock_storage.get_perm_thumbnail_path.assert_not_called()
        mock_storage.get_temp_thumbnail_path.assert_not_called()
        mock_cache.get_and_update.assert_not_called()
        mock_file_serve_headers.assert_not_called()

        mock_get_storage_instance.reset_mock()

        mock_jwt_data.type = http_util.FileType.THUMBNAIL_TEMP
        mock_storage.get_temp_thumbnail_path.return_value = path = object()
        mock_cache.get_and_update.side_effect = FileNotFoundError

        # Temp thumbnail could not be found on disk:
        with self.assertRaises(http_util.HTTPNotFound):
            await http_util.handle_thumbnail_request(mock_jwt_data)
        mock_get_storage_instance.assert_called_once_with()
        mock_storage.get_perm_thumbnail_path.assert_not_called()
        mock_storage.get_temp_thumbnail_path.assert_called_once_with(
            hash_, num=num
        )
        mock_cache.get_and_update.assert_called_once_with(path)
        mock_file_serve_headers.assert_not_called()

        mock_get_storage_instance.reset_mock()
        mock_storage.get_temp_thumbnail_path.reset_mock()
        mock_cache.get_and_update.reset_mock()

        mock_body = b"spam"
        mock_cache.get_and_update = AsyncMock(return_value=mock_body)

        # Temp thumbnail successfully returned:
        output = await http_util.handle_thumbnail_request(mock_jwt_data)
        self.assertIsInstance(output, http_util.Response)
        self.assertEqual(mock_headers["foo"], output.headers["foo"])
        self.assertEqual(mock_body, output.body)
        self.assertEqual("image/jpeg", output.content_type)
        mock_get_storage_instance.assert_called_once_with()
        mock_storage.get_perm_thumbnail_path.assert_not_called()
        mock_storage.get_temp_thumbnail_path.assert_called_once_with(
            hash_, num=num
        )
        mock_cache.get_and_update.assert_called_once_with(path)
        mock_file_serve_headers.assert_called_once_with()

        mock_get_storage_instance.reset_mock()
        mock_storage.get_temp_thumbnail_path.reset_mock()
        mock_cache.get_and_update.reset_mock()
        mock_file_serve_headers.reset_mock()

        mock_jwt_data.type = http_util.FileType.THUMBNAIL
        mock_storage.get_perm_thumbnail_path.return_value = path = object()

        # Permanently stored thumbnail successfully returned:
        output = await http_util.handle_thumbnail_request(mock_jwt_data)
        self.assertIsInstance(output, http_util.Response)
        self.assertEqual(mock_headers["foo"], output.headers["foo"])
        self.assertEqual(mock_body, output.body)
        self.assertEqual("image/jpeg", output.content_type)
        mock_get_storage_instance.assert_called_once_with()
        mock_storage.get_perm_thumbnail_path.assert_called_once_with(
            hash_, ext, num=num
        )
        mock_storage.get_temp_thumbnail_path.assert_not_called()
        mock_cache.get_and_update.assert_called_once_with(path)
        mock_file_serve_headers.assert_called_once_with()

        mock_get_storage_instance.reset_mock()
        mock_storage.get_perm_thumbnail_path.reset_mock()
        mock_cache.get_and_update.reset_mock()
        mock_file_serve_headers.reset_mock()

        mock_storage.get_perm_thumbnail_path.side_effect = FileNotFoundError

        # Supposedly permanent thumbnail not known to file storage:
        with self.assertRaises(http_util.HTTPNotFound):
            await http_util.handle_thumbnail_request(mock_jwt_data)
        mock_get_storage_instance.assert_called_once_with()
        mock_storage.get_perm_thumbnail_path.assert_called_once_with(
            hash_, ext, num=num
        )
        mock_storage.get_temp_thumbnail_path.assert_not_called()
        mock_cache.get_and_update.assert_not_called()
        mock_file_serve_headers.assert_not_called()
