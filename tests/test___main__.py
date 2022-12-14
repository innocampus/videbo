import logging
from pathlib import Path
from unittest import TestCase
from unittest.mock import AsyncMock, MagicMock, patch

from videbo import __main__


class MainTestCase(TestCase):
    def test_path_list(self) -> None:
        paths = "foo,  bar  , baz "
        expected_output = [Path("foo"), Path("bar"), Path("baz")]
        output = __main__.path_list(paths)
        self.assertListEqual(expected_output, output)

    @patch.object(__main__, "execute_cli_command")
    def test_cli_run(self, mock_execute_cli_command: AsyncMock) -> None:
        test_kwargs = {"foo": "bar", "spam": "eggs"}
        __main__.cli_run(**test_kwargs)
        mock_execute_cli_command.assert_awaited_once_with(**test_kwargs)

    @patch.object(__main__, "setup_cli_args")
    def test_parse_cli(self, mock_setup_cli_args: MagicMock) -> None:
        args = ["storage"]
        expected_output = {
            __main__.MODE: __main__.STORAGE,
            __main__.FUNCTION: __main__.start_storage,
        }
        output = __main__.parse_cli(args)
        self.assertDictEqual(expected_output, output)
        mock_setup_cli_args.assert_called_once()
        mock_setup_cli_args.reset_mock()

        args = ["-c", "foo_path,bar_path", "storage"]
        expected_output = {
            __main__.MODE: __main__.STORAGE,
            __main__.FUNCTION: __main__.start_storage,
            __main__.CONFIG_FILE_PATHS_PARAM: [
                Path("foo_path"),
                Path("bar_path"),
            ],
        }
        output = __main__.parse_cli(args)
        self.assertDictEqual(expected_output, output)
        mock_setup_cli_args.assert_called_once()
        mock_setup_cli_args.reset_mock()

        args = ["-A", "some.host", "-P", "456", "distributor"]
        expected_output = {
            __main__.MODE: __main__.DISTRIBUTOR,
            __main__.FUNCTION: __main__.start_distributor,
            __main__.LISTEN_ADDRESS: "some.host",
            __main__.LISTEN_PORT: 456,
        }
        output = __main__.parse_cli(args)
        self.assertDictEqual(expected_output, output)
        mock_setup_cli_args.assert_called_once()
        mock_setup_cli_args.reset_mock()

        args = ["-c", "foo_path", "cli"]
        expected_output = {
            __main__.MODE: __main__.CLI,
            __main__.FUNCTION: __main__.cli_run,
            __main__.CONFIG_FILE_PATHS_PARAM: [
                Path("foo_path"),
            ],
        }
        output = __main__.parse_cli(args)
        self.assertDictEqual(expected_output, output)
        mock_setup_cli_args.assert_called_once()

    @patch.object(__main__.Path, "open")
    def test_prepare_settings(self, mock_path_open: MagicMock) -> None:
        test_kwargs = {
            __main__.MODE: "foo",
            __main__.LISTEN_ADDRESS: "some.host",
            "spam": "eggs",
        }
        expected_output = Path(".", ".videbo_foo_settings.json")

        # Dev mode `False`:

        with patch.object(__main__, "settings") as mock_settings:
            mock_settings.dev_mode = False
            self.assertNotEqual("some.host", mock_settings.listen_address)
            output = __main__.prepare_settings(test_kwargs)
            # Check that the path was correctly formed:
            self.assertEqual(expected_output, output)
            # Check that the settings field was correctly assigned:
            self.assertEqual("some.host", mock_settings.listen_address)
            # Check that the dictionary was correctly reduced:
            self.assertDictEqual({"spam": "eggs"}, test_kwargs)
        mock_path_open.assert_not_called()

        test_kwargs = {
            __main__.MODE: "foo",
            __main__.LISTEN_ADDRESS: "some.host",
            "spam": "eggs",
        }

        # Dev mode `True` and able to open settings dump file:

        mock_file = MagicMock()
        mock_path_open.return_value = MagicMock(
            __enter__=lambda _: mock_file,
            __exit__=MagicMock(),
        )
        with patch.object(__main__, "settings") as mock_settings:
            mock_settings.dev_mode = True
            self.assertNotEqual("some.host", mock_settings.listen_address)
            with self.assertLogs(level=logging.WARNING):
                output = __main__.prepare_settings(test_kwargs)
            # Check that the path was correctly formed:
            self.assertEqual(expected_output, output)
            # Check that the settings field was correctly assigned:
            self.assertEqual("some.host", mock_settings.listen_address)
            # Check that the dictionary was correctly reduced:
            self.assertDictEqual({"spam": "eggs"}, test_kwargs)
            mock_path_open.assert_called_once_with("w")
            mock_settings.json.assert_called_once_with(indent=4)
            mock_file.write.assert_called_once_with(
                mock_settings.json.return_value
            )

        mock_path_open.reset_mock()
        mock_file.reset_mock()
        test_kwargs = {
            __main__.MODE: "foo",
            __main__.LISTEN_ADDRESS: "some.host",
            "spam": "eggs",
        }

        # Dev mode `True` and no permission to open settings dump file:

        mock_path_open.side_effect = PermissionError
        with patch.object(__main__, "settings") as mock_settings:
            mock_settings.dev_mode = True
            self.assertNotEqual("some.host", mock_settings.listen_address)
            with self.assertLogs(level=logging.WARNING):
                output = __main__.prepare_settings(test_kwargs)
            # Check that the path was correctly formed:
            self.assertEqual(expected_output, output)
            # Check that the settings field was correctly assigned:
            self.assertEqual("some.host", mock_settings.listen_address)
            # Check that the dictionary was correctly reduced:
            self.assertDictEqual({"spam": "eggs"}, test_kwargs)
            mock_path_open.assert_called_once_with("w")
            mock_settings.json.assert_not_called()
            mock_file.assert_not_called()

    @patch.object(__main__.logging, "basicConfig")
    @patch.object(__main__, "prepare_settings")
    @patch.object(__main__, "parse_cli")
    def test_main(
        self,
        mock_parse_cli: MagicMock,
        mock_prepare_settings: MagicMock,
        mock_log_basic_conf: MagicMock,
    ) -> None:
        mock_kwargs = {"spam": "eggs"}
        mock_run = MagicMock()
        mock_parse_cli.return_value = mock_kwargs | {
            __main__.FUNCTION: mock_run,
        }
        mock_prepare_settings.return_value = mock_path = MagicMock()

        self.assertIsNone(__main__.main())
        mock_log_basic_conf.assert_called_once_with(level=logging.INFO)
        mock_parse_cli.assert_called_once_with()
        mock_prepare_settings.assert_called_once_with(mock_kwargs)
        mock_run.assert_called_once_with(**mock_kwargs)
        mock_path.unlink.assert_called_once_with(missing_ok=True)
