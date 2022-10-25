from argparse import ArgumentParser
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from videbo.cli import args as args_module


class ArgsTestCase(IsolatedAsyncioTestCase):
    def test_setup_cli_args(self) -> None:
        parser = ArgumentParser()
        args_module.setup_cli_args(parser)

        args = ["-y", "status"]
        expected_output = {
            args_module.YES: True,
            args_module.CMD: args_module.print_storage_status,
        }
        output = vars(parser.parse_args(args))
        self.assertDictEqual(expected_output, output)

        args = ["find-orphaned-files"]
        expected_output = {
            args_module.CMD: args_module.find_orphaned_files,
            args_module.DELETE: False,
        }
        output = vars(parser.parse_args(args))
        self.assertDictEqual(expected_output, output)

        args = ["-y", "find-orphaned-files", "-d"]
        expected_output = {
            args_module.YES: True,
            args_module.CMD: args_module.find_orphaned_files,
            args_module.DELETE: True,
        }
        output = vars(parser.parse_args(args))
        self.assertDictEqual(expected_output, output)

        test_url = "http://foo.bar"
        args = ["disable-dist-node", test_url]
        expected_output = {
            args_module.CMD: args_module.disable_distributor_node,
            args_module.URL: test_url,
        }
        output = vars(parser.parse_args(args))
        self.assertDictEqual(expected_output, output)

        args = ["enable-dist-node", test_url]
        expected_output = {
            args_module.CMD: args_module.enable_distributor_node,
            args_module.URL: test_url,
        }
        output = vars(parser.parse_args(args))
        self.assertDictEqual(expected_output, output)

        with patch("sys.stderr") as mock_stderr:
            with self.assertRaises(SystemExit):
                parser.parse_args(["disable-dist-node"])
            mock_stderr.write.assert_called()

    @patch.object(args_module, "StorageClient")
    async def test_execute_cli_command(self, mock_client_cls: MagicMock) -> None:
        mock_client = object()
        mock_aenter = AsyncMock(return_value=mock_client)
        mock_aexit = AsyncMock()
        mock_context_manager = MagicMock(
            __aenter__=mock_aenter,
            __aexit__=mock_aexit,
        )
        mock_client_cls.return_value = mock_context_manager

        mock_run_command = AsyncMock()
        kwargs = {"spam": "eggs", "foo": "bar"}
        await args_module.execute_cli_command(
            **kwargs,
            **{args_module.CMD: mock_run_command},
        )
        mock_client_cls.assert_called_once_with()
        mock_aenter.assert_awaited_once_with()
        mock_run_command.assert_awaited_once_with(mock_client, **kwargs)
        mock_aexit.assert_awaited_once_with(None, None, None)
