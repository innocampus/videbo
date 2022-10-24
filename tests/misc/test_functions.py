from pathlib import Path
from typing import Any, Union
from unittest import IsolatedAsyncioTestCase
from unittest.mock import MagicMock, patch

from videbo.misc import functions


class FunctionsTestCase(IsolatedAsyncioTestCase):
    def test_ensure_string_does_not_end_with_slash(self) -> None:
        url = "foo///"
        expected_output = "foo"
        output = functions.ensure_string_does_not_end_with_slash(url)
        self.assertEqual(expected_output, output)

        url = "/"
        expected_output = ""
        output = functions.ensure_string_does_not_end_with_slash(url)
        self.assertEqual(expected_output, output)

        url = "http://localhost:9020"
        expected_output = url
        output = functions.ensure_string_does_not_end_with_slash(url)
        self.assertEqual(expected_output, output)

    @patch.object(functions.os, "statvfs")
    async def test_get_free_disk_space(self, mock_statvfs: MagicMock) -> None:
        test_path = "foo/bar"
        mock_statvfs.return_value = MagicMock(
            f_bavail=100_000,
            f_frsize=1000,
        )
        expected_output = 100_000_000 / functions.MEGA
        output = await functions.get_free_disk_space(test_path)
        self.assertEqual(expected_output, output)
        mock_statvfs.assert_called_once_with(test_path)

    def test_sanitize_filename(self) -> None:
        name = r"🔧fÄäo\\\o.🔥.ß␣.#тест#.b␀'*a`?r␦"
        expected_output = "foo..bar"
        output = functions.sanitize_filename(name)
        self.assertEqual(expected_output, output)

    def test_rel_path(self) -> None:
        name = "foo/bar"
        with self.assertRaises(ValueError):
            functions.rel_path(name)

        name = "foobar.txt"
        expected_output = Path("fo", "foobar.txt")
        output = functions.rel_path(name)
        self.assertEqual(expected_output, output)

    def test_get_parameters_of_class(self) -> None:
        def function(x: Any, y: bool, z: Union[str, float], foo: int) -> None:
            pass
        cls = int
        expected_names = ["y", "foo"]
        output = functions.get_parameters_of_class(function, cls)
        self.assertListEqual(expected_names, [p.name for p in output])

        cls = bool
        expected_names = ["y"]
        output = functions.get_parameters_of_class(function, cls)
        self.assertListEqual(expected_names, [p.name for p in output])

    def test_get_route_model_param(self) -> None:
        async def function(x: Any, y: bool, z: Union[str, float], foo: int) -> None:
            pass
        cls = str
        with self.assertRaises(functions.InvalidRouteSignature):
            functions.get_route_model_param(function, cls)

        cls = int
        with self.assertRaises(functions.InvalidRouteSignature):
            functions.get_route_model_param(function, cls)

        cls = bool
        name, ann = functions.get_route_model_param(function, cls)
        self.assertEqual("y", name)
        self.assertIs(bool, ann)
