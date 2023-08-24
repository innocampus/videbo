from pathlib import Path
from shutil import rmtree
from subprocess import DEVNULL
from tempfile import mkdtemp
from typing import Any, Union
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from videbo.misc import functions


class FunctionsTestCase(IsolatedAsyncioTestCase):
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

    def test_move_file(self) -> None:
        test_content = "abc"
        test_dir = mkdtemp(prefix="videbo_test")
        try:
            src_path = Path(test_dir, "foo")
            with src_path.open("w") as f:
                f.write(test_content)
            dst_dir = Path(test_dir, "bar")
            dst_dir.mkdir()
            dst_path = Path(dst_dir, "baz")

            # Destination directory exists:
            src, dst = str(src_path), str(dst_path)
            chmod = 0o100600
            self.assertTrue(functions.move_file(src, dst, chmod=chmod))
            self.assertFalse(src_path.exists())
            self.assertEqual(test_content, dst_path.read_text())
            self.assertEqual(chmod, dst_path.stat().st_mode)

            rmtree(dst_dir)
            with src_path.open("w") as f:
                f.write(test_content)

            # Destination directory should be created:
            create_dir_mode = 0o777
            self.assertTrue(functions.move_file(
                src, dst, create_dir_mode=create_dir_mode, chmod=chmod
            ))
            self.assertFalse(src_path.exists())
            self.assertEqual(test_content, dst_path.read_text())
            self.assertEqual(chmod, dst_path.stat().st_mode)

            with src_path.open("w") as f:
                f.write(test_content + " xyz 123")
            with dst_path.open("w") as f:
                f.write(test_content)

            # Destination file already exists;
            # source should just be removed:
            self.assertFalse(functions.move_file(src, dst))
            self.assertFalse(src_path.exists())
            self.assertEqual(test_content, dst_path.read_text())

            with src_path.open("w") as f:
                f.write(test_content + " xyz 123")

            # Destination file already exists;
            # error should be raised:
            with self.assertRaises(FileExistsError):
                functions.move_file(src, dst, safe=True)
            self.assertTrue(src_path.exists())
            self.assertEqual(test_content, dst_path.read_text())
        finally:
            rmtree(test_dir)

    def test_copy_file(self) -> None:
        test_content = "abc"
        test_dir = mkdtemp(prefix="videbo_test")
        try:
            src_path = Path(test_dir, "foo")
            with src_path.open("w") as f:
                f.write(test_content)
            dst_path = Path(test_dir, "baz")
            src, dst = str(src_path), str(dst_path)

            # Destination file does not exist;
            # source file should be copied:
            self.assertTrue(functions.copy_file(src, dst))
            self.assertEqual(test_content, dst_path.read_text())

            with dst_path.open("w") as f:
                f.write(test_content + " xyz 123")

            # Destination file exists and should be overwritten:
            self.assertTrue(functions.copy_file(src, dst, overwrite=True))
            self.assertEqual(test_content, dst_path.read_text())

            # Destination file exists; do nothing:
            self.assertFalse(functions.copy_file(src, dst))
        finally:
            rmtree(test_dir)

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

    def test_mime_type_from_file_name(self) -> None:
        path = "something.mp4"
        mime_type = functions.mime_type_from_file_name(path)
        self.assertEqual("video/mp4", mime_type)
        path = "something.invalid"
        mime_type = functions.mime_type_from_file_name(path)
        self.assertEqual("application/octet-stream", mime_type)
        with self.assertRaises(ValueError):
            functions.mime_type_from_file_name(path, strict=True)
            functions.mime_type_from_file_name("", strict=True)
            functions.mime_type_from_file_name(".mp4", strict=True)

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

    @patch.object(functions, "_create_subproc")
    async def test_create_user_subprocess(self, mock_create_subprocess_exec: AsyncMock) -> None:
        program = "foo"
        args = ("--bar", "baz")
        kwargs = {"stdout": "spam/eggs", "abc": 123}
        output = await functions.create_user_subprocess(program, *args, **kwargs)
        self.assertEqual(
            mock_create_subprocess_exec.return_value,
            output,
        )
        mock_create_subprocess_exec.assert_awaited_once_with(
            program,
            *args,
            **kwargs,
            stderr=DEVNULL,
        )

        mock_create_subprocess_exec.reset_mock()
        kwargs = {"abc": 123}
        sudo_user = "tester"
        output = await functions.create_user_subprocess(
            program, *args, sudo_user=sudo_user, **kwargs
        )
        self.assertEqual(
            mock_create_subprocess_exec.return_value,
            output,
        )
        mock_create_subprocess_exec.assert_awaited_once_with(
            "sudo",
            "-u",
            sudo_user,
            program,
            *args,
            **kwargs,
            stdout=DEVNULL,
            stderr=DEVNULL,
        )

    def test_is_subclass(self) -> None:
        self.assertTrue(functions.is_subclass(bool, int))
        self.assertFalse(functions.is_subclass(str, int))
        self.assertFalse(functions.is_subclass(Union[int, float], int))
        self.assertFalse(functions.is_subclass(123, int))
        self.assertFalse(functions.is_subclass(object(), int))
