from pathlib import Path
from unittest import TestCase
from unittest.mock import MagicMock, patch

from pydantic import ValidationError

from tests.silent_log import SilentLogMixin
from videbo import config


class SettingsBaseModelTestCase(TestCase):

    class TestModel(config.SettingsBaseModel):
        list_of_str: list[str]
        set_of_str: set[str]
        foo: str
        bar: int

    def test_split_str(self) -> None:
        data = {
            "list_of_str": "a,b,c",
            "set_of_str": "x,y",
            "foo": "FOO",
            "bar": 1,
        }
        instance = self.TestModel(**data)
        self.assertListEqual(["a", "b", "c"], instance.list_of_str)
        self.assertSetEqual({"y", "x"}, instance.set_of_str)
        self.assertEqual("FOO", instance.foo)
        self.assertEqual(1, instance.bar)

    def test_discard_empty_str_elements(self) -> None:
        data = {
            "list_of_str": ["a", "b", "", "", ""],
            "set_of_str": {"x", "y", ""},
            "foo": "FOO",
            "bar": 1,
        }
        instance = self.TestModel(**data)
        self.assertListEqual(["a", "b"], instance.list_of_str)
        self.assertSetEqual({"y", "x"}, instance.set_of_str)
        self.assertEqual("FOO", instance.foo)
        self.assertEqual(1, instance.bar)


class BaseSettingsTestCase(TestCase):
    @patch.object(config.PydanticBaseSettings, "__init__")
    def test___init__(self, mock_super___init__: MagicMock) -> None:
        add_config_file_paths = ["foo", "bar"]
        kwargs = {
            "spam": "eggs",
        }

        obj = config.BaseSettings(**kwargs, **{
            config.CONFIG_FILE_PATHS_PARAM: add_config_file_paths
        })
        self.assertListEqual(
            config.DEFAULT_CONFIG_FILE_PATHS + [Path("foo"), Path("bar")],
            obj._config_file_paths,
        )
        mock_super___init__.assert_called_once_with(**kwargs)

    def test_get_config_file_paths(self) -> None:
        obj = config.BaseSettings()
        obj._config_file_paths = expected_output = MagicMock()
        self.assertEqual(expected_output, obj.get_config_file_paths())

    def test_none_to_model_defaults(self) -> None:
        class SubModel(config.SettingsBaseModel):
            a: int = 1

        default = SubModel()

        class TestModel(config.BaseSettings):
            foo: SubModel = default

        obj = TestModel(**{"foo": None})
        self.assertEqual(default, obj.foo)

    def test_config_customise_sources(self) -> None:
        init_settings, env_settings = MagicMock(), MagicMock()
        expected_output = (
            init_settings,
            env_settings,
            config.config_file_settings,
        )
        output = config.BaseSettings.Config.customise_sources(
            init_settings=init_settings,
            env_settings=env_settings,
            file_secret_settings=MagicMock(),
        )
        self.assertTupleEqual(expected_output, output)


class ValidatorTestCase(TestCase):
    def test_no_slash_at_the_end(self) -> None:
        url = "foo///"
        expected_output = "foo"
        output = config.no_slash_at_the_end(url)
        self.assertEqual(expected_output, output)

        url = "/"
        expected_output = ""
        output = config.no_slash_at_the_end(url)
        self.assertEqual(expected_output, output)

        url = "http://localhost:9020"
        expected_output = url
        output = config.no_slash_at_the_end(url)
        self.assertEqual(expected_output, output)

        something_else = MagicMock()
        output = config.no_slash_at_the_end(something_else)
        self.assertIs(something_else, output)


class WebserverSettingsTestCase(TestCase):
    def test_get_x_accel_limit_rate(self) -> None:
        rate_mbit = 8
        expected_output = 1024 ** 2
        obj = config.WebserverSettings(x_accel_limit_rate_mbit=rate_mbit)
        output = obj.get_x_accel_limit_rate(internal=False)
        self.assertEqual(expected_output, output)
        output = obj.get_x_accel_limit_rate(internal=True)
        self.assertEqual(0, output)


class VideoSettingsTestCase(TestCase):
    def test_ensure_valid_mime_type(self) -> None:
        mime_types = {"video/x-m4v", "video/mpeg"}
        valid = config.VideoSettings(mime_types_allowed=mime_types)
        self.assertSetEqual(mime_types, valid.mime_types_allowed)
        mime_types.add("foo/bar")
        with self.assertRaises(ValidationError):
            config.VideoSettings(mime_types_allowed=mime_types)

    def test_ensure_valid_format(self) -> None:
        formats = {"m4v", "mpg"}
        valid = config.VideoSettings(container_formats_allowed=formats)
        self.assertSetEqual(formats, valid.container_formats_allowed)
        formats.add("foobarbaz")
        with self.assertRaises(ValidationError):
            config.VideoSettings(container_formats_allowed=formats)

    def test_max_file_size_bytes(self) -> None:
        in_mb = 69.420
        in_b = int(in_mb * config.MEGA)
        obj = config.VideoSettings(max_file_size_mb=in_mb)
        self.assertEqual(in_b, obj.max_file_size_bytes)


class DistributionSettingsTestCase(TestCase):
    def test_slash_removal(self) -> None:
        url1, url2 = "http://foo.bar", "http://spam.eggs"
        data = {"static_node_base_urls": [url1 + "/", url2]}
        obj = config.DistributionSettings.parse_obj(data)
        self.assertListEqual([url1, url2], obj.static_node_base_urls)


class SettingsTestCase(TestCase):
    def test_make_url(self) -> None:
        addr, port = "12.34.56.78", 123
        path, scheme = "bar", "ftp"
        expected_output = f"{scheme}://{addr}:{port}/{path}"

        obj = config.Settings(listen_address=addr, listen_port=port)
        output = obj.make_url(path=path, scheme=scheme)
        self.assertEqual(expected_output, output)

        obj = config.Settings(listen_address=addr, listen_port=port)
        output = obj.make_url(path="/" + path, scheme=scheme)
        self.assertEqual(expected_output, output)

    def test_temp_file_cleanup_freq(self) -> None:
        hours = 123.456
        obj = config.Settings(temp_file_cleanup_freq_hours=hours)
        self.assertEqual(hours * 3600, obj.temp_file_cleanup_freq)

    def test_views_retention_seconds(self) -> None:
        minutes = 420.69
        obj = config.Settings.parse_obj(
            {"distribution": {"views_retention_minutes": minutes}}
        )
        self.assertEqual(minutes * 60, obj.views_retention_seconds)

    def test_views_update_freq(self) -> None:
        minutes = 123.456
        obj = config.Settings.parse_obj(
            {"distribution": {"views_update_freq_minutes": minutes}}
        )
        self.assertEqual(minutes * 60, obj.views_update_freq)

    def test_dist_cleanup_freq(self) -> None:
        minutes = 123.456
        obj = config.Settings.parse_obj(
            {"distribution": {"node_cleanup_freq_minutes": minutes}}
        )
        self.assertEqual(minutes * 60, obj.dist_cleanup_freq)


class FunctionsTestCase(SilentLogMixin, TestCase):
    @patch.object(config, "load_toml")
    def test_config_file_settings(self, mock_load_toml: MagicMock) -> None:
        mock_load_toml.return_value = expected_output = {"foo": "bar"}
        path1 = MagicMock(is_file=lambda: False)
        path2 = MagicMock(suffix=".toml")
        path3 = MagicMock(suffix=".unknown")
        mock_settings = MagicMock(
            get_config_file_paths=lambda: [path1, path2, path3]
        )
        output = config.config_file_settings(mock_settings)
        self.assertDictEqual(expected_output, output)
        mock_load_toml.assert_called_once_with(path2)

    @patch.object(config.tomli, "load")
    @patch.object(config.Path, "open")
    def test_load_toml(
        self,
        mock_path_open: MagicMock,
        mock_load: MagicMock,
    ) -> None:
        mock_file = object()
        mock_path_open.return_value = MagicMock(
            __enter__=lambda _: mock_file,
            __exit__=MagicMock(),
        )
        mock_load.return_value = expected_output = {"spam": "eggs"}
        output = config.load_toml("foo")
        self.assertDictEqual(expected_output, output)
        mock_path_open.assert_called_once_with("rb")
        mock_load.assert_called_once_with(mock_file)
