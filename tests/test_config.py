from pathlib import Path
from unittest import TestCase
from unittest.mock import MagicMock, patch

from tests.silent_log import SilentLogMixin
from videbo import config


class SettingsBaseModelTestCase(TestCase):

    class TestModel(config.SettingsBaseModel):
        list_of_str: list[str]
        set_of_str: set[str]
        foo: str
        bar: int

    def test_split_str(self) -> None:
        data = dict(list_of_str="a,b,c", set_of_str="x,y", foo="FOO", bar=1)
        instance = self.TestModel(**data)
        self.assertListEqual(["a", "b", "c"], instance.list_of_str)
        self.assertSetEqual({"y", "x"}, instance.set_of_str)
        self.assertEqual("FOO", instance.foo)
        self.assertEqual(1, instance.bar)

    def test_discard_empty_str_elements(self) -> None:
        data = dict(
            list_of_str=["a", "b", "", "", ""],
            set_of_str={"x", "y", ""},
            foo="FOO",
            bar=1,
        )
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

        obj = TestModel(**dict(foo=None))
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
    def test_ensure_string_does_not_end_with_slash(self) -> None:
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


class DistributionSettingsTestCase(TestCase):
    def test_ensure_min_reset_freq(self) -> None:
        obj = config.DistributionSettings(reset_views_every_minutes=-10)
        self.assertEqual(1., obj.reset_views_every_minutes)
        obj = config.DistributionSettings(reset_views_every_minutes=0)
        self.assertEqual(1., obj.reset_views_every_minutes)
        obj = config.DistributionSettings(reset_views_every_minutes=2.5)
        self.assertEqual(2.5, obj.reset_views_every_minutes)


class SettingsTestCase(TestCase):
    def test_make_url(self) -> None:
        addr, port = "foo", 123
        path, scheme = "bar", "ftp"
        expected_output = f"{scheme}://{addr}:{port}/{path}"

        obj = config.Settings(listen_address=addr, listen_port=port)
        output = obj.make_url(path=path, scheme=scheme)
        self.assertEqual(expected_output, output)

        obj = config.Settings(listen_address=addr, listen_port=port)
        output = obj.make_url(path="/" + path, scheme=scheme)
        self.assertEqual(expected_output, output)


class FunctionsTestCase(SilentLogMixin, TestCase):
    @patch.object(config, "load_yaml")
    def test_config_file_settings(self, mock_load_yaml: MagicMock) -> None:
        mock_load_yaml.return_value = expected_output = {"foo": "bar"}
        path1 = MagicMock(is_file=lambda: False)
        path2 = MagicMock(suffix=".yaml")
        path3 = MagicMock(suffix=".unknown")
        mock_settings = MagicMock(
            get_config_file_paths=lambda: [path1, path2, path3]
        )
        output = config.config_file_settings(mock_settings)
        self.assertDictEqual(expected_output, output)
        mock_load_yaml.assert_called_once_with(path2)

    @patch.object(config.yaml, "safe_load")
    @patch.object(config.Path, "open")
    def test_load_yaml(
        self,
        mock_path_open: MagicMock,
        mock_safe_load: MagicMock,
    ) -> None:
        mock_file = object()
        mock_path_open.return_value = MagicMock(
            __enter__=lambda _: mock_file,
            __exit__=MagicMock(),
        )
        mock_safe_load.return_value = object()
        with self.assertRaises(TypeError):
            config.load_yaml("foo")
        mock_path_open.assert_called_once_with("r")
        mock_safe_load.assert_called_once_with(mock_file)

        mock_path_open.reset_mock()
        mock_safe_load.reset_mock()

        mock_safe_load.return_value = expected_output = {"spam": "eggs"}
        output = config.load_yaml("foo")
        self.assertDictEqual(expected_output, output)
        mock_path_open.assert_called_once_with("r")
        mock_safe_load.assert_called_once_with(mock_file)
