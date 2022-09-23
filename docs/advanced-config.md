## Settings sources

When **Videbo** is run, the first thing it does is to load all the settings it requires to. All settings have a sensible default value, which means it is technically not required to provide any configuration. However for any reasonable use case beyond testing, you will want to provide specific configuration.

Videbo overloads its settings from multiple sources in the following order:

1. Config file(s)
2. Environment variables
3. Command line options

This means for example that the `listen_port` directive in a configuration file will always be ignored in favor of a `-P`/`--listen-port` option provided on the command line.

By default, Videbo will search for configuration files in the following locations in that order:

1. `/etc/videbo/config.ini`
2. `./config.ini`

The order of precedence is analogous to the one described above. If you provide custom configuration file paths via the `-c`/`--config-file-paths` command line option, directives placed there will take precedence over those in the default config files listed above.

## Environment variables

In addition to CLI and config files, you have the option of setting _any_ configuration directive via an environment variable. All environment variables for Videbo must have the following form:
```
VIDEBO_[DIRECTIVE]
```

Here `[DIRECTIVE]` must correspond to a valid configuration directive. See the [`config.example.ini`](https://github.com/innocampus/videbo/blob/master/config.example.ini){.external-link target=_blank} for an exhaustive listing of all available directives and their meaning. Environment variable names are **not** case-sensitive. The following are both valid for example:
```shell
export VIDEBO_INTERNAL_API_SECRET=supersecretstring
export videbo_listen_port=9001
```
