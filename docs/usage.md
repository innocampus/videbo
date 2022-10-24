## Intro

With **Videbo** there are three fundamental modes of operation:

* Running a **Storage** Node
* Running a **Distributor** Node
* Executing **administrative** commands

The **Storage** Node is the central part of the **Videbo**. It encompasses the main video hosting API, format checking, monitoring capabilities (optional), and distribution logic (optional). It has its own webserver and it can be run as a standalone service. Every Videbo setup requires exactly one Storage Node.

The purpose of a **Distributor** Node is to provide load balancing and redundancy to a Videbo hosting service. Any (reasonable) number of Distributor Nodes can be connected to one Storage Node. Depending on the overall configuration and load, requests for video streams are automatically redirected from Storage to Distributor.

Additionally, Videbo has a built-in **command line interface** (CLI). It can be used for communication and even on-line interaction with a running Storage Node. It allows you to query the current node status, find orphaned files, as well as to disable/enable Distributor Nodes on the fly.

The following sections give an overview of the core functionality of **Videbo** in the most common use cases.

## Running a Storage Node

### Creating a config file

After [installing](/#installation) Videbo, create a configuration file:
```shell
mkdir /etc/videbo
touch /etc/videbo/config.yaml
```
Videbo uses the [YAML](https://yaml.org/){.external-link target=_blank} format for all its configuration files. The default configuration file path for a Videbo node is `/etc/videbo/config.yaml`.

The minimum recommendation is to at least configure your storage directory. This is where all the video files will be uploaded, where the thumbnails will be save, etc.

By default, files will be stored in `/tmp/videbo`.

To specify your storage directory, open the configuration file and add the following:
```yaml title="config.yaml"
files_path: /path/to/video/storage
```

Make sure the directory exists and your user has write access to it.

### Starting the server

Now you can run the storage node:
```shell
python -m videbo storage
```

You should immediately see a few log messages appear in your terminal followed by a notice about where your server is listening:
```
...
INFO:videbo.storage.util:Found 0 videos in storage
...
======== Running on http://127.0.0.1:9020 ========
(Press CTRL+C to quit)
```

As you can see, by default, the server will listen for incoming requests on [http://127.0.0.1:9020](http://127.0.0.1:9020). If you want it to bind to a different port, say `8080`, you can do so via the command line:
```shell
python -m videbo -P 8080 storage
```

Alternatively, you can set the port in your config file:
```yaml title="config.yaml" hl_lines="2"
files_path: /path/to/video/storage
listen_port: 8080
```

!!! note
    Command line parameters will always take precedence over the configuration file.

All available configuration options are explained [here](#){.internal-link}.

Technically, this setup is all you need for **Videbo**.

### Connect a Moodle instance

To try it out with a local Moodle instance, you can install the [**`mod_videoservice`**](https://github.com/innocampus/moodle-mod_videoservice){.external-link target=_blank} there, then navigate to `Plugins` &rarr; `Activity modules` &rarr; `Video service` and adjust the video server URL in the settings:
```title="video_server_url"
http://localhost:8080
```

Since we changed the listen port, we will need to adjust the "public" facing URL of our Storage Node accordingly, so that Moodle can route our video requests properly:
```yaml title="config.yaml" hl_lines="3"
files_path: /path/to/video/storage
listen_port: 8080
public_base_url: http://localhost:8080
```

Then create a **`Video`** activity within a course. Inside that activity you can now upload, manage, stream, and download videos. The files should appear inside your `/path/to/video/storage` directory.

### Securing the Storage Node

In production, this setup would be completely insecure because we did not define a **shared secret** between our Moodle instance and **Videbo**. Without that, anyone could theoretically upload videos to a public facing Storage Node. We secure our **Videbo** Node in the config file:
```yaml title="config.yaml" hl_lines="4-5"
files_path: /path/to/video/storage
listen_port: 8080
public_base_url: http://localhost:8080
internal_api_secret: 'OUR_SUPER_SECURE_INTERNAL_SECRET'
external_api_secret: 'OUR_SUPER_SECURE_EXTERNAL_SECRET'
```

The `internal_api_secret` is what secures the node for administrative commands.

!!! note
    In YAML a string value can be safely enclosed in single quotes `'`, if it is made up of digits only (e.g. `123456`) or contains special characters or character sequences (e.g. `jC\n:6s[z`).

The `external_api_secret` is shared with our Moodle instance. We again set it in the `Video service` Plugin settings:
```title="video_api_secret"
OUR_SUPER_SECURE_EXTERNAL_SECRET
```

## Running a Distributor Node

### Configuring the nodes

The setup for a Distributor Node is essentially the same as with the Storage Node. Following installation we need to create a config file.

If we just want to have a local testing setup, we can spin up a Distributor Node on the same host as our Storage Node. To do that, we need to put the Distributor Node configuration in a separate file:
```yaml title="config.yaml (Distributor)"
files_path: /path/to/video/distributor
listen_port: 8081
public_base_url: http://localhost:8080
internal_api_secret: 'OUR_SUPER_SECURE_INTERNAL_SECRET'
external_api_secret: 'OUR_SUPER_SECURE_EXTERNAL_SECRET'
```

We already discussed why we configure the files path, listen port, and secrets in the [Storage section](#running-a-storage-node). We let our Distributor Node bind locally to port `8081` in this example.

The config parameter `public_base_url` must to be set to the **same value as on our Storage Node**. In this case, since we are creating a local test-setup, the url is `http://localhost:8080` as we configured it in the previous chapter.

!!! tip
    If we just want to have a local testing setup, we can spin up a Distributor Node on the same host as our Storage Node. To do that, we need to put the Distributor Node configuration in a separate file (say `/etc/videbo/config_dist.yaml`) and launch it with the `-c` command line option (see below).


Next, we need to configure our Storage Node to make it aware of the available Distributor:
```yaml title="config.yaml" hl_lines="6-7"
files_path: /path/to/video/storage
listen_port: 8080
public_base_url: http://localhost:8080
internal_api_secret: 'OUR_SUPER_SECURE_INTERNAL_SECRET'
external_api_secret: 'OUR_SUPER_SECURE_EXTERNAL_SECRET'
distribution:
  static_node_base_urls: http://localhost:8081
```
The `static_node_base_urls` parameter accepts either an array or a comma-separated list of Distributor Node base URLs. (If there is only one distributor node, a single URL is also valid.)

### Starting the Distributor Node

Once the setup is complete, we can start the Distributor Node:
```shell
python -m videbo distributor
```

Again, you should see some logs appear in the terminal and a notice about where the server is listening. **Only now**, once the the Distributor has started, should you (re-)start the Storage Node from a separate terminal:
```shell
python -m videbo storage
```

If the config files are located under different paths (e.g. because we want two nodes running on the same host), we can specify them using the `-c`/`--config-file-paths` CLI option:
```shell
python -m videbo -c ./config_dist.yaml distributor
```

```shell
python -m videbo -c ./config_storage.yaml storage
```

The Storage Node will immediately begin its periodical checks of the status of the enabled Distributor Nodes. In the log output of the Storage Node, you should see a message like this:
```
...
======== Running on http://127.0.0.1:9020 ========
...
INFO:videbo.storage.distribution:<Distribution watcher http://localhost:8081> connected. Free space currently: ... MB
INFO:videbo.storage.distribution:Found 0 files on http://localhost:8081
```

Whereas the Distributor will output messages about incoming status requests every few seconds. These are made by the Storage Node.

...
