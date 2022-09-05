# Videbo

**Distributed video hosting for Moodle and other LMS**

---

**Documentation**: [tba](#)

**Source Code**: [https://github.com/innocampus/videbo](https://github.com/innocampus/videbo){.external-link target=_blank}

---

**Videbo** is a video hosting server specifically designed for [**Moodle**](https://moodle.org/) and other learning management systems (LMS).

It exposes an intuitive API for uploading, downloading, and streaming videos, which can be accessed via Moodle [**`mod_videoservice`**](https://github.com/innocampus/moodle-mod_videoservice){.external-link target=_blank} and similar plugins.

**Videbo** is written in Python, and it is powered by [`aiohttp`](https://docs.aiohttp.org/en/stable/index.html){.external-link target=_blank}, [`Pydantic`](https://pydantic-docs.helpmanual.io/){.external-link target=_blank}, and [`PyJWT`](https://pyjwt.readthedocs.io/en/latest/){.external-link target=_blank}.

## Key Features

* **Performant**: Asynchronous HTTP server that can easily handle large request loads.
* **Reliable**: Built-in distribution and load balancing capabilities. Dynamically enable/disable distributor nodes at any time.
* **Integrated**: Designed to be compatible with **`mod_videoservice`** out of the box.
* **Well-tested**: Used in production continuously since **2020** at the [**Technical University of Berlin**](https://www.tu.berlin/){.external-link target=_blank} (approx. 35k students) and during the peak of the remote-learning period.
* **Secure**: Authentication via industry standard JSON Web Tokens.

## Installation

**Videbo** is easily installed with `pip` like this:
```shell
pip install videbo
```

To install **Videbo** including all optional features:
```shell
pip install 'videbo[full]'
```

The latter includes monitoring capabilities for the [Prometheus Node Exporter](https://github.com/prometheus/node_exporter){.external-link target=_blank}.

## System dependencies

* Linux
* Python 3.9+
* `sudo`
* `ffmpeg`/`ffprobe`
