# Videbo

**Distributed video hosting for Moodle and other LMS**

---

**Documentation**: <a href="https://innocampus.github.io/videbo" target="_blank"> https://innocampus.github.io/videbo </a>

**Source Code**: <a href="https://github.com/innocampus/videbo" target="_blank"> https://github.com/innocampus/videbo </a>

---

**Videbo** is a video hosting server specifically designed for <a href="https://moodle.org" target="_blank" class="external-link">**Moodle**</a> and other learning management systems (LMS).

It exposes an intuitive API for uploading, downloading, and streaming videos, which can be accessed via Moodle <a href="https://github.com/innocampus/moodle-mod_videoservice" target="_blank" class="external-link">**`mod_videoservice`**</a> and similar plugins.

**Videbo** is written in Python, and it is powered by <a href="https://docs.aiohttp.org/en/stable/index.html" target="_blank" class="external-link">`aiohttp`</a>, <a href="https://pydantic-docs.helpmanual.io" target="_blank" class="external-link">`Pydantic`</a>, and <a href="https://pyjwt.readthedocs.io/en/latest" target="_blank" class="external-link">`PyJWT`</a>.

## Key Features

* **Performant**: Asynchronous HTTP server that can easily handle large request loads.
* **Reliable**: Built-in distribution and load balancing capabilities. Dynamically enable/disable distributor nodes at any time.
* **Integrated**: Designed to be compatible with **`mod_videoservice`** out of the box.
* **Well-tested**: Used in production continuously since **2020** at the <a href="https://www.tu.berlin" target="_blank" class="external-link">**Technical University of Berlin**</a> (approx. 35k students) and during the peak of the remote-learning period.
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

The latter includes monitoring capabilities for the <a href="https://github.com/prometheus/node_exporter" target="_blank" class="external-link">Prometheus Node Exporter</a>.

## System dependencies

* Linux
* Python 3.9+
* `sudo`
* `ffmpeg`/`ffprobe`
