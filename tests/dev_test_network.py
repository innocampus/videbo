#!/usr/bin/env python

"""
Simple script to test network interface information fetching.
"""

import asyncio
import logging
import sys

from videbo.network import NetworkInterfaces


async def main():
    ni = NetworkInterfaces.get_instance()
    log = logging.getLogger()
    log.setLevel(logging.DEBUG)
    log.addHandler(logging.StreamHandler(sys.stdout))
    ni.start_fetching()
    while True:
        print(f"Fetching: {ni.is_fetching}")
        print(repr(ni.get_first_interface() or "No interface"))
        await asyncio.sleep(3)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\rStopped.")

# Ignore the `print` calls in this script.
# ruff: noqa: T201
