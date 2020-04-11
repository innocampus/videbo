import asyncio
import os


async def get_free_disk_space(path: str) -> int:
    """Get free disk space in the given path. Returns MB."""
    st = await asyncio.get_running_loop().run_in_executor(None, os.statvfs, path)
    free_bytes = st.f_bavail * st.f_frsize
    return int(free_bytes / 1024 / 1024)
