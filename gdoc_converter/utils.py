import asyncio
import logging
import re
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)

def normalize_filename(name: str) -> str:
    """
    Normalize filename: remove special characters and spaces.
    """
    name = name.strip().lower().replace(" ", "_")
    normalized = "".join([c for c in name if c.isalnum() or c in ('_', '-')])
    logger.debug(f"Normalized filename: {name} -> {normalized}")
    return normalized


def run_async_in_thread(async_func, *args):
    """
    Runs an async function.
    If an event loop is already running (e.g., in Jupyter), runs it in a new thread.
    Otherwise, runs it using asyncio.run().
    """
    try:
        loop = asyncio.get_running_loop()
        # If loop exists, run in a separate thread to avoid blocking
        with ThreadPoolExecutor() as executor:
            future = executor.submit(asyncio.run, async_func(*args))
            return future.result()
    except RuntimeError:
        # No event loop is running, we can safely use asyncio.run()
        return asyncio.run(async_func(*args))

def extract_gdoc_id_from_url(url: str) -> str:
    """
    Extract Google Doc ID from URL.
    """
    if "/d/" in url:
        start = url.index("/d/") + 3
        end = url.find("/", start)
        if end == -1:
            end = url.find("?", start)
        if end == -1:
            end = len(url)
        return url[start:end]
    # If it's already an ID
    if len(url) == 44 and not url.startswith("http"):
        return url
    logger.warning(f"Could not extract ID from URL: {url}, using as is.")
    return url