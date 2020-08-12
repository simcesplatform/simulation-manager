# -*- coding: utf-8 -*-

"""This module contains a Timer class that can be used to set up events at some point in the future."""

# Based on https://stackoverflow.com/a/45430833

import asyncio


class Timer:
    """Timer class that sets up a timed task that can be cancelled."""
    def __init__(self, timeout, callback):
        self.__timeout = timeout
        self.__callback = callback
        self.__task = asyncio.ensure_future(self.__sleep_job())

    async def __sleep_job(self):
        """Sleeps and calls the callback function after the sleep."""
        await asyncio.sleep(self.__timeout)
        await self.__callback()

    def cancel(self):
        """Cancels the sleep job."""
        self.__task.cancel()
