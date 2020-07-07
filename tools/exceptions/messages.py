# -*- coding: utf-8 -*-

"""This module contains the exception classes for the message errors."""

from tools.tools import get_logger

FILE_LOGGER = get_logger(__name__)


class MessageError(Exception):
    """Base class for message related errors."""
    def __init__(self, message):
        super(MessageError, self).__init__(message)
        FILE_LOGGER.error(message)
        self.message = message


class MessageTypeError(MessageError):
    """Exception class for errors related to invalid message types."""


class MessageDateError(MessageError):
    """Exception class for errors related to invalid datetimes."""


class MessageIdError(MessageError):
    """Exception class for errors related to invalid message ids."""


class MessageSourceError(MessageError):
    """Exception class for errors related to invalid sources."""


class MessageValueError(MessageError):
    """Exception class for errors related to invalid values in messages."""


class MessageEpochValueError(MessageValueError):
    """Exception class for errors related to invalid epoch values in messages."""


class MessageStateValueError(MessageValueError):
    """Exception class for errors related to invalid simulation state values in messages."""
