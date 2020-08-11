# -*- coding: utf-8 -*-

"""This module contains classes for the callbacks for the RabbitMQ message bus listeners."""

import json
import threading

from tools.messages import AbstractMessage, AbstractResultMessage, EpochMessage, \
                           ResultMessage, SimulationStateMessage, StatusMessage
from tools.tools import FullLogger

LOGGER = FullLogger(__name__)


class AbstractMessageCallback:
    """The abstract callback class for handling messages in AbstractMessage format."""
    MESSAGE_CODING = "UTF-8"
    DEFAULT_MESSAGE_TYPE = AbstractMessage

    def __init__(self, callback_function, message_type=None):
        self.__lock = threading.Lock()
        self.__callback_function = callback_function

        if message_type is None:
            self.__message_type = self.__class__.DEFAULT_MESSAGE_TYPE
        else:
            self.__message_type = message_type
        self.__last_message = None

    @property
    def last_message(self):
        """Returns the last message that was received."""
        return self.__last_message

    def callback(self, message):
        """Callback function for the received messages."""
        with self.__lock:
            message_str = message.body.decode(AbstractMessageCallback.MESSAGE_CODING)
            message_json = json.loads(message_str)
            message_object = self.__message_type.from_json(message_json)

            self.__last_message = message_object
            self.__callback_function(message_object, message.routing_key)

            self.log_last_message()

    def log_last_message(self):
        """Writes a log message based on the last received message."""
        if isinstance(self.__last_message, (AbstractResultMessage, ResultMessage)):
            LOGGER.info("Received '{:s}' message from '{:s}' for epoch {:d}".format(
                self.__last_message.message_type,
                self.__last_message.source_process_id,
                self.__last_message.epoch_number))
        elif isinstance(self.__last_message, SimulationStateMessage):
            LOGGER.info("Received simulation state message '{:s}' from '{:s}'".format(
                self.__last_message.simulation_state, self.__last_message.source_process_id))
        elif isinstance(self.__last_message, EpochMessage):
            LOGGER.info("Epoch message received from '{:s}' for epoch number {:d} ({:s} - {:s})".format(
                self.__last_message.source_process_id,
                self.__last_message.epoch_number,
                self.__last_message.start_time,
                self.__last_message.end_time))
        elif isinstance(self.__last_message, StatusMessage):
            LOGGER.info("Status message received from '{:s}' for epoch number {:d}".format(
                self.__last_message.source_process_id,
                self.__last_message.epoch_number))
        elif isinstance(self.__last_message, AbstractMessage):
            LOGGER.info("Received '{:s}' message from '{:s}'".format(
                self.__last_message.message_type,
                self.__last_message.source_process_id))
        elif self.__last_message is None:
            LOGGER.warning("No last message found.")
        else:
            LOGGER.warning("The last message is of unknown type: {:s}".format(type(self.__last_message)))


class SimulationStateMessageCallback(AbstractMessageCallback):
    """The callback class for handling messages in AbstractResultMessage format."""
    DEFAULT_MESSAGE_TYPE = SimulationStateMessage


class AbstractResultMessageCallback(AbstractMessageCallback):
    """The callback class for handling messages in AbstractResultMessage format."""
    DEFAULT_MESSAGE_TYPE = AbstractResultMessage


class EpochMessageCallback(AbstractResultMessageCallback):
    """The callback class for handling messages in EpochMessage format."""
    DEFAULT_MESSAGE_TYPE = EpochMessage


class StatusMessageCallback(AbstractResultMessageCallback):
    """The callback class for handling messages in StatusMessage format."""
    DEFAULT_MESSAGE_TYPE = StatusMessage
