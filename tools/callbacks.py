# -*- coding: utf-8 -*-

"""This module contains classes for the callbacks for the RabbitMQ message bus listeners."""

import json
import threading

from tools.messages import AbstractMessage, AbstractResultMessage, EpochMessage, GeneralMessage, \
                           ResultMessage, SimulationStateMessage, StatusMessage
from tools.messages import MESSAGE_TYPES as ALL_MESSAGE_TYPES, \
                           DEFAULT_MESSAGE_TYPE as GENERAL_DEFAULT_MESSAGE_TYPE
from tools.tools import FullLogger

LOGGER = FullLogger(__name__)


class AbstractMessageCallback:
    """The abstract callback class for handling messages in AbstractMessage format."""
    MESSAGE_CODING = "UTF-8"
    DEFAULT_MESSAGE_TYPE = AbstractMessage

    def __init__(self, callback_function, message_type=None):
        self._lock = threading.Lock()
        self._callback_function = callback_function

        if message_type is None:
            self._message_type = self.__class__.DEFAULT_MESSAGE_TYPE
        else:
            self._message_type = message_type

        self._last_message = None
        self._last_topic = None

    @property
    def last_message(self):
        """Returns the last message that was received."""
        return self._last_message

    @property
    def last_topic(self):
        """Returns the topic from which the last message was received."""
        return self._last_topic

    def callback(self, message):
        """Callback function for the received messages."""
        with self._lock:
            message_str = message.body.decode(AbstractMessageCallback.MESSAGE_CODING)
            message_json = json.loads(message_str)
            message_object = self._message_type.from_json(message_json)

            self.last_message = message_object
            self.last_topic = message.routing_key
            self._callback_function(message_object, message.routing_key)

            self.log_last_message()

    def log_last_message(self):
        """Writes a log message based on the last received message."""
        if isinstance(self.last_message, (AbstractResultMessage, ResultMessage)):
            LOGGER.info("Received '{:s}' message from '{:s}' for epoch {:d}".format(
                self.last_message.message_type,
                self.last_message.source_process_id,
                self.last_message.epoch_number))
        elif isinstance(self.last_message, SimulationStateMessage):
            LOGGER.info("Received simulation state message '{:s}' from '{:s}'".format(
                self.last_message.simulation_state, self._last_message.source_process_id))
        elif isinstance(self.last_message, EpochMessage):
            LOGGER.info("Epoch message received from '{:s}' for epoch number {:d} ({:s} - {:s})".format(
                self.last_message.source_process_id,
                self.last_message.epoch_number,
                self.last_message.start_time,
                self.last_message.end_time))
        elif isinstance(self.last_message, StatusMessage):
            LOGGER.info("Status message received from '{:s}' for epoch number {:d}".format(
                self.last_message.source_process_id,
                self.last_message.epoch_number))
        elif isinstance(self.last_message, AbstractMessage):
            LOGGER.info("Received '{:s}' message from '{:s}' on topic '{:s}'".format(
                self.last_message.message_type,
                self.last_message.source_process_id,
                self.last_topic))
        elif self.last_message is None:
            LOGGER.warning("No last message found.")
        else:
            LOGGER.warning("The last message is of unknown type: {:s}".format(type(self.last_message)))


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


class ResultMessageCallback(AbstractResultMessageCallback):
    """The callback class for handling messages in ResultMessage format."""
    DEFAULT_MESSAGE_TYPE = ResultMessage


class GeneralMessageCallback(AbstractMessageCallback):
    """The callback class for handling messages in GeneralMessage format."""
    DEFAULT_MESSAGE_TYPE = GeneralMessage

    # def __init__(self, callback_function, message_type=None):
    #     super().__init__(callback_function, message_type)
    #     self.__lock = threading.Lock()

    def callback(self, message):
        """General allback function for the received messages."""
        with self._lock:
            message_str = message.body.decode(AbstractMessageCallback.MESSAGE_CODING)
            message_json = json.loads(message_str)

            # Convert the message to the specified special cases if possible.
            actual_message_type = message_json.get(
                next(iter(AbstractMessage.MESSAGE_ATTRIBUTES)),  # first attribute in the dict, i.e. "Type"
                GENERAL_DEFAULT_MESSAGE_TYPE)
            if actual_message_type not in ALL_MESSAGE_TYPES:
                actual_message_type = GENERAL_DEFAULT_MESSAGE_TYPE
            message_object = ALL_MESSAGE_TYPES[actual_message_type].from_json(message_json)

            self._last_message = message_object
            self._last_topic = message.routing_key
            self._callback_function(message_object, message.routing_key)

            self.log_last_message()
