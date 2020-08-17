# -*- coding: utf-8 -*-

"""This module contains classes for the callbacks for the RabbitMQ message bus listeners."""

import inspect
import json
import threading

from tools.exceptions.messages import MessageError
from tools.messages import AbstractMessage, AbstractResultMessage, EpochMessage, SimulationStateMessage, \
                           StatusMessage, MESSAGE_TYPES, DEFAULT_MESSAGE_TYPE
from tools.tools import FullLogger

LOGGER = FullLogger(__name__)


class MessageCallback():
    """The callback class for handling received messages that are instances of AbstractMessage."""
    MESSAGE_CODING = "UTF-8"

    def __init__(self, callback_function, message_type=None):
        self.__lock = threading.Lock()
        self.__callback_function = callback_function

        if message_type is None:
            self.__message_type = DEFAULT_MESSAGE_TYPE
        else:
            self.__message_type = message_type

        self.__last_message = None
        self.__last_topic = None

    @property
    def last_message(self):
        """Returns the last message that was received."""
        return self.__last_message

    @property
    def last_topic(self):
        """Returns the topic from which the last message was received."""
        return self.__last_topic

    def log_last_message(self):
        """Writes a log message based on the last received message."""
        if isinstance(self.last_message, AbstractResultMessage):
            LOGGER.info("Received '{:s}' message from '{:s}' for epoch {:d}".format(
                self.last_message.message_type,
                self.last_message.source_process_id,
                self.last_message.epoch_number))
        elif isinstance(self.last_message, SimulationStateMessage):
            LOGGER.info("Received simulation state message '{:s}' from '{:s}'".format(
                self.last_message.simulation_state, self.last_message.source_process_id))
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
        elif isinstance(self.last_message, dict):
            LOGGER.info("Received a JSON message with errors: '{:s}'".format(json.loads(self.last_message)))
        elif self.last_message is None:
            LOGGER.warning("No last message found.")
        else:
            LOGGER.warning("The last message in unknown format: '{:s}'".format(str(self.last_message)))

    async def callback(self, message):
        """Callback function for the received messages from the message bus."""
        with self.__lock:
            try:
                message_str = message.body.decode(MessageCallback.MESSAGE_CODING)
                message_json = json.loads(message_str)

                # Convert the message to the specified special cases if possible.
                actual_message_type = message_json.get(
                    next(iter(AbstractMessage.MESSAGE_ATTRIBUTES)),  # the first defined attribute, should be "Type"
                    DEFAULT_MESSAGE_TYPE)
                if actual_message_type not in MESSAGE_TYPES:
                    actual_message_type = DEFAULT_MESSAGE_TYPE
                message_object = MESSAGE_TYPES[actual_message_type].from_json(message_json)

            except MessageError:
                # The message did not conform to the simulation platform message schema.
                message_object = message_json

            except json.decoder.JSONDecodeError:
                LOGGER.warning("Received message could not be decoded into JSON format.")
                message_object = message_str

            self.__last_message = message_object
            self.__last_topic = message.routing_key
            self.log_last_message()

            if inspect.iscoroutinefunction(self.__callback_function):
                await self.__callback_function(message_object, message.routing_key)
            else:
                LOGGER.error("Callback function '{:s}' is not awaitable.".format(str(type(self.__callback_function))))
