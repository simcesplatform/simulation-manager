# -*- coding: utf-8 -*-

"""This module contains a class for keeping track of the simulation components."""

import asyncio
import queue
import threading
import time

import aio_pika

from tools.messages import AbstractMessage
from tools.tools import FullLogger, load_environmental_variables

LOGGER = FullLogger(__name__)

LONG_TIMEOUT_INTERVAL = 60  # wait this long for a new message to before printing debug output
SHORT_TIMEOUT_INTERVAL = 30  # wait this long for a new message to be sent while having open connection


def default_env_variable_definitions():
    """Returns the default environment variable definitions."""
    def env_variable_name(simple_variable_name):
        return "{:s}{:s}".format(RabbitmqClient.DEFAULT_ENV_VARIABLE_PREFIX, simple_variable_name.upper())

    return [
        (env_variable_name("host"), str, "localhost"),
        (env_variable_name("port"), int, 5672),
        (env_variable_name("login"), str, ""),
        (env_variable_name("password"), str, ""),
        (env_variable_name("ssl"), bool, False),
        (env_variable_name("ssl_version"), str, "PROTOCOL_TLS"),
        (env_variable_name("exchange"), str, "")
    ]


def load_config_from_env_variables():
    """Returns configuration dictionary from which values are fetched from environmental variables."""
    def simple_name(env_variable_name):
        return env_variable_name[len(RabbitmqClient.DEFAULT_ENV_VARIABLE_PREFIX):].lower()

    env_variables = load_environmental_variables(*default_env_variable_definitions())

    return {
        simple_name(variable_name): env_variables[variable_name]
        for variable_name in env_variables
    }


def validate_message(topic_name, message_to_publish):
    """Validates the message received from a queue for publishing.
       Returns a tuple (topic_name: str, message_to_publish: bytes) if the message is valid.
       Otherwise, returns (None, None)."""
    # if not isinstance(queue_message, (list, tuple)):
    #     LOGGER.warning("Wrong message type ('{:s})' sent to publish queue.".format(str(type(message_item))))
    #     return None

    # if len(queue_message) != 2:
    #     LOGGER.warning("Incompatible list (length: {:d} != 2) sent to publish queue.".format(len(queue_message)))
    #     return None

    # topic_name, message_to_publish = queue_message

    if not isinstance(topic_name, str):
        topic_name = str(topic_name)
    if isinstance(message_to_publish, AbstractMessage):
        message_to_publish = message_to_publish.bytes()

    if topic_name == "":
        LOGGER.warning("Topic name for the message to publish was empty.")
        return None, None

    if not isinstance(message_to_publish, bytes):
        LOGGER.warning("Wrong message type ('{:s}') for publishing.".format(str(type(message_to_publish))))
        return None, None

    return topic_name, message_to_publish


class RabbitmqClient:
    """RabbitMQ client that can be used to send messages and to create topic listeners."""
    DEFAULT_ENV_VARIABLE_PREFIX = "RABBITMQ_"
    CONNECTION_PARAMTERS = ["host", "port", "login", "password", "ssl"]
    OPTIONAL_SSL_PARAMETER_TOP = "ssl_options"
    OPTIONAL_SSL_PARAMETER = "ssl_version"

    MESSAGE_ENCODING = "UTF-8"

    def __init__(self, **kwargs):
        if not kwargs:
            kwargs = load_config_from_env_variables()
        self.__connection_parameters = RabbitmqClient.__get_connection_parameters_only(kwargs)
        self.__exchange_name = kwargs["exchange"]

        # self.__send_queue = queue.Queue()
        # self.__send_thread = threading.Thread(
        #     name="sender_thread",
        #     target=RabbitmqClient.send_thread,
        #     daemon=True,
        #     kwargs={
        #         "connection_parameters": self.__connection_parameters,
        #         "exchange_name": self.__exchange_name,
        #         "send_queue": self.__send_queue
        #     })
        # self.__send_thread.start()

        self.__listeners = {}

    def close(self):
        """Closes the sender thread and all the listener threads.
           Note: not really needed since all threads have been created with the daemon flag."""
        pass

    @property
    def listeners(self):
        """Returns a dictionary containing the topics as keys and
           a list of (thread, callback class)-tuples as values."""
        return self.__listeners

    def add_listeners(self, topic_names, callback_class):
        """Adds a new listener to the given topic."""
        if isinstance(topic_names, str):
            topic_names = [topic_names]

        new_listener_thread = threading.Thread(
            name="listen_thread_{:s}".format(",".join(topic_names)),
            target=RabbitmqClient.listener_thread,
            daemon=True,
            kwargs={
                "connection_parameters": self.__connection_parameters,
                "exchange_name": self.__exchange_name,
                "topic_names": topic_names,
                "callback_class": callback_class
            })

        new_listener_thread.start()
        for topic_name in topic_names:
            if topic_name not in self.__listeners:
                self.__listeners[topic_name] = []
            self.__listeners[topic_name].append((new_listener_thread, callback_class))

    # def remove_listener(self, topic_name):
    #     """Removes all listeners from the given topic."""

    async def send_message(self, topic_name, message_bytes):
        """Sends the given message to the given topic. Assumes that the message is in bytes format."""
        # self.__send_queue.put((topic_name, message_bytes))
        publish_message_task = asyncio.create_task(self.send_task(topic_name, message_bytes))
        await asyncio.wait([publish_message_task])

    # @classmethod
    # def send_thread(cls, connection_parameters, exchange_name, send_queue):
        # """The send thread loop that listens to the queue and sends the received messages to the message bus."""
        # LOGGER.info("Starting sender thread for RabbitMQ client")
        # asyncio.run(cls.start_send_connection(connection_parameters, exchange_name, send_queue))

    async def send_task(self, topic_name, message_to_publish):
        """Publishes the given message to the given topic."""
        topic_name, message_to_publish = validate_message(topic_name, message_to_publish)
        if topic_name is None or message_to_publish is None:
            return

        try:
            rabbitmq_connection = await aio_pika.connect_robust(**self.__connection_parameters)

            rabbitmq_channel = await rabbitmq_connection.channel()
            rabbitmq_exchange = await rabbitmq_channel.declare_exchange(
                self.__exchange_name, aio_pika.exchange.ExchangeType.TOPIC)

            await rabbitmq_exchange.publish(aio_pika.Message(message_to_publish), routing_key=topic_name)
            LOGGER.debug("Message '{:s}' send to topic: '{:s}'".format(
                message_to_publish.decode(RabbitmqClient.MESSAGE_ENCODING), topic_name))

        except Exception as error:
            LOGGER.warning("Error: '{:s}' when trying to publish message.".format(str(error)))

    # @classmethod
    # async def start_send_connection(cls, connection_parameters, exchange_name, send_queue):
    #     """The actual connection and queue listener function for the sender thread."""

    #     program_is_alive = True
    #     message_sent_connection_counter = 0
    #     last_publish_time = time.time()

    #     while program_is_alive:
    #         # Wait for the first message to send before starting a connection.
    #         try:
    #             message_item = send_queue.get(timeout=LONG_TIMEOUT_INTERVAL)
    #             if message_item is None:
    #                 break
    #         except queue.Empty:
    #             LOGGER.debug("{:.1f} seconds without any messages to publish".format(time.time() - last_publish_time))
    #             continue

    #         validated_message_item = validate_queue_message(message_item)
    #         if validated_message_item is None:
    #             continue
    #         topic_name, message_to_publish = validated_message_item

    #         try:
    #             rabbitmq_connection = await aio_pika.connect_robust(**connection_parameters)
    #             message_sent_connection_counter += 1

    #             async with rabbitmq_connection:
    #                 rabbitmq_channel = await rabbitmq_connection.channel()
    #                 rabbitmq_exchange = await rabbitmq_channel.declare_exchange(
    #                     exchange_name, aio_pika.exchange.ExchangeType.TOPIC)

    #                 while True:
    #                     LOGGER.debug("{:d} {:s} {:s}".format(message_sent_connection_counter, topic_name, str(message_to_publish)))
    #                     await rabbitmq_exchange.publish(
    #                         aio_pika.Message(message_to_publish),
    #                         routing_key=topic_name)
    #                     last_publish_time = time.time()
    #                     LOGGER.debug("Message '{:s}' send to topic: '{:s}' ({:d})".format(
    #                         message_to_publish.decode(cls.MESSAGE_ENCODING),
    #                         topic_name,
    #                         message_sent_connection_counter))

    #                     try:
    #                         LOGGER.debug("{:.1f}".format(max(SHORT_TIMEOUT_INTERVAL - (time.time() - last_publish_time), 1)))
    #                         message_item = send_queue.get(
    #                             timeout=max(SHORT_TIMEOUT_INTERVAL - (time.time() - last_publish_time), 1))
    #                         if message_item is None:
    #                             program_is_alive = False
    #                             break
    #                     except queue.Empty:
    #                         break

    #                     validated_message_item = validate_queue_message(message_item)
    #                     if validated_message_item is None:
    #                         break
    #                     topic_name, message_to_publish = validated_message_item

    #         except Exception as error:
    #             LOGGER.warning("Error: '{:s}' when trying to publish message.".format(str(error)))

    @classmethod
    def listener_thread(cls, connection_parameters, exchange_name, topic_names, callback_class):
        """The listener thread loop that listens to the given topic in the message bus and
           sends the received messages to the send function of the given callback class."""
        LOGGER.info("Opening listener thread for RabbitMQ client for the topics '{:s}'".format(", ".join(topic_names)))
        asyncio.run(cls.start_listen_connection(connection_parameters, exchange_name, topic_names, callback_class))
        LOGGER.info("Closing listener thread for RabbitMQ client for the topics '{:s}'".format(", ".join(topic_names)))

    @classmethod
    async def start_listen_connection(cls, connection_parameters, exchange_name, topic_names, callback_class):
        """The actual connection and topic listener function for the listener thread."""
        if isinstance(topic_names, str):
            topic_names = [topic_names]

        rabbitmq_connection = await aio_pika.connect_robust(**connection_parameters)

        async with rabbitmq_connection:
            rabbitmq_channel = await rabbitmq_connection.channel()
            rabbitmq_exchange = await rabbitmq_channel.declare_exchange(
                exchange_name, aio_pika.exchange.ExchangeType.TOPIC)

            # Declaring a queue; type: aio_pika.Queue
            # No name provided -> the server generates a random name
            rabbitmq_queue = await rabbitmq_channel.declare_queue(
                auto_delete=True,  # Delete the queue when no one uses it anymore
                exclusive=True     # No other application can access the queue; delete on exit
            )

            # Binding the queue to the given topics
            for topic_name in topic_names:
                await rabbitmq_queue.bind(rabbitmq_exchange, routing_key=topic_name)
                LOGGER.info("Now listening to messages; exc={:s}, topic={:s}".format(exchange_name, topic_name))

            async with rabbitmq_queue.iterator() as queue_iter:
                async for message in queue_iter:
                    async with message.process():
                        LOGGER.debug("Message '{:s}' received from topic: '{:s}'".format(
                            message.body.decode(cls.MESSAGE_ENCODING), message.routing_key))
                        await callback_class.callback(message)

    @classmethod
    def __get_connection_parameters_only(cls, connection_config_dict):
        """Returns only the parameters needed for creating a connection."""
        stripped_connection_config = {
            config_parameter: parameter_value
            for config_parameter, parameter_value in connection_config_dict.items()
            if config_parameter in cls.CONNECTION_PARAMTERS
        }
        if stripped_connection_config["ssl"]:
            stripped_connection_config[cls.OPTIONAL_SSL_PARAMETER_TOP] = {
                cls.OPTIONAL_SSL_PARAMETER: connection_config_dict[cls.OPTIONAL_SSL_PARAMETER]
            }

        return stripped_connection_config
