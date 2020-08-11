# -*- coding: utf-8 -*-

"""This module contains a class for keeping track of the simulation components."""

import asyncio
import queue
import threading

import aio_pika

from tools.messages import AbstractMessage
from tools.tools import FullLogger, load_environmental_variables

LOGGER = FullLogger(__name__)


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

        self.__send_queue = queue.Queue()
        self.__send_thread = threading.Thread(
            name="sender_thread",
            target=RabbitmqClient.send_thread,
            daemon=True,
            kwargs={
                "connection_parameters": self.__connection_parameters,
                "exchange_name": self.__exchange_name,
                "send_queue": self.__send_queue
            })
        self.__send_thread.start()

        self.__listeners = {}

    def close(self):
        """Closes the sender thread and all the listener threads.
           Note: not really needed since all threads have been created with the daemon flag."""
        send_queue = getattr(self, "__send_queue", None)
        if send_queue:
            send_queue.put(None)

    @property
    def listeners(self):
        """Returns a dictionary containing the topics as keys and
           a list of (thread, callback class)-tuples as values."""
        return self.__listeners

    def add_listener(self, topic_name, callback_class):
        """Adds a new listener to the given topic."""
        new_listener_thread = threading.Thread(
            name="listen_thread_{:s}".format(topic_name),
            target=RabbitmqClient.listener_thread,
            daemon=True,
            kwargs={
                "connection_parameters": self.__connection_parameters,
                "exchange_name": self.__exchange_name,
                "topic_name": topic_name,
                "callback_class": callback_class
            })

        new_listener_thread.start()
        if topic_name not in self.__listeners:
            self.__listeners[topic_name] = []
        self.__listeners[topic_name].append((new_listener_thread, callback_class))

    # def remove_listener(self, topic_name):
    #     """Removes all listeners from the given topic."""

    def send_message(self, topic_name, message_bytes):
        """Sends the given message to the given topic. Assumes that the message is in bytes format."""
        self.__send_queue.put((topic_name, message_bytes))

    @classmethod
    def send_thread(cls, connection_parameters, exchange_name, send_queue):
        """The send thread loop that listens to the queue and sends the received messages to the message bus."""
        LOGGER.info("Opening sender thread for RabbitMQ client")
        asyncio.run(cls.start_send_connection(connection_parameters, exchange_name, send_queue))
        LOGGER.info("Closing sender thread for RabbitMQ client")

    @classmethod
    async def start_send_connection(cls, connection_parameters, exchange_name, send_queue):
        """The actual connection and queue listener function for the sender thread."""
        rabbitmq_connection = await aio_pika.connect_robust(**connection_parameters)

        async with rabbitmq_connection:
            rabbitmq_channel = await rabbitmq_connection.channel()
            rabbitmq_exchange = await rabbitmq_channel.declare_exchange(
                exchange_name, aio_pika.exchange.ExchangeType.TOPIC)

            while True:
                message_item = send_queue.get()
                if message_item is None:
                    break

                if len(message_item) == 2:
                    topic_name, message = message_item[0], message_item[1]
                    if isinstance(message, AbstractMessage):
                        message = message.bytes()
                    await rabbitmq_exchange.publish(aio_pika.Message(message), routing_key=topic_name)

                    LOGGER.debug("Message '{:s}' send to topic: '{:s}'".format(
                        message.decode(cls.MESSAGE_ENCODING), topic_name))
                else:
                    LOGGER.warning("Invalid message '{:s}' received in sender thread".format(message_item))

    @classmethod
    def listener_thread(cls, connection_parameters, exchange_name, topic_name, callback_class):
        """The listener thread loop that listens to the given topic in the message bus and
           sends the received messages to the send function of the given callback class."""
        LOGGER.info("Opening listener thread for RabbitMQ client for the topic '{:s}'".format(topic_name))
        asyncio.run(cls.start_listen_connection(connection_parameters, exchange_name, topic_name, callback_class))
        LOGGER.info("Closing listener thread for RabbitMQ client for the topic '{:s}'".format(topic_name))

    @classmethod
    async def start_listen_connection(cls, connection_parameters, exchange_name, topic_name, callback_class):
        """The actual connection and topic listener function for the listener thread."""
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

            # Binding the queue
            await rabbitmq_queue.bind(rabbitmq_exchange, routing_key=topic_name)
            LOGGER.info("Now listening to messages; exc={:s}, topic={:s}".format(exchange_name, topic_name))

            async with rabbitmq_queue.iterator() as queue_iter:
                async for message in queue_iter:
                    async with message.process():
                        LOGGER.debug("Message '{:s}' received from topic: '{:s}'".format(
                            message.body.decode(cls.MESSAGE_ENCODING), message.routing_key))
                        callback_class.callback(message)

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
