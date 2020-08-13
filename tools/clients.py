# -*- coding: utf-8 -*-

"""This module contains a class for keeping track of the simulation components."""

import asyncio
import queue
import threading

import aio_pika

from tools.callbacks import MessageCallback
from tools.messages import AbstractMessage
from tools.tools import FullLogger, handle_async_exception, load_environmental_variables

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


def validate_message(topic_name, message_to_publish):
    """Validates the message received from a queue for publishing.
       Returns a tuple (topic_name: str, message_to_publish: bytes) if the message is valid.
       Otherwise, returns (None, None)."""
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

        self.__listeners = {}

    def close(self):
        """Closes the sender thread and all the listener threads.
           Note: not really needed since all threads have been created with the daemon flag."""
        pass

    @property
    def listeners(self):
        """Returns a dictionary containing the topic names as keys and
           a list of (thread, callback function)-tuples as values."""
        return self.__listeners

    def add_listener(self, topic_names, callback_function):
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
                "callback_class": MessageCallback(callback_function)
            })

        new_listener_thread.start()
        for topic_name in topic_names:
            if topic_name not in self.__listeners:
                self.__listeners[topic_name] = []
            self.__listeners[topic_name].append((new_listener_thread, callback_function))

    # def remove_listener(self, topic_name):
    #     """Removes all listeners from the given topic."""

    async def send_message(self, topic_name, message_bytes):
        """Sends the given message to the given topic. Assumes that the message is in bytes format."""
        # self.__send_queue.put((topic_name, message_bytes))
        publish_message_task = asyncio.create_task(self.send_message_task(topic_name, message_bytes))
        await asyncio.wait([publish_message_task])

    async def send_message_task(self, topic_name, message_to_publish):
        """Publishes the given message to the given topic."""
        topic_name, message_to_publish = validate_message(topic_name, message_to_publish)
        if topic_name is None or message_to_publish is None:
            return

        try:
            asyncio.get_running_loop().set_exception_handler(handle_async_exception)
            rabbitmq_connection = await aio_pika.connect_robust(**self.__connection_parameters)

            rabbitmq_channel = await rabbitmq_connection.channel()
            rabbitmq_exchange = await rabbitmq_channel.declare_exchange(
                self.__exchange_name, aio_pika.exchange.ExchangeType.TOPIC)

            await rabbitmq_exchange.publish(aio_pika.Message(message_to_publish), routing_key=topic_name)
            LOGGER.debug("Message '{:s}' send to topic: '{:s}'".format(
                message_to_publish.decode(RabbitmqClient.MESSAGE_ENCODING), topic_name))

        except Exception as error:
            LOGGER.warning("Error: '{:s}' when trying to publish message.".format(str(error)))

    @classmethod
    def listener_thread(cls, connection_parameters, exchange_name, topic_names, callback_class):
        """The listener thread loop that listens to the given topic in the message bus and
           sends the received messages to the callback() function of the given callback class."""
        LOGGER.info("Opening listener thread for RabbitMQ client for the topics '{:s}'".format(", ".join(topic_names)))
        asyncio.run(cls.start_listen_connection(
            connection_parameters=connection_parameters,
            exchange_name=exchange_name,
            topic_names=topic_names,
            callback_class=callback_class))
        LOGGER.info("Closing listener thread for RabbitMQ client for the topics '{:s}'".format(", ".join(topic_names)))

    @classmethod
    async def start_listen_connection(cls, connection_parameters, exchange_name, topic_names, callback_class):
        """The actual connection and topic listener function for the listener thread."""
        if isinstance(topic_names, str):
            topic_names = [topic_names]

        asyncio.get_running_loop().set_exception_handler(handle_async_exception)
        while True:
            try:
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
                        LOGGER.info("Now listening to messages; exc={:s}, topic={:s}".format(
                            exchange_name, topic_name))

                    async with rabbitmq_queue.iterator() as queue_iter:
                        async for message in queue_iter:
                            async with message.process():
                                LOGGER.debug("Message '{:s}' received from topic: '{:s}'".format(
                                    message.body.decode(cls.MESSAGE_ENCODING), message.routing_key))
                                await callback_class.callback(message)

            except Exception as error:
                LOGGER.warning("Error: '{:s}' when trying to listen to the message bus.".format(str(error)))

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
