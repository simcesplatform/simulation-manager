# -*- coding: utf-8 -*-

"""This module contains a listener simulation component that prints out all messages from the message bus."""

import asyncio

from tools.clients import RabbitmqClient
from tools.messages import AbstractMessage
from tools.tools import FullLogger, load_environmental_variables

LOGGER = FullLogger(__name__)

__SIMULATION_ID = "SIMULATION_ID"


class ListenerComponent:
    """Class for the message bus listener component."""
    LISTENED_TOPICS = "#"

    def __init__(self, rabbitmq_client: RabbitmqClient, simulation_id: str):
        self.__rabbitmq_client = rabbitmq_client
        self.__simulation_id = simulation_id

        self.__rabbitmq_client.add_listener(ListenerComponent.LISTENED_TOPICS, self.simulation_message_handler)

    @property
    def simulation_id(self):
        """The simulation ID for the simulation."""
        return self.__simulation_id

    async def simulation_message_handler(self, message_object, message_routing_key):
        """Handles the received simulation state messages."""
        if isinstance(message_object, AbstractMessage):
            if message_object.simulation_id != self.simulation_id:
                LOGGER.info(
                    "Received state message for a different simulation: '{:s}' instead of '{:s}'".format(
                        message_object.simulation_id, self.simulation_id))
            else:
                LOGGER.info("{:s} : {:s}".format(message_routing_key, str(message_object.json())))

        else:
            LOGGER.warning("Received '{:s}' message when expecting for '{:s}' message".format(
                str(type(message_object)), str(AbstractMessage)))


async def start_listener_component():
    """Start a listener component for the simulation platform."""
    env_variables = load_environmental_variables(
        (__SIMULATION_ID, str)
    )

    message_client = RabbitmqClient()
    ListenerComponent(message_client, env_variables[__SIMULATION_ID])

    while True:
        await asyncio.sleep(3600)


if __name__ == "__main__":
    asyncio.run(start_listener_component())
