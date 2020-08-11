# -*- coding: utf-8 -*-

"""This module contains a listener simulation component that prints out all messages from the message bus."""

import queue
# import threading
import time

from tools.callbacks import GeneralMessageCallback
from tools.clients import RabbitmqClient
from tools.messages import AbstractMessage, SimulationStateMessage
from tools.tools import FullLogger, load_environmental_variables

LOGGER = FullLogger(__name__)

TIMEOUT_INTERVAL = 15

__SIMULATION_ID = "SIMULATION_ID"


class ListenerComponent:
    """Class for the message bus listener component."""
    def __init__(self, rabbitmq_client: RabbitmqClient, simulation_id: str, end_queue: queue.Queue):
        self.__rabbitmq_client = rabbitmq_client
        self.__simulation_id = simulation_id
        self.__end_queue = end_queue

        self.__rabbitmq_client.add_listeners("#", GeneralMessageCallback(self.simulation_message_handler))

    @property
    def simulation_id(self):
        """The simulation ID for the simulation."""
        return self.__simulation_id

    @property
    def simulation_topics(self):
        """The component name in the simulation."""
        return self.__simulation_topics

    async def simulation_message_handler(self, message_object, message_routing_key):
        """Handles the received simulation state messages."""
        if isinstance(message_object, AbstractMessage):
            if message_object.simulation_id != self.simulation_id:
                LOGGER.info(
                    "Received state message for a different simulation: '{:s}' instead of '{:s}'".format(
                        message_object.simulation_id, self.simulation_id))
            else:
                LOGGER.debug("{:s} : {:s}".format(message_routing_key, str(message_object.json())))

                # Check if the message is simulation ending message.
                if (isinstance(message_object, SimulationStateMessage) and
                        message_object.simulation_state == SimulationStateMessage.SIMULATION_STATES[-1]):
                    LOGGER.info("Listener stopping in {:d} seconds.".format(TIMEOUT_INTERVAL))
                    time.sleep(TIMEOUT_INTERVAL)
                    self.__end_queue.put(None)

        else:
            LOGGER.warning("Received '{:s}' message when expecting for '{:s}' message".format(
                str(type(message_object)), str(AbstractMessage)))


def start_listener_component():
    """Start a listener component for the simulation platform."""
    env_variables = load_environmental_variables(
        (__SIMULATION_ID, str)
    )

    message_client = RabbitmqClient()

    end_queue = queue.Queue()
    listener_component = ListenerComponent(
        message_client,
        env_variables[__SIMULATION_ID],
        end_queue)

    while True:
        end_item = end_queue.get()
        if end_item is None:
            message_client.close()
            break


if __name__ == "__main__":
    start_listener_component()
