# -*- coding: utf-8 -*-

"""This module contains a dummy simulation component that has very simple internal logic."""

import queue
import random
import threading
import time

from tools.callbacks import GeneralMessageCallback
from tools.clients import RabbitmqClient
from tools.messages import EpochMessage, StatusMessage, SimulationStateMessage, get_next_message_id
from tools.tools import FullLogger, load_environmental_variables

LOGGER = FullLogger(__name__)

TIMEOUT_INTERVAL = 15

__SIMULATION_ID = "SIMULATION_ID"
__SIMULATION_COMPONENT_NAME = "SIMULATION_COMPONENT_NAME"
__SIMULATION_EPOCH_MESSAGE_TOPIC = "SIMULATION_EPOCH_MESSAGE_TOPIC"
__SIMULATION_STATUS_MESSAGE_TOPIC = "SIMULATION_STATUS_MESSAGE_TOPIC"
__SIMULATION_STATE_MESSAGE_TOPIC = "SIMULATION_STATE_MESSAGE_TOPIC"

__MIN_SLEEP_TIME = "MIN_SLEEP_TIME"
__MAX_SLEEP_TIME = "MAX_SLEEP_TIME"


class DummyComponent:
    """Class for holding the state of a dummy simulation component."""
    SIMULATION_STATE_VALUE_RUNNING = SimulationStateMessage.SIMULATION_STATES[0]   # "running"
    SIMULATION_STATE_VALUE_STOPPED = SimulationStateMessage.SIMULATION_STATES[-1]  # "stopped"

    def __init__(self, rabbitmq_client, simulation_id, component_name,
                 simulation_state_topic, epoch_topic, status_topic,
                 min_delay, max_delay, end_queue):
        self.__rabbitmq_client = rabbitmq_client
        self.__simulation_id = simulation_id
        self.__component_name = component_name

        self.__simulation_state_topic = simulation_state_topic
        self.__epoch_topic = epoch_topic
        self.__status_topic = status_topic

        self.__min_delay = min_delay
        self.__max_delay = max_delay

        self.__simulation_state = DummyComponent.SIMULATION_STATE_VALUE_STOPPED
        self.__latest_epoch = 0

        self.__end_queue = end_queue
        self.__message_id_generator = get_next_message_id(component_name)

        self.__rabbitmq_client.add_listeners(
            [
                self.__simulation_state_topic,
                self.__epoch_topic
            ],
            GeneralMessageCallback(self.general_message_handler))

    @property
    def simulation_id(self):
        """The simulation ID for the simulation."""
        return self.__simulation_id

    @property
    def component_name(self):
        """The component name in the simulation."""
        return self.__component_name

    @property
    def simulation_state(self):
        """Return the simulation state attribute."""
        return self.__simulation_state

    @simulation_state.setter
    def simulation_state(self, new_simulation_state):
        if new_simulation_state in SimulationStateMessage.SIMULATION_STATES:
            self.__simulation_state = new_simulation_state

            if new_simulation_state == DummyComponent.SIMULATION_STATE_VALUE_RUNNING:
                if self.__latest_epoch == 0:
                    self.__send_new_status_message()

            elif new_simulation_state == DummyComponent.SIMULATION_STATE_VALUE_STOPPED:
                LOGGER.info("Component {:s} stopping in {:d} seconds.".format(
                    self.__component_name, TIMEOUT_INTERVAL))
                time.sleep(TIMEOUT_INTERVAL)
                self.__end_queue.put(None)

    def start_epoch(self, epoch_number):
        """Starts a new epoch for the component. Sends a status message when finished."""
        if self.__simulation_state == DummyComponent.SIMULATION_STATE_VALUE_RUNNING:
            self.__latest_epoch = epoch_number

            rand_wait_time = random.randint(self.__min_delay, self.__max_delay)
            LOGGER.info("Component {:s} sending status message for epoch {:d} in {:d} seconds.".format(
                self.__component_name, self.__latest_epoch, rand_wait_time))
            time.sleep(rand_wait_time)
            self.__send_new_status_message()

    def general_message_handler(self, message_object, message_routing_key):
        """Forwards the message handling to the appropriate function depending on the message type."""
        if isinstance(message_object, SimulationStateMessage):
            self.simulation_state_message_handler(message_object, message_routing_key)
        elif isinstance(message_object, EpochMessage):
            self.epoch_message_handler(message_object, message_routing_key)
        else:
            LOGGER.warning("Received '{:s}' message when expecting for '{:s}' or '{:s}' message".format(
                str(type(message_object)), str(SimulationStateMessage), str(EpochMessage)))

    def simulation_state_message_handler(self, message_object, message_routing_key):
        """Handles the received simulation state messages."""
        if message_object.simulation_id != self.simulation_id:
            LOGGER.info(
                "Received state message for a different simulation: '{:s}' instead of '{:s}'".format(
                    message_object.simulation_id, self.simulation_id))
        elif message_object.message_type != SimulationStateMessage.CLASS_MESSAGE_TYPE:
            LOGGER.info(
                "Received a state message with wrong message type: '{:s}' instead of '{:s}'".format(
                    message_object.message_type, SimulationStateMessage.CLASS_MESSAGE_TYPE))
        else:
            LOGGER.debug("Received a state message from {:s} on topic {:s}".format(
                message_object.source_process_id, message_routing_key))
            self.simulation_state = message_object.simulation_state

    def epoch_message_handler(self, message_object, message_routing_key):
        """Handles the received epoch messages."""
        if message_object.simulation_id != self.simulation_id:
            LOGGER.info(
                "Received epoch message for a different simulation: '{:s}' instead of '{:s}'".format(
                    message_object.simulation_id, self.simulation_id))
        elif message_object.message_type != EpochMessage.CLASS_MESSAGE_TYPE:
            LOGGER.info(
                "Received a epoch message with wrong message type: '{:s}' instead of '{:s}'".format(
                    message_object.message_type, EpochMessage.CLASS_MESSAGE_TYPE))
        else:
            LOGGER.debug("Received an epoch from {:s} on topic {:s}".format(
                message_object.source_process_id, message_routing_key))
            self.start_epoch(message_object.epoch_number)

    def __send_new_status_message(self):
        new_status_message = self.__get_status_message()
        self.__rabbitmq_client.send_message(self.__status_topic, new_status_message)

    def __get_status_message(self):
        status_message = StatusMessage(**{
            "Type": StatusMessage.CLASS_MESSAGE_TYPE,
            "SimulationId": self.simulation_id,
            "SourceProcessId": self.component_name,
            "MessageId": next(self.__message_id_generator),
            "EpochNumber": self.__latest_epoch,
            "TriggeringMessageIds": ["placeholder"],
            "Value": StatusMessage.STATUS_VALUES[0]
        })
        if status_message is None:
            LOGGER.error("Problem with creating a status message")

        return status_message.bytes()


def start_dummy_component():
    """Start a dummy component for the simulation platform."""
    env_variables = load_environmental_variables(
        (__SIMULATION_ID, str),
        (__SIMULATION_COMPONENT_NAME, str, "dummy"),
        (__SIMULATION_EPOCH_MESSAGE_TOPIC, str, "epoch"),
        (__SIMULATION_STATUS_MESSAGE_TOPIC, str, "status"),
        (__SIMULATION_STATE_MESSAGE_TOPIC, str, "state"),
        (__MIN_SLEEP_TIME, int, 2),
        (__MAX_SLEEP_TIME, int, 15)
    )

    message_client = RabbitmqClient()

    end_queue = queue.Queue()
    dummy_component = DummyComponent(
        message_client,
        env_variables[__SIMULATION_ID],
        env_variables[__SIMULATION_COMPONENT_NAME],
        env_variables[__SIMULATION_STATE_MESSAGE_TOPIC],
        env_variables[__SIMULATION_EPOCH_MESSAGE_TOPIC],
        env_variables[__SIMULATION_STATUS_MESSAGE_TOPIC],
        env_variables[__MIN_SLEEP_TIME],
        env_variables[__MAX_SLEEP_TIME],
        end_queue)

    while True:
        end_item = end_queue.get()
        if end_item is None:
            message_client.close()
            break


if __name__ == "__main__":
    start_dummy_component()
