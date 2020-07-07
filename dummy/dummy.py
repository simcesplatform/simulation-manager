# -*- coding: utf-8 -*-

"""This module a dummy simulation component."""

import queue
import random
import threading
import time

from tools.callbacks import EpochMessageCallback, SimulationStateMessageCallback
from tools.clients import RabbitmqClient
from tools.messages import EpochMessage, StatusMessage, SimulationStateMessage, get_next_message_id
from tools.tools import get_logger, load_environmental_variables

FILE_LOGGER = get_logger(__name__)

TIMEOUT_INTERVAL = 300

__SIMULATION_ID = "SIMULATION_ID"
__SIMULATION_COMPONENT_NAME = "SIMULATION_COMPONENT_NAME"
__SIMULATION_EPOCH_MESSAGE_TOPIC = "SIMULATION_EPOCH_MESSAGE_TOPIC"
__SIMULATION_STATUS_MESSAGE_TOPIC = "SIMULATION_STATUS_MESSAGE_TOPIC"
__SIMULATION_STATE_MESSAGE_TOPIC = "SIMULATION_STATE_MESSAGE_TOPIC"

MIN_SLEEP_TIME = 2
MAX_SLEEP_TIME = 15


def simulation_state_queue_listener(message_queue, dummy_object):
    """Listens to the given queue for simulation state messages."""
    simulation_state_message = SimulationStateMessage.CLASS_MESSAGE_TYPE

    while True:
        try:
            message = message_queue.get(timeout=TIMEOUT_INTERVAL)
            if message is None:
                break
        except queue.Empty:
            FILE_LOGGER.info("No simulations state messages received in {:d} seconds".format(TIMEOUT_INTERVAL))
            continue

        if isinstance(message, StatusMessage):
            if message.simulation_id != dummy_object.simulation_id:
                FILE_LOGGER.info(
                    "Received state message for a different simulation: '{:s}' instead of '{:s}'".format(
                        message.simulation_id, dummy_object.simulation_id))
            elif message.message_type != simulation_state_message:
                FILE_LOGGER.info(
                    "Received a state message with wrong message type: '{:s}' instead of '{:s}'".format(
                        message.message_type, simulation_state_message))
            else:
                dummy_object.simulation_state = message.simulation_state
        else:
            FILE_LOGGER.warning("Received '{:s}' message when expecting for '{:s}' message".format(
                type(message), simulation_state_message))


def epoch_queue_listener(message_queue, dummy_object):
    """Listens to the given queue for epoch messages."""
    epoch_message_type = EpochMessage.CLASS_MESSAGE_TYPE
    FILE_LOGGER.info("{:s} started epoch_queue_listener".format(dummy_object.component_name))

    while True:
        try:
            message = message_queue.get(timeout=TIMEOUT_INTERVAL)
            if message is None:
                break
        except queue.Empty:
            FILE_LOGGER.info("No simulations state messages received in {:d} seconds".format(TIMEOUT_INTERVAL))
            continue

        if isinstance(message, EpochMessage):
            if message.simulation_id != dummy_object.simulation_id:
                FILE_LOGGER.info(
                    "Received epoch message for a different simulation: '{:s}' instead of '{:s}'".format(
                        message.simulation_id, dummy_object.simulation_id))
            elif message.message_type != epoch_message_type:
                FILE_LOGGER.info(
                    "Received a epoch message with wrong message type: '{:s}' instead of '{:s}'".format(
                        message.message_type, epoch_message_type))
            else:
                dummy_object.start_epoch(message.epoch_number)
        else:
            FILE_LOGGER.warning("Received '{:s}' message when expecting for '{:s}' message".format(
                type(message), epoch_message_type))


class DummyComponent:
    """Class for holding the state of a dummy simulation component."""
    SIMULATION_STATE_VALUE_RUNNING = "running"
    SIMULATION_STATE_VALUE_STOPPED = "stopped"

    def __init__(self, rabbitmq_client, simulation_id, component_name, status_topic, end_queue):
        self.__rabbitmq_client = rabbitmq_client
        self.__simulation_id = simulation_id
        self.__component_name = component_name
        self.__status_topic = status_topic
        self.__simulation_state = DummyComponent.SIMULATION_STATE_VALUE_STOPPED

        self.__end_queue = end_queue
        self.__message_id_generator = get_next_message_id(component_name)

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
        if new_simulation_state in (
                DummyComponent.SIMULATION_STATE_VALUE_RUNNING,
                DummyComponent.SIMULATION_STATE_VALUE_STOPPED):
            self.__simulation_state = new_simulation_state

            if new_simulation_state == DummyComponent.SIMULATION_STATE_VALUE_STOPPED:
                time.sleep(TIMEOUT_INTERVAL)
                self.__end_queue.put(None)

    def start_epoch(self, epoch_number):
        """Starts a new epoch for the component. Sends a status message when finished."""
        if self.__simulation_state == DummyComponent.SIMULATION_STATE_VALUE_RUNNING:
            time.sleep(random.randint(MIN_SLEEP_TIME, MAX_SLEEP_TIME))
            self.__send_new_status_message(epoch_number)

    def __send_new_status_message(self, epoch_number):
        new_status_message = self.__get_status_message(epoch_number)
        self.__rabbitmq_client.send_message(self.__status_topic, new_status_message)

    def __get_status_message(self, epoch_number):
        status_message = StatusMessage(**{
            "Type": StatusMessage.CLASS_MESSAGE_TYPE,
            "SimulationId": self.simulation_id,
            "SourceProcessId": self.component_name,
            "MessageId": next(self.__message_id_generator),
            "EpochNumber": epoch_number,
            "TriggeringMessageIds": ["placeholder"],
            "Value": StatusMessage.STATUS_VALUES[0]
        })
        if status_message is None:
            FILE_LOGGER.error("Problem with creating a status message")

        return status_message.bytes()


def start_dummy_component():
    """Start a dummy component for the simulation platform."""
    env_variables = load_environmental_variables(
        (__SIMULATION_ID, str),
        (__SIMULATION_COMPONENT_NAME, str, "dummy"),
        (__SIMULATION_EPOCH_MESSAGE_TOPIC, str, "epoch"),
        (__SIMULATION_STATUS_MESSAGE_TOPIC, str, "status"),
        (__SIMULATION_STATE_MESSAGE_TOPIC, str, "state")
    )

    time.sleep(TIMEOUT_INTERVAL / 5)

    message_client = RabbitmqClient()

    epoch_queue = queue.Queue()
    epoch_callback = EpochMessageCallback(epoch_queue)
    message_client.add_listener(env_variables[__SIMULATION_EPOCH_MESSAGE_TOPIC], epoch_callback)

    simulation_state_queue = queue.Queue()
    simulation_state_callback = SimulationStateMessageCallback(simulation_state_queue)
    message_client.add_listener(env_variables[__SIMULATION_STATE_MESSAGE_TOPIC], simulation_state_callback)

    end_queue = queue.Queue()
    dummy_component = DummyComponent(
        message_client,
        env_variables[__SIMULATION_ID],
        env_variables[__SIMULATION_COMPONENT_NAME],
        env_variables[__SIMULATION_STATUS_MESSAGE_TOPIC],
        end_queue)

    epoch_listener = threading.Thread(
        target=epoch_queue_listener,
        args=(
            epoch_queue,
            dummy_component),
        daemon=True)
    epoch_listener.start()

    simulation_state_listener = threading.Thread(
        target=simulation_state_queue_listener,
        args=(
            simulation_state_queue,
            dummy_component),
        daemon=True)
    simulation_state_listener.start()

    while True:
        end_item = end_queue.get()
        if end_item is None:
            message_client.close()
            break


if __name__ == "__main__":
    start_dummy_component()
