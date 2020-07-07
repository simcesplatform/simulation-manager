# -*- coding: utf-8 -*-

"""This module for Omega."""

import datetime
import queue
import threading
import time

from tools.callbacks import StatusMessageCallback, SimulationStateMessageCallback
from tools.clients import RabbitmqClient
from tools.components import SimulationComponents
from tools.datetime_tools import to_utc_datetime_object
from tools.messages import EpochMessage, StatusMessage, SimulationStateMessage, get_next_message_id
from tools.tools import get_logger, load_environmental_variables

FILE_LOGGER = get_logger(__name__)

TIMEOUT_INTERVAL = 300

__SIMULATION_ID = "SIMULATION_ID"
__SIMULATION_OMEGA_NAME = "SIMULATION_OMEGA_NAME"
__SIMULATION_COMPONENTS = "SIMULATION_COMPONENTS"
__SIMULATION_EPOCH_MESSAGE_TOPIC = "SIMULATION_EPOCH_MESSAGE_TOPIC"
__SIMULATION_STATUS_MESSAGE_TOPIC = "SIMULATION_STATUS_MESSAGE_TOPIC"
__SIMULATION_STATE_MESSAGE_TOPIC = "SIMULATION_STATE_MESSAGE_TOPIC"
__SIMULATION_INITIAL_START_TIME = "SIMULATION_INITIAL_START_TIME"
__SIMULATION_EPOCH_LENGTH = "SIMULATION_EPOCH_LENGTH"


def status_queue_listener(message_queue, components_object, omega_object):
    """Listens to the given queue for status messages."""
    status_message_type = StatusMessage.CLASS_MESSAGE_TYPE
    status_message_value_ok = StatusMessage.STATUS_VALUES[0]

    while True:
        try:
            message = message_queue.get(timeout=TIMEOUT_INTERVAL)
            if message is None:
                break
        except queue.Empty:
            FILE_LOGGER.info("No status messages received in {:d} seconds".format(TIMEOUT_INTERVAL))
            continue

        if isinstance(message, StatusMessage):
            if message.simulation_id != omega_object.simulation_id:
                FILE_LOGGER.info(
                    "Received status message for a different simulation: '{:s}' instead of '{:s}'".format(
                        message.simulation_id, omega_object.simulation_id))
            elif message.message_type != status_message_type:
                FILE_LOGGER.info(
                    "Received a status message with wrong message type: '{:s}' instead of '{:s}'".format(
                        message.message_type, status_message_type))
            elif message.value != status_message_value_ok:
                FILE_LOGGER.info(
                    "Received a status message with an unknown value: '{:s}' instead of '{:s}'".format(
                        message.value, status_message_value_ok))
            elif message.source_process_id != omega_object.omega_name:
                components_object.register_ready_message(message.source_process_id, message.epoch_number)
                omega_object.check_components()
        else:
            FILE_LOGGER.warning("Received '{:s}' message when expecting for '{:s}' message".format(
                type(message), status_message_type))


def simulation_state_queue_listener(message_queue, omega_object):
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
            if message.simulation_id != omega_object.simulation_id:
                FILE_LOGGER.info(
                    "Received state message for a different simulation: '{:s}' instead of '{:s}'".format(
                        message.simulation_id, omega_object.simulation_id))
            elif message.message_type != simulation_state_message:
                FILE_LOGGER.info(
                    "Received a state message with wrong message type: '{:s}' instead of '{:s}'".format(
                        message.message_type, simulation_state_message))
            else:
                omega_object.simulation_state = message.simulation_state
        else:
            FILE_LOGGER.warning("Received '{:s}' message when expecting for '{:s}' message".format(
                type(message), simulation_state_message))


class Omega:
    """Class for holding the state of Omega component."""
    SIMULATION_STATE_VALUE_RUNNING = "running"
    SIMULATION_STATE_VALUE_STOPPED = "stopped"

    def __init__(self, rabbitmq_client, simulation_id, omega_name, simulation_components,
                 initial_start_time, epoch_length, epoch_topic, status_topic, end_queue):
        self.__rabbitmq_client = rabbitmq_client
        self.__simulation_id = simulation_id
        self.__omega_name = omega_name
        self.__simulation_components = simulation_components

        self.__simulation_state = Omega.SIMULATION_STATE_VALUE_STOPPED
        self.__epoch_number = 0
        self.__epoch_length = epoch_length
        self.__current_start_time = to_utc_datetime_object(initial_start_time)
        self.__current_end_time = None

        self.__epoch_topic = epoch_topic
        self.__status_topic = status_topic
        self.__latest_status = SimulationComponents.NO_MESSAGES

        self.__end_queue = end_queue
        self.__message_id_generator = get_next_message_id(omega_name)

        # Send the first ready message for Simulation Manager 10 seconds after start.
        time.sleep(TIMEOUT_INTERVAL)
        self.check_components()

    @property
    def simulation_id(self):
        """The simulation ID for the simulation."""
        return self.__simulation_id

    @property
    def omega_name(self):
        """The name of the omega component in the simulation."""
        return self.__omega_name

    @property
    def epoch_number(self):
        """The name of the omega component in the simulation."""
        return self.__epoch_number

    @property
    def simulation_state(self):
        """Return the simulation state attribute."""
        return self.__simulation_state

    @simulation_state.setter
    def simulation_state(self, new_simulation_state):
        if new_simulation_state in (
                Omega.SIMULATION_STATE_VALUE_RUNNING,
                Omega.SIMULATION_STATE_VALUE_STOPPED):
            self.__simulation_state = new_simulation_state
            self.check_components()

    def check_components(self):
        """Checks the status of the simulation components and sends a new epoch or status message if needed."""
        latest_full_epoch = self.__simulation_components.get_latest_full_epoch()

        # the current epoch is finished => inform the simulation manager with a status message
        if self.__latest_status < self.__epoch_number:
            if (self.__epoch_number == 0 or
                    (self.__simulation_state == Omega.SIMULATION_STATE_VALUE_RUNNING and
                     latest_full_epoch == self.__epoch_number)):
                self.__send_status_message()

        # the current epoch is finished and the simulation manager has been informed previously,
        # => send a new epoch message
        elif (self.__simulation_state == Omega.SIMULATION_STATE_VALUE_RUNNING and
              latest_full_epoch == self.__epoch_number):
            self.__send_new_epoch_message()

        # the simulation has stopped after running at least one epoch => stop Omega
        elif (self.__simulation_state == Omega.SIMULATION_STATE_VALUE_STOPPED and self.__epoch_number > 0):
            time.sleep(TIMEOUT_INTERVAL)
            self.__end_queue.put(None)

    def __send_status_message(self):
        self.__latest_status = self.__epoch_number
        new_status_message = self.__get_new_message(StatusMessage.CLASS_MESSAGE_TYPE)
        self.__rabbitmq_client.send_message(self.__status_topic, new_status_message)

    def __send_new_epoch_message(self):
        self.__epoch_number += 1
        if self.__current_end_time is not None:
            self.__current_start_time = self.__current_end_time
        self.__current_end_time = self.__current_start_time + datetime.timedelta(seconds=self.__epoch_length)

        new_epoch_message = self.__get_new_message(EpochMessage.CLASS_MESSAGE_TYPE)
        self.__rabbitmq_client.send_message(self.__epoch_topic, new_epoch_message)
        print("Starting epoch {:d}".format(self.__epoch_number))

    def __get_new_message(self, message_type):
        """Returns a new message for the message bus."""
        if message_type == EpochMessage.CLASS_MESSAGE_TYPE:
            new_message = EpochMessage(**{
                "Type": message_type,
                "SimulationId": self.__simulation_id,
                "SourceProcessId": self.__omega_name,
                "MessageId": next(self.__message_id_generator),
                "EpochNumber": self.__epoch_number,
                "TriggeringMessageIds": ["placeholder"],
                "StartTime": self.__current_start_time,
                "EndTime": self.__current_end_time
            })
        elif message_type == StatusMessage.CLASS_MESSAGE_TYPE:
            return StatusMessage(**{
                "Type": message_type,
                "SimulationId": self.__simulation_id,
                "SourceProcessId": self.__omega_name,
                "MessageId": next(self.__message_id_generator),
                "EpochNumber": self.__epoch_number,
                "TriggeringMessageIds": ["placeholder"],
                "Value": StatusMessage.STATUS_VALUES[0]
            })
        else:
            new_message = None

        if new_message is None:
            FILE_LOGGER.error("Problem with creating a {:s} message".format(message_type))

        return new_message.bytes()


def start_omega():
    """Simple test for the Omega process."""
    env_variables = load_environmental_variables(
        (__SIMULATION_ID, str),
        (__SIMULATION_OMEGA_NAME, str, "omega"),
        (__SIMULATION_COMPONENTS, str, ""),
        (__SIMULATION_EPOCH_MESSAGE_TOPIC, str, "epoch"),
        (__SIMULATION_STATUS_MESSAGE_TOPIC, str, "status"),
        (__SIMULATION_STATE_MESSAGE_TOPIC, str, "state"),
        (__SIMULATION_EPOCH_LENGTH, int, 3600),
        (__SIMULATION_INITIAL_START_TIME, str, "2020-01-01T00:00:00.000Z")
    )

    time.sleep(TIMEOUT_INTERVAL / 5)

    message_client = RabbitmqClient()

    simulation_components = SimulationComponents()
    for component_name in env_variables[__SIMULATION_COMPONENTS].split(","):
        simulation_components.add_component(component_name)

    status_queue = queue.Queue()
    status_callback = StatusMessageCallback(status_queue)
    message_client.add_listener(env_variables[__SIMULATION_STATUS_MESSAGE_TOPIC], status_callback)

    simulation_state_queue = queue.Queue()
    simulation_state_callback = SimulationStateMessageCallback(simulation_state_queue)
    message_client.add_listener(env_variables[__SIMULATION_STATE_MESSAGE_TOPIC], simulation_state_callback)

    end_queue = queue.Queue()
    omega = Omega(
        message_client,
        env_variables[__SIMULATION_ID],
        env_variables[__SIMULATION_OMEGA_NAME],
        simulation_components,
        env_variables[__SIMULATION_INITIAL_START_TIME],
        env_variables[__SIMULATION_EPOCH_LENGTH],
        env_variables[__SIMULATION_STATUS_MESSAGE_TOPIC],
        env_variables[__SIMULATION_STATE_MESSAGE_TOPIC],
        end_queue)

    status_listener = threading.Thread(
        target=status_queue_listener,
        args=(
            status_queue,
            simulation_components,
            omega),
        daemon=True)
    status_listener.start()

    simulation_state_listener = threading.Thread(
        target=simulation_state_queue_listener,
        args=(
            simulation_state_queue,
            omega),
        daemon=True)
    simulation_state_listener.start()

    while True:
        end_item = end_queue.get()
        if end_item is None:
            message_client.close()
            break


if __name__ == "__main__":
    start_omega()
