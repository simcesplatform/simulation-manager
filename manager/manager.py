# -*- coding: utf-8 -*-

"""This module for Simulation Manager."""

import datetime
import queue
import threading
import time

from tools.callbacks import GeneralMessageCallback
from tools.clients import RabbitmqClient
from tools.components import SimulationComponents
from tools.datetime_tools import to_utc_datetime_object
from tools.messages import EpochMessage, ErrorMessage, StatusMessage, SimulationStateMessage, get_next_message_id
from tools.tools import FullLogger, load_environmental_variables

LOGGER = FullLogger(__name__)

TIMEOUT_INTERVAL = 15

__SIMULATION_ID = "SIMULATION_ID"
__SIMULATION_MANAGER_NAME = "SIMULATION_MANAGER_NAME"
__SIMULATION_OMEGA_NAME = "SIMULATION_OMEGA_NAME"

__SIMULATION_EPOCH_MESSAGE_TOPIC = "SIMULATION_EPOCH_MESSAGE_TOPIC"
__SIMULATION_STATUS_MESSAGE_TOPIC = "SIMULATION_STATUS_MESSAGE_TOPIC"
__SIMULATION_STATE_MESSAGE_TOPIC = "SIMULATION_STATE_MESSAGE_TOPIC"
__SIMULATION_ERROR_MESSAGE_TOPIC = "SIMULATION_ERROR_MESSAGE_TOPIC"

__SIMULATION_COMPONENTS = "SIMULATION_COMPONENTS"

__SIMULATION_INITIAL_START_TIME = "SIMULATION_INITIAL_START_TIME"
__SIMULATION_EPOCH_LENGTH = "SIMULATION_EPOCH_LENGTH"
__SIMULATION_MAX_EPOCHS = "SIMULATION_MAX_EPOCHS"


class SimulationManager:
    """Class that holds the state of the simulation manager."""
    SIMULATION_STATE_VALUE_RUNNING = SimulationStateMessage.SIMULATION_STATES[0]   # "running"
    SIMULATION_STATE_VALUE_STOPPED = SimulationStateMessage.SIMULATION_STATES[-1]  # "stopped"

    STATUS_MESSAGE_VALUE_OK = StatusMessage.STATUS_VALUES[0]

    def __init__(self, rabbitmq_client, simulation_id, manager_name, simulation_components,
                 initial_start_time, epoch_length, max_epochs,
                 epoch_topic, state_topic, status_topic, error_topic, end_queue):
        self.__rabbitmq_client = rabbitmq_client
        self.__simulation_id = simulation_id
        self.__manager_name = manager_name
        self.__simulation_components = simulation_components

        self.__simulation_state = SimulationManager.SIMULATION_STATE_VALUE_STOPPED
        self.__epoch_number = 0
        self.__epoch_length = epoch_length
        self.__max_epochs = max_epochs

        self.__current_start_time = to_utc_datetime_object(initial_start_time)
        self.__current_end_time = None

        self.__epoch_topic = epoch_topic
        self.__state_topic = state_topic
        self.__status_topic = status_topic
        self.__error_topic = error_topic
        self.__end_queue = end_queue

        self.__message_id_generator = get_next_message_id(self.manager_name)

        self.__rabbitmq_client.add_listeners(
            [
                self.__status_topic,
                self.__error_topic
            ],
            GeneralMessageCallback(self.general_message_handler))

    def start(self):
        """Starts the simulation. Sends a simulation state message."""
        LOGGER.info("Starting the simulation.")
        self.simulation_state = SimulationManager.SIMULATION_STATE_VALUE_RUNNING

    def stop(self):
        """Stops the simulation. Sends a simulation state message to the message bus."""
        LOGGER.info("Stopping the simulation.")
        self.simulation_state = SimulationManager.SIMULATION_STATE_VALUE_STOPPED

    @property
    def simulation_id(self):
        """The simulation ID for the simulation."""
        return self.__simulation_id

    @property
    def manager_name(self):
        """The simulation manager name."""
        return self.__manager_name

    @property
    def epoch_number(self):
        """The name of the omega component in the simulation."""
        return self.__epoch_number

    @property
    def max_epochs(self):
        """The maximum number of epochs for the simulation."""
        return self.__max_epochs

    @property
    def simulation_state(self):
        """Return the simulation state attribute."""
        return self.__simulation_state

    @simulation_state.setter
    def simulation_state(self, new_simulation_state):
        if new_simulation_state in (
                SimulationManager.SIMULATION_STATE_VALUE_RUNNING,
                SimulationManager.SIMULATION_STATE_VALUE_STOPPED):
            self.__simulation_state = new_simulation_state
            self.send_state_message()

            if new_simulation_state == SimulationManager.SIMULATION_STATE_VALUE_STOPPED:
                LOGGER.info("Simulation manager '{:s}' stopping in {:d} seconds.".format(
                    self.__manager_name, TIMEOUT_INTERVAL))
                time.sleep(TIMEOUT_INTERVAL)
                self.__end_queue.put(None)

    def check_components(self):
        """Checks the status of the simulation components and sends a new epoch message if needed."""
        latest_full_epoch = self.__simulation_components.get_latest_full_epoch()

        if self.simulation_state == SimulationManager.SIMULATION_STATE_VALUE_RUNNING:
            # the current epoch is finished => send a new epoch message
            if self.__epoch_number == 0 or latest_full_epoch == self.__epoch_number:
                self.__send_epoch_message()

    def send_state_message(self):
        """Sends a simulation state message."""
        LOGGER.debug("Sending state message: '{:s}'".format(self.simulation_state))

        new_simulation_state_message = self.__get_simulation_state_message()
        self.__rabbitmq_client.send_message(self.__state_topic, new_simulation_state_message)

    def general_message_handler(self, message_object, message_routing_key):
        """Forwards the message handling to the appropriate function depending on the message type."""
        if isinstance(message_object, StatusMessage):
            self.status_message_handler(message_object, message_routing_key)
        elif isinstance(message_object, ErrorMessage):
            self.error_message_handler(message_object, message_routing_key)
        else:
            LOGGER.warning("Received '{:s}' message when expecting for '{:s}' or '{:s}' message".format(
                str(type(message_object)), str(StatusMessage), str(ErrorMessage)))

    def status_message_handler(self, message_object, message_routing_key):
        """Handles received status messages."""
        if message_object.simulation_id != self.simulation_id:
            LOGGER.info(
                "Received a status message for a different simulation: '{:s}' instead of '{:s}'".format(
                    message_object.simulation_id, self.simulation_id))
        elif message_object.message_type != StatusMessage.CLASS_MESSAGE_TYPE:
            LOGGER.warning(
                "Received a status message with wrong message type: '{:s}' instead of '{:s}'".format(
                    message_object.message_type, StatusMessage.CLASS_MESSAGE_TYPE))
        elif message_object.value != SimulationManager.STATUS_MESSAGE_VALUE_OK:
            LOGGER.warning(
                "Received a status message with an unknown value: '{:s}' instead of '{:s}'".format(
                    message_object.value, SimulationManager.STATUS_MESSAGE_VALUE_OK))
        elif message_object.source_process_id != self.__manager_name:
            LOGGER.debug("Received a status message from {:s}".format(message_object.source_process_id))
            self.__simulation_components.register_ready_message(
                message_object.source_process_id, message_object.epoch_number)
            self.check_components()

    def error_message_handler(self, message_object, message_routing_key):
        """Handles received error messages."""
        if message_object.simulation_id != self.simulation_id:
            LOGGER.info(
                "Received an error message for a different simulation: '{:s}' instead of '{:s}'".format(
                    message_object.simulation_id, self.simulation_id))
        elif message_object.message_type != ErrorMessage.CLASS_MESSAGE_TYPE:
            LOGGER.warning(
                "Received an error message with wrong message type: '{:s}' instead of '{:s}'".format(
                    message_object.message_type, ErrorMessage.CLASS_MESSAGE_TYPE))
        else:
            LOGGER.debug("Received an error message from {:s} with description '{:s}'".format(
                message_object.source_process_id, message_object.description))
            self.stop()

    def __send_epoch_message(self, new_epoch=True):
        if new_epoch or self.epoch_number == 0:
            self.__epoch_number += 1
            if self.__current_end_time is not None:
                self.__current_start_time = self.__current_end_time
            self.__current_end_time = self.__current_start_time + datetime.timedelta(seconds=self.__epoch_length)

        if self.epoch_number <= self.max_epochs:
            new_epoch_message = self.__get_epoch_message(EpochMessage.CLASS_MESSAGE_TYPE)
            self.__rabbitmq_client.send_message(self.__epoch_topic, new_epoch_message)

            LOGGER.info("Starting epoch {:d}".format(self.__epoch_number))

        else:
            self.stop()

    def __get_simulation_state_message(self):
        """Return epoch message."""
        state_message = SimulationStateMessage(**{
            "Type": SimulationStateMessage.CLASS_MESSAGE_TYPE,
            "SimulationId": self.simulation_id,
            "SourceProcessId": self.manager_name,
            "MessageId": next(self.__message_id_generator),
            "SimulationState": self.simulation_state
        })
        if state_message is None:
            LOGGER.error("Problem with creating a simulation state message")

        return state_message.bytes()

    def __get_epoch_message(self, message_type):
        """Returns a new message for the message bus."""
        epoch_message = EpochMessage(**{
            "Type": message_type,
            "SimulationId": self.simulation_id,
            "SourceProcessId": self.manager_name,
            "MessageId": next(self.__message_id_generator),
            "EpochNumber": self.epoch_number,
            "TriggeringMessageIds": ["placeholder"],
            "StartTime": self.__current_start_time,
            "EndTime": self.__current_end_time
        })
        if epoch_message is None:
            LOGGER.error("Problem with creating a epoch message")

        return epoch_message.bytes()


def start_manager():
    """Starts the Simulation manager process."""
    env_variables = load_environmental_variables(
        (__SIMULATION_ID, str),
        (__SIMULATION_MANAGER_NAME, str, "manager"),
        (__SIMULATION_COMPONENTS, str, ""),
        (__SIMULATION_EPOCH_MESSAGE_TOPIC, str, "epoch"),
        (__SIMULATION_STATUS_MESSAGE_TOPIC, str, "status"),
        (__SIMULATION_STATE_MESSAGE_TOPIC, str, "state"),
        (__SIMULATION_ERROR_MESSAGE_TOPIC, str, "error"),
        (__SIMULATION_EPOCH_LENGTH, int, 3600),
        (__SIMULATION_INITIAL_START_TIME, str, "2020-01-01T00:00:00.000Z"),
        (__SIMULATION_MAX_EPOCHS, int, 5)
    )

    simulation_components = SimulationComponents()
    for component_name in env_variables[__SIMULATION_COMPONENTS].split(","):
        simulation_components.add_component(component_name)

    message_client = RabbitmqClient()

    end_queue = queue.Queue()
    manager = SimulationManager(
        message_client,
        env_variables[__SIMULATION_ID],
        env_variables[__SIMULATION_MANAGER_NAME],
        simulation_components,
        env_variables[__SIMULATION_INITIAL_START_TIME],
        env_variables[__SIMULATION_EPOCH_LENGTH],
        env_variables[__SIMULATION_MAX_EPOCHS],
        env_variables[__SIMULATION_EPOCH_MESSAGE_TOPIC],
        env_variables[__SIMULATION_STATE_MESSAGE_TOPIC],
        env_variables[__SIMULATION_STATUS_MESSAGE_TOPIC],
        env_variables[__SIMULATION_ERROR_MESSAGE_TOPIC],
        end_queue)

    # wait a bit to allow other components to initialize and then start the simulation
    time.sleep(TIMEOUT_INTERVAL)
    manager.start()

    while True:
        end_item = end_queue.get()
        if end_item is None:
            LOGGER.info("Closing the simulation manager: '{:s}'".format(manager.manager_name))
            message_client.close()
            break


if __name__ == "__main__":
    start_manager()
