# -*- coding: utf-8 -*-

"""This module for Simulation Manager."""

import queue
import threading
import time

from tools.callbacks import StatusMessageCallback
from tools.clients import RabbitmqClient
from tools.messages import get_next_message_id, StatusMessage, SimulationStateMessage
from tools.tools import get_logger, load_environmental_variables

FILE_LOGGER = get_logger(__name__)

TIMEOUT_INTERVAL = 300

__SIMULATION_ID = "SIMULATION_ID"
__SIMULATION_MANAGER_NAME = "SIMULATION_MANAGER_NAME"
__SIMULATION_OMEGA_NAME = "SIMULATION_OMEGA_NAME"
__SIMULATION_STATUS_MESSAGE_TOPIC = "SIMULATION_STATUS_MESSAGE_TOPIC"
__SIMULATION_STATE_MESSAGE_TOPIC = "SIMULATION_STATE_MESSAGE_TOPIC"
__SIMULATION_MAX_EPOCHS = "SIMULATION_MAX_EPOCHS"


def status_queue_listener(message_queue, omega_name, manager_object):
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
            if message.simulation_id != manager_object.simulation_id:
                FILE_LOGGER.info(
                    "Received status message for a different simulation: '{:s}' instead of '{:s}'".format(
                        message.simulation_id, manager_object.simulation_id))
            elif message.message_type != status_message_type:
                FILE_LOGGER.info(
                    "Received a status message with wrong message type: '{:s}' instead of '{:s}'".format(
                        message.message_type, status_message_type))
            elif message.value != status_message_value_ok:
                FILE_LOGGER.info(
                    "Received a status message with an unknown value: '{:s}' instead of '{:s}'".format(
                        message.value, status_message_value_ok))
            elif message.source_process_id == omega_name:
                FILE_LOGGER.debug("Received a status message from Omega")
                manager_object.check_simulation_state(message.epoch_number)
        else:
            FILE_LOGGER.warning("Received '{:s}' message when expecting for '{:s}' message".format(
                type(message), status_message_type))


class SimulationManager:
    """Class that holds the state of the simulation manager."""
    SIMULATION_STATE_VALUE_RUNNING = "running"
    SIMULATION_STATE_VALUE_STOPPED = "stopped"

    def __init__(self, rabbitmq_client, simulation_id, manager_name, max_epochs, state_topic, end_queue):
        self.__rabbitmq_client = rabbitmq_client
        self.__simulation_id = simulation_id
        self.__manager_name = manager_name
        self.__simulation_state = SimulationManager.SIMULATION_STATE_VALUE_STOPPED
        self.__epoch_number = -1
        self.__max_epochs = max_epochs
        self.__state_topic = state_topic
        self.__end_queue = end_queue
        self.__message_id_generator = get_next_message_id(self.manager_name)

    @property
    def simulation_id(self):
        """The simulation ID for the simulation."""
        return self.__simulation_id

    @property
    def manager_name(self):
        """The simulation manager name."""
        return self.__manager_name

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

    def check_simulation_state(self, omega_done_with_epoch):
        """Called when Omega is finished with the current epoch.
           Sends a simulation state message after updating the state."""
        if omega_done_with_epoch < self.max_epochs:
            self.simulation_state = SimulationManager.SIMULATION_STATE_VALUE_RUNNING
        else:
            self.simulation_state = SimulationManager.SIMULATION_STATE_VALUE_STOPPED

        self.__epoch_number = omega_done_with_epoch
        self.send_state_message()

    def send_state_message(self):
        """Sends a simulation state message."""
        new_simulation_state_message = self.__get_simulation_state_message()
        self.__rabbitmq_client.send_message(self.__state_topic, new_simulation_state_message)

        if (self.__epoch_number == 0 and
                self.simulation_state == SimulationManager.SIMULATION_STATE_VALUE_RUNNING):
            print("Starting the simulation", flush=True)
        elif (self.__epoch_number >= self.max_epochs and
              self.simulation_state == SimulationManager.SIMULATION_STATE_VALUE_STOPPED):
            print("Stopping the simulation", flush=True)

            time.sleep(TIMEOUT_INTERVAL)
            self.__end_queue.put(None)

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
            FILE_LOGGER.error("Problem with creating a simulation state message")

        return state_message.bytes()


def start_manager():
    """Simple test for the Simulation manager process."""
    env_variables = load_environmental_variables(
        (__SIMULATION_ID, str),
        (__SIMULATION_MANAGER_NAME, str, "manager"),
        (__SIMULATION_OMEGA_NAME, str, "omega"),
        (__SIMULATION_STATUS_MESSAGE_TOPIC, str, "status"),
        (__SIMULATION_STATE_MESSAGE_TOPIC, str, "state"),
        (__SIMULATION_MAX_EPOCHS, int, 5)
    )

    time.sleep(TIMEOUT_INTERVAL / 5)

    message_client = RabbitmqClient()

    status_queue = queue.Queue()
    status_callback = StatusMessageCallback(status_queue)
    message_client.add_listener(env_variables[__SIMULATION_STATUS_MESSAGE_TOPIC], status_callback)

    end_queue = queue.Queue()
    manager = SimulationManager(
        message_client,
        env_variables[__SIMULATION_ID],
        env_variables[__SIMULATION_MANAGER_NAME],
        env_variables[__SIMULATION_MAX_EPOCHS],
        env_variables[__SIMULATION_STATE_MESSAGE_TOPIC],
        end_queue)

    status_listener = threading.Thread(
        target=status_queue_listener,
        args=(
            status_queue,
            env_variables[__SIMULATION_OMEGA_NAME],
            manager),
        daemon=True)
    status_listener.start()

    while True:
        end_item = end_queue.get()
        if end_item is None:
            message_client.close()
            break


if __name__ == "__main__":
    start_manager()
