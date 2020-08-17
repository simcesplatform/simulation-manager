# -*- coding: utf-8 -*-

"""This module contains a dummy simulation component that has very simple internal logic."""

import asyncio
import queue
import random

from tools.clients import RabbitmqClient
from tools.messages import EpochMessage, ErrorMessage, StatusMessage, SimulationStateMessage, get_next_message_id
from tools.tools import FullLogger, load_environmental_variables

LOGGER = FullLogger(__name__)

TIMEOUT_INTERVAL = 10

__SIMULATION_ID = "SIMULATION_ID"
__SIMULATION_COMPONENT_NAME = "SIMULATION_COMPONENT_NAME"
__SIMULATION_EPOCH_MESSAGE_TOPIC = "SIMULATION_EPOCH_MESSAGE_TOPIC"
__SIMULATION_STATUS_MESSAGE_TOPIC = "SIMULATION_STATUS_MESSAGE_TOPIC"
__SIMULATION_STATE_MESSAGE_TOPIC = "SIMULATION_STATE_MESSAGE_TOPIC"
__SIMULATION_ERROR_MESSAGE_TOPIC = "SIMULATION_ERROR_MESSAGE_TOPIC"

__MIN_SLEEP_TIME = "MIN_SLEEP_TIME"
__MAX_SLEEP_TIME = "MAX_SLEEP_TIME"

__ERROR_CHANCE = "ERROR_CHANCE"
__SEND_MISS_CHANCE = "SEND_MISS_CHANCE"
__RECEIVE_MISS_CHANCE = "RECEIVE_MISS_CHANCE"
__WARNING_CHANCE = "WARNING_CHANCE"


class DummyComponent:
    """Class for holding the state of a dummy simulation component."""
    SIMULATION_STATE_VALUE_RUNNING = SimulationStateMessage.SIMULATION_STATES[0]   # "running"
    SIMULATION_STATE_VALUE_STOPPED = SimulationStateMessage.SIMULATION_STATES[-1]  # "stopped"

    def __init__(self, rabbitmq_client, simulation_id, component_name,
                 simulation_state_topic, epoch_topic, status_topic, error_topic, min_delay, max_delay,
                 error_chance, send_miss_chance, receive_miss_chance, warning_chance, end_queue):
        self.__rabbitmq_client = rabbitmq_client
        self.__simulation_id = simulation_id
        self.__component_name = component_name

        self.__simulation_state_topic = simulation_state_topic
        self.__epoch_topic = epoch_topic
        self.__status_topic = status_topic
        self.__error_topic = error_topic

        self.__min_delay = min_delay
        self.__max_delay = max_delay
        self.__error_chance = error_chance
        self.__send_miss_chance = send_miss_chance
        self.__receive_miss_chance = receive_miss_chance
        self.__warning_chance = warning_chance

        self.__simulation_state = DummyComponent.SIMULATION_STATE_VALUE_STOPPED
        self.__latest_epoch = 0
        self.__completed_epoch = 0
        self.__triggering_message_id = ""
        self.__last_status_message_id = None

        self.__end_queue = end_queue
        self.__message_id_generator = get_next_message_id(component_name)

        self.__rabbitmq_client.add_listener(
            [
                self.__simulation_state_topic,
                self.__epoch_topic
            ],
            self.general_message_handler)

    @property
    def simulation_id(self):
        """The simulation ID for the simulation."""
        return self.__simulation_id

    @property
    def component_name(self):
        """The component name in the simulation."""
        return self.__component_name

    async def stop(self):
        """Stops the component."""
        LOGGER.info("Stopping the component: '{:s}'".format(self.component_name))
        await self.set_simulation_state(DummyComponent.SIMULATION_STATE_VALUE_STOPPED)

    def get_simulation_state(self):
        """Returns the simulation state attribute."""
        return self.__simulation_state

    async def set_simulation_state(self, new_simulation_state):
        """Sets the simulation state. If the new simulation state is "running" and the current epoch is 0,
           sends a status message to the message bus.
           If the new simulation state is "stopped", stops the dummy component."""
        if new_simulation_state in SimulationStateMessage.SIMULATION_STATES:
            self.__simulation_state = new_simulation_state

            if new_simulation_state == DummyComponent.SIMULATION_STATE_VALUE_RUNNING:
                if self.__latest_epoch == 0:
                    await self.__send_new_status_message()

            elif new_simulation_state == DummyComponent.SIMULATION_STATE_VALUE_STOPPED:
                LOGGER.info("Component {:s} stopping in {:d} seconds.".format(
                    self.__component_name, TIMEOUT_INTERVAL))
                await asyncio.sleep(TIMEOUT_INTERVAL)
                self.__end_queue.put(None)

    async def start_epoch(self, epoch_number):
        """Starts a new epoch for the component. Sends a status message when finished."""
        if self.__simulation_state == DummyComponent.SIMULATION_STATE_VALUE_RUNNING:
            self.__latest_epoch = epoch_number

            # If the epoch is already completed, send a new status message immediately.
            if self.__completed_epoch == epoch_number:
                LOGGER.debug("Resending status message for epoch {:d}".format(epoch_number))
                await self.__send_new_status_message()
                return

            rand_error_chance = random.random()
            if rand_error_chance < self.__error_chance:
                LOGGER.error("Encountered a random error.")
                await self.__send_error_message("Random error")
            else:
                rand_wait_time = random.randint(self.__min_delay, self.__max_delay)
                LOGGER.info("Component {:s} sending status message for epoch {:d} in {:d} seconds.".format(
                    self.__component_name, self.__latest_epoch, rand_wait_time))
                await asyncio.sleep(rand_wait_time)
            await self.__send_new_status_message()

    async def general_message_handler(self, message_object, message_routing_key):
        """Forwards the message handling to the appropriate function depending on the message type."""
        if isinstance(message_object, SimulationStateMessage):
            await self.simulation_state_message_handler(message_object, message_routing_key)
            return

        if random.random() < self.__receive_miss_chance:
            # Simulate a connection error by not receiving an epoch message.
            LOGGER.warning("Received message was ignored.")
            return

        if isinstance(message_object, EpochMessage):
            await self.epoch_message_handler(message_object, message_routing_key)
        else:
            LOGGER.warning("Received '{:s}' message when expecting for '{:s}' or '{:s}' message".format(
                str(type(message_object)), str(SimulationStateMessage), str(EpochMessage)))

    async def simulation_state_message_handler(self, message_object, message_routing_key):
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
            self.__triggering_message_id = message_object.message_id
            await self.set_simulation_state(message_object.simulation_state)

    async def epoch_message_handler(self, message_object, message_routing_key):
        """Handles the received epoch messages."""
        if message_object.simulation_id != self.simulation_id:
            LOGGER.info(
                "Received epoch message for a different simulation: '{:s}' instead of '{:s}'".format(
                    message_object.simulation_id, self.simulation_id))
        elif message_object.message_type != EpochMessage.CLASS_MESSAGE_TYPE:
            LOGGER.info(
                "Received a epoch message with wrong message type: '{:s}' instead of '{:s}'".format(
                    message_object.message_type, EpochMessage.CLASS_MESSAGE_TYPE))
        elif (message_object.epoch_number == self.__latest_epoch and
                self.__last_status_message_id in message_object.triggering_message_ids):
            LOGGER.info("Status message has already been registered for epoch {:d}".format(self.__latest_epoch))
        else:
            LOGGER.debug("Received an epoch from {:s} on topic {:s}".format(
                message_object.source_process_id, message_routing_key))
            self.__triggering_message_id = message_object.message_id
            await self.start_epoch(message_object.epoch_number)

    async def __send_new_status_message(self):
        new_status_message = self.__get_status_message()

        if self.__latest_epoch > 0 and random.random() < self.__send_miss_chance:
            # simulate connection error by not sending the status message for an epoch
            LOGGER.warning("No status message sent this time.")
        else:
            await self.__rabbitmq_client.send_message(self.__status_topic, new_status_message)

        self.__completed_epoch = self.__latest_epoch

    async def __send_error_message(self, description):
        error_message = self.__get_error_message(description)
        await self.__rabbitmq_client.send_message(self.__error_topic, error_message)

    def __get_status_message(self):
        status_message = StatusMessage(**{
            "Type": StatusMessage.CLASS_MESSAGE_TYPE,
            "SimulationId": self.simulation_id,
            "SourceProcessId": self.component_name,
            "MessageId": next(self.__message_id_generator),
            "EpochNumber": self.__latest_epoch,
            "TriggeringMessageIds": [self.__triggering_message_id],
            "Value": StatusMessage.STATUS_VALUES[0]
        })
        if status_message is None:
            LOGGER.error("Problem with creating a status message")
        elif random.random() < self.__warning_chance:
            LOGGER.debug("Adding a warning to the status message.")
            status_message.warnings = ["warning.internal"]

        self.__last_status_message_id = status_message.message_id
        return status_message.bytes()

    def __get_error_message(self, description):
        error_message = ErrorMessage(**{
            "Type": ErrorMessage.CLASS_MESSAGE_TYPE,
            "SimulationId": self.simulation_id,
            "SourceProcessId": self.component_name,
            "MessageId": next(self.__message_id_generator),
            "EpochNumber": self.__latest_epoch,
            "TriggeringMessageIds": [self.__triggering_message_id],
            "Description": description
        })
        if error_message is None:
            LOGGER.error("Problem with creating an error message")

        return error_message.bytes()


async def start_dummy_component():
    """Start a dummy component for the simulation platform."""
    env_variables = load_environmental_variables(
        (__SIMULATION_ID, str),
        (__SIMULATION_COMPONENT_NAME, str, "dummy"),
        (__SIMULATION_EPOCH_MESSAGE_TOPIC, str, "epoch"),
        (__SIMULATION_STATUS_MESSAGE_TOPIC, str, "status"),
        (__SIMULATION_STATE_MESSAGE_TOPIC, str, "state"),
        (__SIMULATION_ERROR_MESSAGE_TOPIC, str, "error"),
        (__MIN_SLEEP_TIME, int, 2),
        (__MAX_SLEEP_TIME, int, 15),
        (__ERROR_CHANCE, float, 0.0),
        (__SEND_MISS_CHANCE, float, 0.0),
        (__RECEIVE_MISS_CHANCE, float, 0.0),
        (__WARNING_CHANCE, float, 0.0)
    )

    message_client = RabbitmqClient()

    end_queue = queue.Queue()
    dummy_component = DummyComponent(
        rabbitmq_client=message_client,
        simulation_id=env_variables[__SIMULATION_ID],
        component_name=env_variables[__SIMULATION_COMPONENT_NAME],
        simulation_state_topic=env_variables[__SIMULATION_STATE_MESSAGE_TOPIC],
        epoch_topic=env_variables[__SIMULATION_EPOCH_MESSAGE_TOPIC],
        status_topic=env_variables[__SIMULATION_STATUS_MESSAGE_TOPIC],
        error_topic=env_variables[__SIMULATION_ERROR_MESSAGE_TOPIC],
        min_delay=env_variables[__MIN_SLEEP_TIME],
        max_delay=env_variables[__MAX_SLEEP_TIME],
        error_chance=env_variables[__ERROR_CHANCE],
        send_miss_chance=env_variables[__SEND_MISS_CHANCE],
        receive_miss_chance=env_variables[__RECEIVE_MISS_CHANCE],
        warning_chance=env_variables[__WARNING_CHANCE],
        end_queue=end_queue)

    while True:
        end_item = end_queue.get()
        if end_item is None:
            message_client.close()
            break


if __name__ == "__main__":
    asyncio.run(start_dummy_component())
