# -*- coding: utf-8 -*-

"""This module contains a dummy simulation component that has very simple internal logic."""

import asyncio
import random
from typing import cast, Any, Union

from dummy.random_series import get_all_random_series, get_latest_values, get_random_initial_values
from tools.components import AbstractSimulationComponent
from tools.exceptions.timeseries import TimeSeriesError
from tools.messages import AbstractMessage, EpochMessage, ResultMessage, StatusMessage, SimulationStateMessage
from tools.tools import FullLogger, load_environmental_variables

LOGGER = FullLogger(__name__)

# The time interval in seconds that is waited before closing after receiving simulation state message "stopped".
TIMEOUT_INTERVAL = 2.5

# The names of the extra environmental variables used by the dummy component.
SIMULATION_RESULT_MESSAGE_TOPIC = "SIMULATION_RESULT_MESSAGE_TOPIC"

MIN_SLEEP_TIME = "MIN_SLEEP_TIME"
MAX_SLEEP_TIME = "MAX_SLEEP_TIME"

ERROR_CHANCE = "ERROR_CHANCE"
SEND_MISS_CHANCE = "SEND_MISS_CHANCE"
RECEIVE_MISS_CHANCE = "RECEIVE_MISS_CHANCE"
WARNING_CHANCE = "WARNING_CHANCE"


class DummyComponent(AbstractSimulationComponent):
    """Class for holding the state of a dummy simulation component."""

    def __init__(self):
        """Loads the parameters required by the dummy component from environmental variables and sets up
           the connection to the RabbitMQ message bus for which the connection parameters are also
           fetched from environmental variables. Opens a topic listener for the simulation state and epoch messages
           after creating the connection to the message bus.
        """
        super().__init__()  # call the AbstractSimulationComponent constructor

        # Load the dummy component specific environmental variables.
        env_variables = load_environmental_variables(
            (SIMULATION_RESULT_MESSAGE_TOPIC, str, "result"),
            (MIN_SLEEP_TIME, float, 2),
            (MAX_SLEEP_TIME, float, 15),
            (ERROR_CHANCE, float, 0.0),
            (SEND_MISS_CHANCE, float, 0.0),
            (RECEIVE_MISS_CHANCE, float, 0.0),
            (WARNING_CHANCE, float, 0.0)
        )

        self._result_topic = cast(str, env_variables[SIMULATION_RESULT_MESSAGE_TOPIC])

        self._min_delay = cast(float, env_variables[MIN_SLEEP_TIME])
        self._max_delay = cast(float, env_variables[MAX_SLEEP_TIME])
        self._error_chance = cast(float, env_variables[ERROR_CHANCE])
        self._send_miss_chance = cast(float, env_variables[SEND_MISS_CHANCE])
        self._receive_miss_chance = cast(float, env_variables[RECEIVE_MISS_CHANCE])
        self._warning_chance = cast(float, env_variables[WARNING_CHANCE])

        # Setup the first values of the randomly generated time series for the result messages.
        self._last_result_values = get_random_initial_values()

    async def start_epoch(self) -> bool:
        """Starts a new epoch for the dummy component. Sends a status message when finished."""
        abstract_start_epoch = await super().start_epoch()
        if not abstract_start_epoch:
            return False

        # At this point the simulation should be running and dummy ready to start the epoch.

        # If the epoch is already completed, send a new status message immediately.
        if self._completed_epoch == self._latest_epoch:
            LOGGER.debug("Resending status message for epoch {:d}".format(self._latest_epoch))
            # The send_status_message call is in the epoch_message_handler.
            return True

        # Simulate an error possibility by using the random error chanche setting.
        rand_error_chance = random.random()
        if rand_error_chance < self._error_chance:
            LOGGER.error("Encountered a random error.")
            await self.send_error_message("Random error")
            return False

        # No errors, do normal epoch handling.
        rand_wait_time = random.uniform(self._min_delay, self._max_delay)
        LOGGER.info("Component {:s} sending status message for epoch {:d} in {:f} seconds.".format(
            self.component_name, self._latest_epoch, rand_wait_time))
        await asyncio.sleep(rand_wait_time)

        await self._send_random_result_message()
        # The send_status_message call is in the epoch_message_handler.
        return True

    async def general_message_handler(self, message_object: Union[AbstractMessage, Any], message_routing_key: str):
        """Forwards the message handling to the appropriate function depending on the message type."""
        await super().general_message_handler(message_object, message_routing_key)

        if not isinstance(message_object, (SimulationStateMessage, EpochMessage)):
            LOGGER.warning("Received '{:s}' message when expecting for '{:s}' or '{:s}' message".format(
                str(type(message_object)), str(SimulationStateMessage), str(EpochMessage)))

    async def epoch_message_handler(self, message_object: EpochMessage, message_routing_key: str) -> None:
        """Handles the received epoch messages."""
        if random.random() < self._receive_miss_chance:
            # Simulate a connection error by not receiving an epoch message.
            LOGGER.warning("Received epoch message was ignored.")
            return

        await super().epoch_message_handler(message_object, message_routing_key)

    async def send_status_message(self) -> None:
        """Sends a new status message to the message bus."""
        if self._latest_epoch > 0 and random.random() < self._send_miss_chance:
            # simulate connection error by not sending the status message for an epoch
            LOGGER.warning("No status message sent this time.")

        await super().send_status_message()

    async def _send_random_result_message(self):
        """Sends a result message with random values and time series to the message bus."""
        random_result_message = self._get_result_message()
        if random_result_message is None:
            await self.send_error_message("Internal error when creating result message.")
        else:
            await self._rabbitmq_client.send_message(self._result_topic, random_result_message.bytes())

    def _get_status_message(self) -> Union[StatusMessage, None]:
        """Creates a new status message and returns it in bytes format.
           Returns None, if there was a problem creating the message."""
        status_message = super()._get_status_message()
        if status_message is None:
            return None

        # Add a warning to the status message on a random chance
        if random.random() < self._warning_chance:
            LOGGER.debug("Adding a warning to the status message.")
            status_message.warnings = ["warning.internal"]

        return status_message

    def _get_result_message(self) -> Union[ResultMessage, None]:
        """Creates a new result message and returns it in bytes format.
           Returns None, if there was a problem creating the message."""
        result_message = ResultMessage.from_json({
            "Type": ResultMessage.CLASS_MESSAGE_TYPE,
            "SimulationId": self.simulation_id,
            "SourceProcessId": self.component_name,
            "MessageId": next(self._message_id_generator),
            "EpochNumber": self._latest_epoch,
            "TriggeringMessageIds": self._triggering_message_ids
        })
        if result_message is None:
            LOGGER.error("Problem with creating a result message")
            return None

        try:
            new_random_series_collection = get_all_random_series(
                self._last_result_values, self._latest_epoch_message.start_time, self._latest_epoch_message.end_time)
            self._last_result_values = get_latest_values(new_random_series_collection)
            result_message.result_values = new_random_series_collection
        except TimeSeriesError:
            LOGGER.error("Error when creating values for result message")
            return None

        return result_message


async def start_dummy_component():
    """Start a dummy component for the simulation platform."""
    dummy_component = DummyComponent()
    await asyncio.sleep(TIMEOUT_INTERVAL)
    await dummy_component.start()

    # Wait in an endless loop until the DummyComponent is stopped and sys.exit() is called.
    while True:
        await asyncio.sleep(100 * TIMEOUT_INTERVAL)


if __name__ == "__main__":
    asyncio.run(start_dummy_component())
