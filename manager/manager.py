# -*- coding: utf-8 -*-

"""This module contains the main Simulation Manager code for the Simulation platform."""

import asyncio
import datetime
from typing import cast, Any, Union

from manager.components import SimulationComponents
from tools.clients import RabbitmqClient
from tools.datetime_tools import to_utc_datetime_object
from tools.messages import AbstractMessage, EpochMessage, StatusMessage, SimulationStateMessage, \
                           get_next_message_id
from tools.timer import Timer
from tools.tools import FullLogger, load_environmental_variables

LOGGER = FullLogger(__name__)

# The time interval in seconds that is waited after the start before the simulation state message "running" is sent.
# Also used as the time interval that is waited before closing after ending the simulation.
TIMEOUT_INTERVAL = 10

# The names of the environmental variables used by the component.
__SIMULATION_ID = "SIMULATION_ID"
__SIMULATION_MANAGER_NAME = "SIMULATION_MANAGER_NAME"

__SIMULATION_EPOCH_MESSAGE_TOPIC = "SIMULATION_EPOCH_MESSAGE_TOPIC"
__SIMULATION_STATUS_MESSAGE_TOPIC = "SIMULATION_STATUS_MESSAGE_TOPIC"
__SIMULATION_STATE_MESSAGE_TOPIC = "SIMULATION_STATE_MESSAGE_TOPIC"
__SIMULATION_ERROR_MESSAGE_TOPIC = "SIMULATION_ERROR_MESSAGE_TOPIC"

__SIMULATION_COMPONENTS = "SIMULATION_COMPONENTS"
__SIMULATION_NAME = "SIMULATION_NAME"
__SIMULATION_DESCRIPTION = "SIMULATION_DESCRIPTION"

__SIMULATION_INITIAL_START_TIME = "SIMULATION_INITIAL_START_TIME"
__SIMULATION_EPOCH_LENGTH = "SIMULATION_EPOCH_LENGTH"
__SIMULATION_MAX_EPOCHS = "SIMULATION_MAX_EPOCHS"
__SIMULATION_EPOCH_TIMER_INTERVAL = "SIMULATION_EPOCH_TIMER_INTERVAL"
__SIMULATION_MAX_EPOCH_RESENDS = "SIMULATION_MAX_EPOCH_RESENDS"


class SimulationManager:
    """Class that holds the state of the simulation manager."""
    SIMULATION_STATE_VALUE_RUNNING = SimulationStateMessage.SIMULATION_STATES[0]   # "running"
    SIMULATION_STATE_VALUE_STOPPED = SimulationStateMessage.SIMULATION_STATES[-1]  # "stopped"

    READY_STATUS = StatusMessage.STATUS_VALUES[0]   # "ready"
    ERROR_STATUS = StatusMessage.STATUS_VALUES[-1]  # "error"

    def __init__(self, simulation_id: str, manager_name: str, simulation_name: str, simulation_description: str,
                 simulation_components: str, initial_start_time: str, epoch_length: int, max_epochs: int,
                 epoch_timer_interval: float, max_epoch_resends: int,
                 epoch_topic: str, state_topic: str, status_topic: str, error_topic: str):
        self.__rabbitmq_client = RabbitmqClient()
        self.__simulation_id = simulation_id
        self.__manager_name = manager_name
        self.__simulation_name = simulation_name
        self.__simulation_description = simulation_description
        self.__is_stopped = True

        self.__simulation_components = SimulationComponents()
        for component_name in cast(str, simulation_components.split(",")):
            self.__simulation_components.add_component(component_name)

        self.__simulation_state = SimulationManager.SIMULATION_STATE_VALUE_STOPPED
        self.__epoch_number = 0
        self.__epoch_length = epoch_length
        self.__max_epochs = max_epochs

        # epoch timer is used to resend the epoch messages after set time interval
        self.__epoch_timer_interval = epoch_timer_interval
        self.__epoch_timer = None
        self.__max_epoch_resends = max_epoch_resends
        self.__epoch_resends = 0

        self.__current_start_time = to_utc_datetime_object(initial_start_time)
        self.__current_end_time = None

        self.__epoch_topic = epoch_topic
        self.__state_topic = state_topic
        self.__status_topic = status_topic
        self.__error_topic = error_topic

        self.__message_id_generator = get_next_message_id(self.manager_name)

        self.__rabbitmq_client.add_listener(
            [
                self.__status_topic,
                self.__error_topic
            ],
            self.general_message_handler)

    @property
    def is_stopped(self) -> bool:
        """Returns True, if the simulation is stopped."""
        return self.__is_stopped

    async def start(self):
        """Starts the simulation. Sends a simulation state message."""
        LOGGER.info("Starting the simulation.")
        await self.set_simulation_state(SimulationManager.SIMULATION_STATE_VALUE_RUNNING)
        self.__is_stopped = False

    async def stop(self):
        """Stops the simulation. Sends a simulation state message to the message bus."""
        LOGGER.info("Stopping the simulation.")
        await self.__stop_epoch_timer()
        self.__simulation_state = SimulationManager.SIMULATION_STATE_VALUE_STOPPED
        await self.send_state_message(start_timer=False)
        await self.__rabbitmq_client.close()
        self.__is_stopped = True

    @property
    def simulation_id(self) -> str:
        """The simulation ID for the simulation."""
        return self.__simulation_id

    @property
    def manager_name(self) -> str:
        """The simulation manager name."""
        return self.__manager_name

    @property
    def epoch_number(self) -> int:
        """The name of the omega component in the simulation."""
        return self.__epoch_number

    @property
    def max_epochs(self) -> int:
        """The maximum number of epochs for the simulation."""
        return self.__max_epochs

    def get_simulation_state(self) -> str:
        """Return the simulation state attribute."""
        return self.__simulation_state

    async def set_simulation_state(self, new_simulation_state):
        """Sets the simulation state. Sends a simulation state message to the message bus.
           If the new simulation state is "stopped", stops the entire the simulation."""
        if new_simulation_state in (
                SimulationManager.SIMULATION_STATE_VALUE_RUNNING,
                SimulationManager.SIMULATION_STATE_VALUE_STOPPED):
            self.__simulation_state = new_simulation_state
            if new_simulation_state == SimulationManager.SIMULATION_STATE_VALUE_RUNNING:
                await self.send_state_message()
            elif new_simulation_state == SimulationManager.SIMULATION_STATE_VALUE_STOPPED:
                await self.stop()

    async def check_components(self):
        """Checks the status of the simulation components and sends a new epoch message if needed."""
        latest_full_epoch = self.__simulation_components.get_latest_full_epoch()

        if self.get_simulation_state() == SimulationManager.SIMULATION_STATE_VALUE_RUNNING:
            if latest_full_epoch == self.__epoch_number:
                if self.__simulation_components.is_in_normal_state():
                    # the current epoch is finished => send a new epoch message
                    await self.__send_epoch_message()
                else:
                    LOGGER.error("Stopping the simulation because one of the components is in an error state.")
                    await self.stop()

    async def send_state_message(self, start_timer: bool = True):
        """Sends a simulation state message."""
        LOGGER.debug("Sending state message: '{:s}'".format(self.get_simulation_state()))

        new_simulation_state_message = self.__get_simulation_state_message()
        if new_simulation_state_message is None:
            # So serious error that even the simulation state message could not be created => stop the manager.
            LOGGER.error("Simulation manager exiting due to internal error")
            await self.stop()
        else:
            await self.__rabbitmq_client.send_message(self.__state_topic, new_simulation_state_message)
            if start_timer:
                await self.__start_epoch_timer()

    async def general_message_handler(self, message_object: Union[AbstractMessage, Any], message_routing_key: str):
        """Forwards the message handling to the appropriate function depending on the message type."""
        if isinstance(message_object, StatusMessage):
            await self.status_message_handler(message_object, message_routing_key)
        else:
            LOGGER.warning("Received '{:s}' message when expecting for '{:s}' message".format(
                str(type(message_object)), str(StatusMessage)))

    async def status_message_handler(self, message_object: StatusMessage, message_routing_key: str):
        """Handles received status message. After receiving a proper status message checks
           if all components have registered for the epoch and a new epoch could be started."""
        if message_object.simulation_id != self.simulation_id:
            LOGGER.info("Received a status message for a different simulation: '{:s}' instead of '{:s}'".format(
                message_object.simulation_id, self.simulation_id))
        elif message_object.message_type != StatusMessage.CLASS_MESSAGE_TYPE:
            LOGGER.warning("Received a status message with wrong message type: '{:s}' instead of '{:s}'".format(
                message_object.message_type, StatusMessage.CLASS_MESSAGE_TYPE))
        # elif message_object.value != SimulationManager.READY_STATUS:
        #     LOGGER.warning("Received a status message with an unknown value: '{:s}' instead of '{:s}'".format(
        #         message_object.value, SimulationManager.READY_STATUS))
        elif message_object.source_process_id != self.__manager_name:
            LOGGER.debug("Received a status message from {:s} at topic {:s}".format(
                message_object.source_process_id, message_routing_key))
            if message_object.warnings:
                # TODO: Implement actual handling of warnings instead of just logging them.
                LOGGER.warning("Status message from '{:s}' contained warnings: {:s}".format(
                    message_object.source_process_id, ", ".join(message_object.warnings)))

            if message_object.value == SimulationManager.READY_STATUS:
                self.__simulation_components.register_status_message(
                    message_object.source_process_id, message_object.epoch_number, message_object.message_id, False)
            elif message_object.value == SimulationManager.ERROR_STATUS:
                LOGGER.debug("Received an error message from {:s} with description '{:s}' at topic {:s}".format(
                    message_object.source_process_id, message_object.description, message_routing_key))
                self.__simulation_components.register_status_message(
                    message_object.source_process_id, message_object.epoch_number, message_object.message_id, True)
                if self.__epoch_number >= 1:
                    # Don't stop the simulation immediately if it is still in the initialization phase (epoch == 0)
                    LOGGER.error("Stopping the simulation because one of the components is in an error state.")
                    await self.stop()

            await self.check_components()

    async def __send_epoch_message(self, new_epoch: bool = True):
        """Sends an epoch message to the message bus.
           If new_epoch is True or the first epoch has not been started yet, starts a new epoch.
           Otherwise, resends the epoch message for the current epoch.
           Stops the simulation if the maximum number of epochs or epoch message resends has been reached.
        """
        if new_epoch or self.epoch_number == 0:
            self.__epoch_number += 1
            self.__epoch_resends = 0
            if self.__current_end_time is not None:
                self.__current_start_time = self.__current_end_time
            self.__current_end_time = self.__current_start_time + datetime.timedelta(seconds=self.__epoch_length)

        if self.epoch_number <= self.max_epochs and self.__epoch_resends <= self.__max_epoch_resends:
            if new_epoch:
                LOGGER.info("Starting Epoch {:d}".format(self.__epoch_number))
            else:
                LOGGER.info("Resending (try {:d}) epoch message for Epoch {:d}".format(
                    self.__epoch_resends, self.__epoch_number))

            new_epoch_message = self.__get_epoch_message()
            if new_epoch_message is None:
                LOGGER.error("Simulation manager stopping the simulation due to internal error.")
                await self.stop()
            else:
                await self.__rabbitmq_client.send_message(self.__epoch_topic, new_epoch_message)
                await self.__start_epoch_timer()

        else:
            await self.stop()

    def __get_simulation_state_message(self) -> Union[bytes, None]:
        """Creates a new simulation state message and returns it in bytes format.
           If there is a problem creating the message, returns None."""
        state_message = SimulationStateMessage.from_json({
            "Type": SimulationStateMessage.CLASS_MESSAGE_TYPE,
            "SimulationId": self.simulation_id,
            "SourceProcessId": self.manager_name,
            "MessageId": next(self.__message_id_generator),
            "SimulationState": self.get_simulation_state(),
            "Name": self.__simulation_name,
            "Description": self.__simulation_description
        })
        if state_message is None:
            LOGGER.error("Problem with creating a simulation state message")
            return None

        return state_message.bytes()

    def __get_epoch_message(self) -> Union[bytes, None]:
        """Creates a new epoch message and returns it in bytes format.
           If there is a problem creating the message, returns None."""
        epoch_message = EpochMessage.from_json({
            "Type": EpochMessage.CLASS_MESSAGE_TYPE,
            "SimulationId": self.simulation_id,
            "SourceProcessId": self.manager_name,
            "MessageId": next(self.__message_id_generator),
            "EpochNumber": self.epoch_number,
            "TriggeringMessageIds": self.__simulation_components.get_latest_status_message_ids(),
            "StartTime": self.__current_start_time,
            "EndTime": self.__current_end_time
        })
        if epoch_message is None:
            LOGGER.error("Problem with creating a epoch message")
            return None

        return epoch_message.bytes()

    async def __start_epoch_timer(self):
        """Starts the epoch timer that is used to resend the epoch message for the running epoch
           after the timer has run out."""
        await self.__stop_epoch_timer()
        self.__epoch_timer = Timer(
            is_repeating=False,
            timeout=self.__epoch_timer_interval * (self.__epoch_resends + 1),
            callback=self.__epoch_timer_handler)

    async def __stop_epoch_timer(self):
        """Stops the epoch timer."""
        if self.__epoch_timer is not None and self.__epoch_timer.is_running():
            await self.__epoch_timer.cancel()

    async def __epoch_timer_handler(self):
        """This is launched if the components in the simulation have not responded to the manager
           within EPOCH_TIMER_INTERVAL seconds.
           The function resends the epoch message for the current epoch,
           or the simulation state message at the beginning of the simulation."""
        if self.get_simulation_state() == SimulationManager.SIMULATION_STATE_VALUE_RUNNING:
            if self.__epoch_resends >= self.__max_epoch_resends:
                LOGGER.info("Maximum number of epoch resends reached for epoch {:d}".format(self.__epoch_number))
                await self.stop()
                return

            self.__epoch_resends += 1
            if self.epoch_number > 0:
                await self.__send_epoch_message(new_epoch=False)
            else:
                await self.send_state_message()


async def start_manager():
    """Starts the Simulation manager process."""
    env_variables = load_environmental_variables(
        (__SIMULATION_ID, str),
        (__SIMULATION_MANAGER_NAME, str, "manager"),
        (__SIMULATION_COMPONENTS, str, ""),
        (__SIMULATION_NAME, str, ""),
        (__SIMULATION_DESCRIPTION, str, ""),
        (__SIMULATION_EPOCH_MESSAGE_TOPIC, str, "Epoch"),
        (__SIMULATION_STATUS_MESSAGE_TOPIC, str, "Status.Ready"),
        (__SIMULATION_STATE_MESSAGE_TOPIC, str, "SimState"),
        (__SIMULATION_ERROR_MESSAGE_TOPIC, str, "Status.Error"),
        (__SIMULATION_EPOCH_LENGTH, int, 3600),
        (__SIMULATION_INITIAL_START_TIME, str, "2020-01-01T00:00:00.000Z"),
        (__SIMULATION_MAX_EPOCHS, int, 5),
        (__SIMULATION_EPOCH_TIMER_INTERVAL, float, 120.0),
        (__SIMULATION_MAX_EPOCH_RESENDS, int, 5)
    )

    # cast()-function added here to allow static linter to recognize the correct types, cast itself does nothing
    manager = SimulationManager(
        simulation_id=cast(str, env_variables[__SIMULATION_ID]),
        manager_name=cast(str, env_variables[__SIMULATION_MANAGER_NAME]),
        simulation_name=cast(str, env_variables[__SIMULATION_NAME]),
        simulation_description=cast(str, env_variables[__SIMULATION_DESCRIPTION]),
        simulation_components=cast(str, env_variables[__SIMULATION_COMPONENTS]),
        initial_start_time=cast(str, env_variables[__SIMULATION_INITIAL_START_TIME]),
        epoch_length=cast(int, env_variables[__SIMULATION_EPOCH_LENGTH]),
        max_epochs=cast(int, env_variables[__SIMULATION_MAX_EPOCHS]),
        epoch_timer_interval=cast(float, env_variables[__SIMULATION_EPOCH_TIMER_INTERVAL]),
        epoch_topic=cast(str, env_variables[__SIMULATION_EPOCH_MESSAGE_TOPIC]),
        max_epoch_resends=cast(int, env_variables[__SIMULATION_MAX_EPOCH_RESENDS]),
        state_topic=cast(str, env_variables[__SIMULATION_STATE_MESSAGE_TOPIC]),
        status_topic=cast(str, env_variables[__SIMULATION_STATUS_MESSAGE_TOPIC]),
        error_topic=cast(str, env_variables[__SIMULATION_ERROR_MESSAGE_TOPIC]))

    # Wait a bit to allow other components to initialize and then start the simulation.
    await asyncio.sleep(TIMEOUT_INTERVAL)
    await manager.start()

    # Wait in an endless loop until the SimulationManager is stopped or sys.exit() is called.
    while not manager.is_stopped:
        await asyncio.sleep(TIMEOUT_INTERVAL)

    # Wait a few seconds extra to allow for certain the last simulation state message to be sent.
    await asyncio.sleep(TIMEOUT_INTERVAL / 2)


if __name__ == "__main__":
    asyncio.run(start_manager())
