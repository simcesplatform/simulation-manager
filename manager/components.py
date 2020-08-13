# -*- coding: utf-8 -*-

"""This module contains a class for keeping track of the simulation components."""

import tools.tools as tools

LOGGER = tools.FullLogger(__name__)


class SimulationComponents():
    """Keeps a list of components for the simulation and the latest epoch number
       for which a ready message was received from the component."""
    NO_MESSAGES = -1
    MISSING_MESSAGE_ID = ""

    def __init__(self):
        self.__components = {}
        self.__latest_status_message_ids = []
        LOGGER.debug("New SimulationComponents object created.")

        # invariant: self.__latest_full_epoch <= for all self.__components[component_value]
        self.__latest_full_epoch = SimulationComponents.NO_MESSAGES

    def add_component(self, component_name: str):
        """Adds a new component to the simulation component list.
           If the given component_name is already in the list, the function prints an error message."""
        if component_name not in self.__components:
            self.__components[component_name] = SimulationComponents.NO_MESSAGES
            LOGGER.info("Component: {:s} registered to SimulationComponents.".format(component_name))
        else:
            LOGGER.warning("{:s} is already registered to the simulation component list".format(component_name))

    def remove_component(self, component_name: str):
        """Removes the given component from the simulation component list.
           If the given component_name is not found in the list, the function prints an error message."""
        if self.__components.pop(component_name, None) is None:
            LOGGER.warning("{:s} was not found in the simulation component list".format(component_name))
        else:
            LOGGER.info("Component: {:s} removed from SimulationComponents.".format(component_name))

        self._update_latest_full_epoch()

    def register_ready_message(self, component_name: str, epoch_number: int, status_message_id: str):
        """Registers a new ready message for the given component and epoch number."""
        if component_name not in self.__components:
            LOGGER.warning("{:s} was not found in the simulation component list".format(component_name))
        elif epoch_number < 0:
            LOGGER.warning("{:d} is not acceptable epoch number".format(epoch_number))
        elif epoch_number <= self.__components[component_name]:
            LOGGER.debug("Epoch {:d} for {:s} is not larger epoch number than the previous {:d}".format(
                epoch_number, component_name, self.__components[component_name]))
        else:
            if (epoch_number != self.__components[component_name] + 1 and
                    self.__components[component_name] != SimulationComponents.NO_MESSAGES):
                LOGGER.warning("{:d} is not the next epoch, previous was {:d}".format(
                    epoch_number, self.__components[component_name]))
            if not status_message_id:
                LOGGER.warning("Status message id should not be empty.")

            if max(self.__components.values()) < epoch_number:
                self.__latest_status_message_ids = [status_message_id]
            else:
                self.__latest_status_message_ids.append(status_message_id)

            self.__components[component_name] = epoch_number
            self._update_latest_full_epoch()
            LOGGER.debug("Ready message for epoch {:d} from component {:s} registered.".format(
                epoch_number, component_name))

    def get_component_list(self, latest_epoch_less_than=None):
        """Returns a list of the registered simulation components."""
        if latest_epoch_less_than is None:
            return list(self.__components.keys())
        return [
            component_name
            for component_name, latest_epoch_for_component in self.__components.items()
            if latest_epoch_for_component < latest_epoch_less_than
        ]

    def get_latest_epoch_for_component(self, component_name: str):
        """Returns the latest epoch number for which the component has responded with a ready message."""
        return self.__components.get(component_name, None)

    def get_latest_full_epoch(self):
        """Returns the latest epoch number for which all registered components have responded with a ready message."""
        return self.__latest_full_epoch

    def get_latest_status_message_ids(self):
        """Returns the status message ids for the latest epoch as a list.
           The ids are given in the order they have been registered."""
        return self.__latest_status_message_ids

    def __str__(self):
        """Returns a list of the component names with the latest epoch numbers given in parenthesis after each name."""
        return ", ".join([
            "{:s} ({:d})".format(component_name, latest_epoch_for_component)
            for component_name, latest_epoch_for_component in self.__components.items()
        ])

    def _update_latest_full_epoch(self):
        """Updates the value for the latest full epoch."""
        self.__latest_full_epoch = min(self.__components.values())
