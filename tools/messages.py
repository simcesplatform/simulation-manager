# -*- coding: utf-8 -*-

"""This module contains a class for creating and holding messages for the RabbitMQ message bus."""

import json

from tools.exceptions.messages import MessageDateError, MessageIdError, MessageSourceError, MessageTypeError, \
                                      MessageValueError, MessageEpochValueError, MessageStateValueError
from tools.datetime_tools import get_utcnow_in_milliseconds, to_iso_format_datetime_string
from tools.tools import FullLogger

LOGGER = FullLogger(__name__)


def get_next_message_id(source_process_id: str, start_number=1):
    """Generator for getting unique message ids."""
    message_number = start_number
    while True:
        yield "{:s}-{:d}".format(source_process_id, message_number)
        message_number += 1


def get_json(message_object):
    """Returns a JSON based on the values of the given message_object and the attribute parameters."""
    return {
        json_attribute_name: getattr(message_object, object_attribute_name)
        for json_attribute_name, object_attribute_name in message_object.__class__.MESSAGE_ATTRIBUTES_FULL.items()
        if (json_attribute_name not in message_object.__class__.OPTIONAL_ATTRIBUTES_FULL or
            getattr(message_object, object_attribute_name) is not None)
    }


def validate_json(message_class, json_message: dict):
    """Validates the given the given json object for the attributes covered in AbstractMessage class.
        Returns True if the message is ok. Otherwise, return False."""
    for json_attribute_name, object_attribute_name in message_class.MESSAGE_ATTRIBUTES_FULL.items():
        if (json_attribute_name not in json_message and
                json_attribute_name not in message_class.OPTIONAL_ATTRIBUTES_FULL):
            LOGGER.warning("%s attribute is missing from the message", json_attribute_name)
            return False

        if not getattr(
                message_class,
                "_".join(["_check", object_attribute_name]))(json_message.get(json_attribute_name, None)):
            # TODO: handle checking for missing timezone information
            LOGGER.warning(
                "'%s' is not valid message value for %s",
                str(json_message[json_attribute_name]), json_attribute_name)
            return False

    return True


class AbstractMessage():
    """The abstract message class that contains the attributes that all messages have."""
    # The allowed message types
    MESSAGE_ENCODING = "UTF-8"
    CLASS_MESSAGE_TYPE = ""

    MESSAGE_TYPES = ["SimState", "Epoch", "Error", "Status", "Result", "General"]
    # The relationships between the JSON attributes and the object properties
    MESSAGE_ATTRIBUTES = {
        "Type": "message_type",
        "SimulationId": "simulation_id",
        "SourceProcessId": "source_process_id",
        "MessageId": "message_id",
        "Timestamp": "timestamp"
    }
    OPTIONAL_ATTRIBUTES = []

    MESSAGE_ATTRIBUTES_FULL = MESSAGE_ATTRIBUTES
    OPTIONAL_ATTRIBUTES_FULL = OPTIONAL_ATTRIBUTES

    def __init__(self, **kwargs):
        """Only attributes "Type", "SimulationId", SourceProcessId", MessageId", and "Timestamp" are considered."""
        for json_attribute_name in AbstractMessage.MESSAGE_ATTRIBUTES:
            setattr(self, AbstractMessage.MESSAGE_ATTRIBUTES[json_attribute_name],
                    kwargs.get(json_attribute_name, None))

    @property
    def message_type(self):
        """The message type attribute."""
        return self.__message_type

    @property
    def simulation_id(self):
        """The simulation id."""
        return self.__simulation_id

    @property
    def source_process_id(self):
        """The source process id."""
        return self.__source_process_id

    @property
    def message_id(self):
        """The message id."""
        return self.__message_id

    @property
    def timestamp(self):
        """The timestamp for the message."""
        return self.__timestamp

    @message_type.setter
    def message_type(self, message_type):
        if not self._check_message_type(message_type):
            raise MessageTypeError("'{:s}' is not an allowed message type".format(str(message_type)))
        self.__message_type = message_type

    @simulation_id.setter
    def simulation_id(self, simulation_id):
        if not self._check_simulation_id(simulation_id):
            raise MessageDateError("'{:s}' is an invalid datetime".format(str(simulation_id)))
        self.__simulation_id = to_iso_format_datetime_string(simulation_id)

    @source_process_id.setter
    def source_process_id(self, source_process_id):
        if not self._check_source_process_id(source_process_id):
            raise MessageSourceError("'{:s}' is an invalid source process id".format(str(source_process_id)))
        self.__source_process_id = source_process_id

    @message_id.setter
    def message_id(self, message_id):
        if not self._check_message_id(message_id):
            raise MessageIdError("'{:s}' is an invalid message id".format(str(message_id)))
        self.__message_id = message_id

    @timestamp.setter
    def timestamp(self, timestamp):
        if timestamp is None:
            self.__timestamp = get_utcnow_in_milliseconds()
        else:
            if not self._check_timestamp(timestamp):
                raise MessageDateError("'{:s}' is an invalid datetime".format(str(timestamp)))
            self.__timestamp = to_iso_format_datetime_string(timestamp)

    @classmethod
    def _check_datetime(cls, datetime_value):
        return to_iso_format_datetime_string(datetime_value) is not None

    @classmethod
    def _check_message_type(cls, message_type):
        return message_type in cls.MESSAGE_TYPES

    @classmethod
    def _check_simulation_id(cls, simulation_id):
        return cls._check_datetime(simulation_id)

    @classmethod
    def _check_source_process_id(cls, source_process_id):
        return isinstance(source_process_id, str) and len(source_process_id) > 0

    @classmethod
    def _check_message_id(cls, message_id):
        return isinstance(message_id, str) and len(message_id) > 0

    @classmethod
    def _check_timestamp(cls, timestamp):
        return cls._check_datetime(timestamp)

    def json(self):
        """Returns the message as a JSON object."""
        return get_json(self)

    def bytes(self):
        """Returns the message in bytes format."""
        return bytes(json.dumps(self.json()), encoding=AbstractMessage.MESSAGE_ENCODING)

    @classmethod
    def validate_json(cls, json_message: dict):
        """Validates the given the given json object for the attributes covered in AbstractMessage class.
           Returns True if the message is ok. Otherwise, return False."""
        return validate_json(cls, json_message)

    @classmethod
    def from_json(cls, json_message: dict):
        """Returns a class object created based on the given JSON attributes.
           If the given JSON is not validated returns None."""
        if cls.validate_json(json_message):
            return cls(**json_message)
        return None


class AbstractResultMessage(AbstractMessage):
    """The abstract result message class that contains the attributes that all result messages have."""
    CLASS_MESSAGE_TYPE = ""

    MESSAGE_ATTRIBUTES = {
        "EpochNumber": "epoch_number",
        "LastUpdatedInEpoch": "last_updated_in_epoch",
        "TriggeringMessageIds": "triggering_message_ids",
        "Warnings": "warnings"
    }
    OPTIONAL_ATTRIBUTES = ["LastUpdatedInEpoch", "Warnings"]

    MESSAGE_ATTRIBUTES_FULL = {
        **AbstractMessage.MESSAGE_ATTRIBUTES_FULL,
        **MESSAGE_ATTRIBUTES
    }
    OPTIONAL_ATTRIBUTES_FULL = AbstractMessage.OPTIONAL_ATTRIBUTES_FULL + OPTIONAL_ATTRIBUTES

    WARNING_TYPES = [
        "warning.convergence",
        "warning.input",
        "warning.input.range",
        "warning.input.unreliable",
        "warning.internal",
        "warning.other"
    ]

    def __init__(self, **kwargs):
        """Only attributes in AbstractResultMessage.MESSAGE_ATTRIBUTES_FULL are considered."""
        super().__init__(**kwargs)
        for json_attribute_name in AbstractResultMessage.MESSAGE_ATTRIBUTES:
            setattr(self, AbstractResultMessage.MESSAGE_ATTRIBUTES[json_attribute_name],
                    kwargs.get(json_attribute_name, None))

    @property
    def epoch_number(self):
        """The epoch number attribute."""
        return self.__epoch_number

    @property
    def last_updated_in_epoch(self):
        """The last updated in epoch attribute. It is either an epoch number or None."""
        return self.__last_updated_in_epoch

    @property
    def triggering_message_ids(self):
        """The triggering message ids attribute. It is a non-empty list."""
        return self.__triggering_message_ids

    @property
    def warnings(self):
        """The warnings attribute. It is either None or a non-empty list."""
        return self.__warnings

    @epoch_number.setter
    def epoch_number(self, epoch_number):
        if not self._check_epoch_number(epoch_number):
            raise MessageEpochValueError("'{:s}' is not a valid epoch number".format(str(epoch_number)))
        self.__epoch_number = epoch_number

    @last_updated_in_epoch.setter
    def last_updated_in_epoch(self, last_updated_in_epoch):
        if not self._check_last_updated_in_epoch(last_updated_in_epoch):
            raise MessageEpochValueError("'{:s}' is not a valid epoch number".format(str(last_updated_in_epoch)))
        self.__last_updated_in_epoch = last_updated_in_epoch

    @triggering_message_ids.setter
    def triggering_message_ids(self, triggering_message_ids):
        if not self._check_triggering_message_ids(triggering_message_ids):
            raise MessageIdError("'{:s}' is not a valid list of message ids".format(str(triggering_message_ids)))
        self.__triggering_message_ids = triggering_message_ids

    @warnings.setter
    def warnings(self, warnings):
        if not self._check_warnings(warnings):
            raise MessageValueError("'{:s}' is not a valid list of warnings".format(str(warnings)))
        if isinstance(warnings, (list, tuple)) and len(warnings) == 0:
            self.__warnings = None
        else:
            self.__warnings = warnings

    @classmethod
    def _check_epoch_number(cls, epoch_number):
        # epoch number 0 is reserved for the initialization phase
        return isinstance(epoch_number, int) and epoch_number >= 0

    @classmethod
    def _check_last_updated_in_epoch(cls, last_updated_in_epoch):
        return last_updated_in_epoch is None or cls._check_epoch_number(last_updated_in_epoch)

    @classmethod
    def _check_triggering_message_ids(cls, triggering_message_ids):
        if (triggering_message_ids is None or
                not isinstance(triggering_message_ids, (list, tuple)) or
                len(triggering_message_ids) == 0):
            return False
        for triggering_message_id in triggering_message_ids:
            if not cls._check_message_id(triggering_message_id):
                return False
        return True

    @classmethod
    def _check_warnings(cls, warnings):
        if warnings is None:
            return True
        if not isinstance(warnings, (list, tuple)):
            return False
        for warning in warnings:
            if warning not in cls.WARNING_TYPES:
                return False
        return True


class SimulationStateMessage(AbstractMessage):
    """Class containing all the attributes for a simulation state message."""
    CLASS_MESSAGE_TYPE = "SimState"

    MESSAGE_ATTRIBUTES = {
        "SimulationState": "simulation_state"
    }
    OPTIONAL_ATTRIBUTES = []

    MESSAGE_ATTRIBUTES_FULL = {
        **AbstractMessage.MESSAGE_ATTRIBUTES_FULL,
        **MESSAGE_ATTRIBUTES
    }
    OPTIONAL_ATTRIBUTES_FULL = AbstractMessage.OPTIONAL_ATTRIBUTES_FULL + OPTIONAL_ATTRIBUTES

    SIMULATION_STATES = [
        "running",
        "stopped"
    ]

    def __init__(self, **kwargs):
        """Only attributes in class SimulationStateMessage.MESSAGE_ATTRIBUTES_FULL are considered."""
        super().__init__(**kwargs)
        for json_attribute_name in SimulationStateMessage.MESSAGE_ATTRIBUTES:
            setattr(self, SimulationStateMessage.MESSAGE_ATTRIBUTES[json_attribute_name],
                    kwargs.get(json_attribute_name, None))

    @property
    def simulation_state(self):
        """The simulation state attribute."""
        return self.__simulation_state

    @simulation_state.setter
    def simulation_state(self, simulation_state):
        if not self._check_simulation_state(simulation_state):
            raise MessageStateValueError("'{:s}' is not a valid value for simulation state".format(
                str(simulation_state)))
        self.__simulation_state = simulation_state

    @classmethod
    def _check_simulation_state(cls, simulation_state):
        return simulation_state in cls.SIMULATION_STATES


class EpochMessage(AbstractResultMessage):
    """Class containing all the attributes for a epoch message."""
    CLASS_MESSAGE_TYPE = "Epoch"

    MESSAGE_ATTRIBUTES = {
        "StartTime": "start_time",
        "EndTime": "end_time"
    }
    OPTIONAL_ATTRIBUTES = []

    MESSAGE_ATTRIBUTES_FULL = {
        **AbstractResultMessage.MESSAGE_ATTRIBUTES_FULL,
        **MESSAGE_ATTRIBUTES
    }
    OPTIONAL_ATTRIBUTES_FULL = AbstractResultMessage.OPTIONAL_ATTRIBUTES_FULL + OPTIONAL_ATTRIBUTES

    def __init__(self, **kwargs):
        """Only attributes in class EpochMessage.MESSAGE_ATTRIBUTES_FULL are considered."""
        super().__init__(**kwargs)
        for json_attribute_name in EpochMessage.MESSAGE_ATTRIBUTES:
            setattr(self, EpochMessage.MESSAGE_ATTRIBUTES[json_attribute_name],
                    kwargs.get(json_attribute_name, None))

    @property
    def start_time(self):
        """The attribute for the start time of the epoch."""
        return self.__start_time

    @property
    def end_time(self):
        """The attribute for the end time of the epoch."""
        return self.__end_time

    @start_time.setter
    def start_time(self, start_time):
        if not self._check_start_time(start_time):
            raise MessageDateError("'{:s}' is an invalid datetime".format(str(start_time)))

        new_start_time = to_iso_format_datetime_string(start_time)
        if getattr(self, "end_time", None) is not None and new_start_time >= self.end_time:
            raise MessageValueError("Epoch start time ({:s}) should be before the end time ({:s})".format(
                new_start_time, self.end_time))
        self.__start_time = to_iso_format_datetime_string(start_time)

    @end_time.setter
    def end_time(self, end_time):
        if not self._check_end_time(end_time):
            raise MessageDateError("'{:s}' is an invalid datetime".format(str(end_time)))

        new_end_time = to_iso_format_datetime_string(end_time)
        if getattr(self, "start_time", None) is not None and new_end_time <= self.start_time:
            raise MessageValueError("Epoch end time ({:s}) should be after the start time ({:s})".format(
                new_end_time, self.start_time))
        self.__end_time = new_end_time

    @classmethod
    def _check_start_time(cls, start_time):
        return cls._check_datetime(start_time)

    @classmethod
    def _check_end_time(cls, end_time):
        return cls._check_datetime(end_time)


class StatusMessage(AbstractResultMessage):
    """Class containing all the attributes for a status message."""
    CLASS_MESSAGE_TYPE = "Status"

    MESSAGE_ATTRIBUTES = {
        "Value": "value"
    }
    OPTIONAL_ATTRIBUTES = []

    STATUS_VALUES = ["ready"]

    MESSAGE_ATTRIBUTES_FULL = {
        **AbstractResultMessage.MESSAGE_ATTRIBUTES_FULL,
        **MESSAGE_ATTRIBUTES
    }
    OPTIONAL_ATTRIBUTES_FULL = AbstractResultMessage.OPTIONAL_ATTRIBUTES_FULL + OPTIONAL_ATTRIBUTES

    def __init__(self, **kwargs):
        """Only attributes in class StatusMessage.MESSAGE_ATTRIBUTES_FULL are considered."""
        super().__init__(**kwargs)
        for json_attribute_name in StatusMessage.MESSAGE_ATTRIBUTES:
            setattr(self, StatusMessage.MESSAGE_ATTRIBUTES[json_attribute_name],
                    kwargs.get(json_attribute_name, None))

    @property
    def value(self):
        """The value attribute containing the status value."""
        return self.__value

    @value.setter
    def value(self, value):
        if not self._check_value(value):
            raise MessageValueError("'{:s}' is an invalid status value".format(str(value)))

        self.__value = value

    @classmethod
    def _check_value(cls, value):
        return isinstance(value, str) and value in cls.STATUS_VALUES


class ErrorMessage(AbstractResultMessage):
    """Class containing all the attributes for an error message."""
    CLASS_MESSAGE_TYPE = "Error"

    MESSAGE_ATTRIBUTES = {
        "Description": "description"
    }
    OPTIONAL_ATTRIBUTES = []

    MESSAGE_ATTRIBUTES_FULL = {
        **AbstractResultMessage.MESSAGE_ATTRIBUTES_FULL,
        **MESSAGE_ATTRIBUTES
    }
    OPTIONAL_ATTRIBUTES_FULL = AbstractResultMessage.OPTIONAL_ATTRIBUTES_FULL + OPTIONAL_ATTRIBUTES

    def __init__(self, **kwargs):
        """Only attributes in class ErrorMessage.MESSAGE_ATTRIBUTES_FULL are considered."""
        super().__init__(**kwargs)
        for json_attribute_name in ErrorMessage.MESSAGE_ATTRIBUTES:
            setattr(self, ErrorMessage.MESSAGE_ATTRIBUTES[json_attribute_name],
                    kwargs.get(json_attribute_name, None))

    @property
    def description(self):
        """The description attribute containing an elaborate description for the error."""
        return self.__description

    @description.setter
    def description(self, description):
        if not self._check_description(description):
            raise MessageValueError("'{:s}' is an invalid error description".format(str(description)))

        self.__description = description

    @classmethod
    def _check_description(cls, description):
        return isinstance(description, str) and len(description) > 0


class ResultMessage(AbstractResultMessage):
    """Class for a generic result message containing at least all the required attributes."""
    CLASS_MESSAGE_TYPE = "Result"

    MESSAGE_ATTRIBUTES = {}
    OPTIONAL_ATTRIBUTES = []

    MESSAGE_ATTRIBUTES_FULL = {
        **AbstractResultMessage.MESSAGE_ATTRIBUTES_FULL,
        **MESSAGE_ATTRIBUTES
    }
    OPTIONAL_ATTRIBUTES_FULL = AbstractResultMessage.OPTIONAL_ATTRIBUTES_FULL + OPTIONAL_ATTRIBUTES

    def __init__(self, **kwargs):
        """All the given attributes are considered."""
        super().__init__(**kwargs)
        for json_attribute_name in ResultMessage.MESSAGE_ATTRIBUTES:
            setattr(self, ResultMessage.MESSAGE_ATTRIBUTES[json_attribute_name],
                    kwargs.get(json_attribute_name, None))

        self.result_values = {
            value_attribute_name: value_attribute_value
            for value_attribute_name, value_attribute_value in kwargs.items()
            if value_attribute_name not in self.__class__.MESSAGE_ATTRIBUTES_FULL
        }

    @property
    def result_values(self):
        """A dictionary containing all the result value attributes."""
        return self.__result_values

    @result_values.setter
    def result_values(self, result_values):
        if not self._check_result_values(result_values):
            raise MessageValueError("'{:s}' is an invalid result value dictionary".format(str(result_values)))

        self.__result_values = result_values

    @classmethod
    def _check_result_values(cls, result_values):
        return isinstance(result_values, dict)

    def json(self):
        """Returns the message as a JSON object."""
        return {**get_json(self), **self.result_values}


class GeneralMessage(AbstractMessage):
    """Class for a generic message containing at least all the required attributes from AbstractMessage.
       Useful when making general use message listeners."""
    CLASS_MESSAGE_TYPE = "General"

    MESSAGE_ATTRIBUTES = {}
    OPTIONAL_ATTRIBUTES = []

    MESSAGE_ATTRIBUTES_FULL = {
        **AbstractMessage.MESSAGE_ATTRIBUTES_FULL,
        **MESSAGE_ATTRIBUTES
    }
    OPTIONAL_ATTRIBUTES_FULL = AbstractMessage.OPTIONAL_ATTRIBUTES_FULL + OPTIONAL_ATTRIBUTES

    def __init__(self, **kwargs):
        """All the given attributes are considered."""
        super().__init__(**kwargs)
        for json_attribute_name in GeneralMessage.MESSAGE_ATTRIBUTES:
            setattr(self, GeneralMessage.MESSAGE_ATTRIBUTES[json_attribute_name],
                    kwargs.get(json_attribute_name, None))

        self.general_attributes = {
            general_attribute_name: general_attribute_value
            for general_attribute_name, general_attribute_value in kwargs.items()
            if general_attribute_name not in self.__class__.MESSAGE_ATTRIBUTES_FULL
        }

    @property
    def general_attributes(self):
        """A dictionary containing all the optional attributes."""
        return self.__general_attributes

    @general_attributes.setter
    def general_attributes(self, general_attributes):
        if not self._check_general_attributes(general_attributes):
            raise MessageValueError("'{:s}' is an invalid general attribute dictionary".format(
                str(general_attributes)))

        self.__general_attributes = general_attributes

    @classmethod
    def _check_general_attributes(cls, general_attributes):
        return isinstance(general_attributes, dict)

    def json(self):
        """Returns the message as a JSON object."""
        return {**get_json(self), **self.general_attributes}
