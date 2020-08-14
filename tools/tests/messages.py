# -*- coding: utf-8 -*-

"""Unit test for the message module."""

import copy
import datetime
import json
import unittest

import tools.exceptions.messages
import tools.messages
from tools.datetime_tools import get_utcnow_in_milliseconds, to_utc_datetime_object
from tools.tools import FullLogger

LOGGER = FullLogger(__name__)

MESSAGE_TYPE_ATTRIBUTE = "Type"
TIMESTAMP_ATTRIBUTE = "Timestamp"
SIMULATION_ID_ATTRIBUTE = "SimulationId"
SOURCE_PROCESS_ID_ATTRIBUTE = "SourceProcessId"
MESSAGE_ID_ATTRIBUTE = "MessageId"
EPOCH_NUMBER_ATTRIBUTE = "EpochNumber"
LAST_UPDATED_IN_EPOCH_ATTRIBUTE = "LastUpdatedInEpoch"
TRIGGERING_MESSAGE_IDS_ATTRIBUTE = "TriggetingMessageIds"
WARNINGS_ATTRIBUTE = "Warnings"
SIMULATION_STATE_ATTRIBUTE = "SimulationState"
START_TIME_ATTRIBUTE = "StartTime"
END_TIME_ATTRIBUTE = "EndTime"
VALUE_ATTRIBUTE = "Value"
DESCRIPTION_ATTRIBUTE = "Description"

DEFAULT_TYPE = "SimState"
DEFAULT_TIMESTAMP = get_utcnow_in_milliseconds()
DEFAULT_SIMULATION_ID = "2020-07-31T11:11:11.123Z"
DEFAULT_SOURCE_PROCESS_ID = "component"
DEFAULT_MESSAGE_ID = "component-10"
DEFAULT_EPOCH_NUMBER = 7
DEFAULT_LAST_UPDATED_IN_EPOCH = 7
DEFAULT_TRIGGERING_MESSAGE_IDS = ["manager-7", "other-7"]
DEFAULT_WARNINGS = ["warnings.convergence"]
DEFAULT_SIMULATION_STATE = "running"
DEFAULT_START_TIME = "2001-01-01T00:00:00Z"
DEFAULT_END_TIME = "2001-01-01T01:00:00Z"
DEFAULT_VALUE = "ready"
DEFAULT_DESCRIPTION = "Random error"
DEFAULT_EXTRA_ATTRIBUTES = {
    "Extra": "Extra attribute",
    "Extra2": 17
}

FULL_JSON = {
    **{
        MESSAGE_TYPE_ATTRIBUTE: DEFAULT_TYPE,
        SIMULATION_ID_ATTRIBUTE: DEFAULT_SIMULATION_ID,
        SOURCE_PROCESS_ID_ATTRIBUTE: DEFAULT_SOURCE_PROCESS_ID,
        MESSAGE_ID_ATTRIBUTE: DEFAULT_MESSAGE_ID,
        EPOCH_NUMBER_ATTRIBUTE: DEFAULT_EPOCH_NUMBER,
        LAST_UPDATED_IN_EPOCH_ATTRIBUTE: DEFAULT_LAST_UPDATED_IN_EPOCH,
        TRIGGERING_MESSAGE_IDS_ATTRIBUTE: DEFAULT_TRIGGERING_MESSAGE_IDS,
        WARNINGS_ATTRIBUTE: DEFAULT_WARNINGS,
        SIMULATION_STATE_ATTRIBUTE: DEFAULT_SIMULATION_STATE,
        START_TIME_ATTRIBUTE: DEFAULT_START_TIME,
        END_TIME_ATTRIBUTE: DEFAULT_END_TIME,
        VALUE_ATTRIBUTE: DEFAULT_VALUE,
        DESCRIPTION_ATTRIBUTE: DEFAULT_DESCRIPTION
    },
    **DEFAULT_EXTRA_ATTRIBUTES
}


class TestMessageHelpers(unittest.TestCase):
    """Unit tests for the Message class helper functions."""

    def test_get_next_message_id(self):
        """Unit test for the get_next_message_id function."""
        id_generator1 = tools.messages.get_next_message_id("dummy")
        id_generator2 = tools.messages.get_next_message_id("manager", 7)

        self.assertEqual(next(id_generator1), "dummy-1")
        self.assertEqual(next(id_generator1), "dummy-2")
        self.assertEqual(next(id_generator2), "manager-7")
        self.assertEqual(next(id_generator1), "dummy-3")
        self.assertEqual(next(id_generator2), "manager-8")
        self.assertEqual(next(id_generator2), "manager-9")
        self.assertEqual(next(id_generator1), "dummy-4")
        self.assertEqual(next(id_generator2), "manager-10")


class TestAbstractMessage(unittest.TestCase):
    """Unit tests for the AbstractMessage class."""

    def test_message_creation(self):
        """Unit test for creating instances of AbstractMessage class."""

        # When message is created without a Timestamp attribute,
        # the current time in millisecond precision is used as the default value.
        utcnow1 = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
        utcnow1 -= datetime.timedelta(microseconds=utcnow1.microsecond % 1000)
        message_full = tools.messages.AbstractMessage.from_json(FULL_JSON)
        message_timestamp = to_utc_datetime_object(message_full.timestamp)
        utcnow2 = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
        utcnow1 -= datetime.timedelta(microseconds=utcnow2.microsecond % 1000)

        self.assertGreaterEqual(message_timestamp, utcnow1)
        self.assertLessEqual(message_timestamp, utcnow2)
        self.assertEqual(message_full.message_type, DEFAULT_TYPE)
        self.assertEqual(message_full.simulation_id, DEFAULT_SIMULATION_ID)
        self.assertEqual(message_full.source_process_id, DEFAULT_SOURCE_PROCESS_ID)
        self.assertEqual(message_full.message_id, DEFAULT_MESSAGE_ID)

        # Test with explicitely set timestamp
        message_timestamped = tools.messages.AbstractMessage(Timestamp=DEFAULT_TIMESTAMP, **FULL_JSON)
        self.assertEqual(message_timestamped.timestamp, DEFAULT_TIMESTAMP)
        self.assertEqual(message_timestamped.message_type, DEFAULT_TYPE)
        self.assertEqual(message_timestamped.simulation_id, DEFAULT_SIMULATION_ID)
        self.assertEqual(message_timestamped.source_process_id, DEFAULT_SOURCE_PROCESS_ID)
        self.assertEqual(message_timestamped.message_id, DEFAULT_MESSAGE_ID)

    def test_message_json(self):
        """Unit test for testing that the json from a message has correct attributes."""
        message_full_json = tools.messages.AbstractMessage.from_json(FULL_JSON).json()

        self.assertIn(MESSAGE_TYPE_ATTRIBUTE, message_full_json)
        self.assertIn(SIMULATION_ID_ATTRIBUTE, message_full_json)
        self.assertIn(SOURCE_PROCESS_ID_ATTRIBUTE, message_full_json)
        self.assertIn(MESSAGE_ID_ATTRIBUTE, message_full_json)
        self.assertIn(TIMESTAMP_ATTRIBUTE, message_full_json)
        self.assertEqual(len(message_full_json), 5)

    def test_message_bytes(self):
        """Unit test for testing that the bytes conversion works correctly."""
        # Convert to bytes and back to Message instance
        message_full = tools.messages.AbstractMessage.from_json(FULL_JSON)
        message_copy = tools.messages.AbstractMessage.from_json(
            json.loads(message_full.bytes().decode("UTF-8"))
        )

        self.assertEqual(message_copy.timestamp, message_full.timestamp)
        self.assertEqual(message_copy.message_type, message_full.message_type)
        self.assertEqual(message_copy.simulation_id, message_full.simulation_id)
        self.assertEqual(message_copy.source_process_id, message_full.source_process_id)
        self.assertEqual(message_copy.message_id, message_full.message_id)

    def test_invalid_values(self):
        """Unit tests for testing that invalid attribute values are recognized."""
        message_full = tools.messages.AbstractMessage.from_json(FULL_JSON)
        message_full_json = message_full.json()

        allowed_message_types = [
            "Epoch",
            "Error",
            "General",
            "Result",
            "SimState",
            "Status"
        ]
        for message_type_str in allowed_message_types:
            message_full.message_type = message_type_str
            self.assertEqual(message_full.message_type, message_type_str)

        invalid_attribute_exceptions = {
            MESSAGE_TYPE_ATTRIBUTE: tools.exceptions.messages.MessageTypeError,
            SIMULATION_ID_ATTRIBUTE: tools.exceptions.messages.MessageDateError,
            SOURCE_PROCESS_ID_ATTRIBUTE: tools.exceptions.messages.MessageSourceError,
            MESSAGE_ID_ATTRIBUTE: tools.exceptions.messages.MessageIdError,
            TIMESTAMP_ATTRIBUTE: tools.exceptions.messages.MessageDateError
        }
        invalid_attribute_values = {
            MESSAGE_TYPE_ATTRIBUTE: ["Test", 12, ""],
            SIMULATION_ID_ATTRIBUTE: ["simulation-id", 12, "2020-07-31T24:11:11.123Z", ""],
            SOURCE_PROCESS_ID_ATTRIBUTE: [12, ""],
            MESSAGE_ID_ATTRIBUTE: ["process", 12, "process-", "-12", ""],
            TIMESTAMP_ATTRIBUTE: ["timestamp", 12, "2020-07-31T24:11:11.123Z", ""]
        }
        for invalid_attribute in invalid_attribute_exceptions:
            if invalid_attribute != TIMESTAMP_ATTRIBUTE:
                json_invalid_attribute = copy.deepcopy(message_full_json)
                json_invalid_attribute.pop(invalid_attribute)
                self.assertRaises(
                    invalid_attribute_exceptions[invalid_attribute],
                    tools.messages.AbstractMessage, **json_invalid_attribute)

            for invalid_value in invalid_attribute_values[invalid_attribute]:
                json_invalid_attribute = copy.deepcopy(message_full_json)
                json_invalid_attribute[invalid_attribute] = invalid_value
                self.assertRaises(
                    (ValueError, invalid_attribute_exceptions[invalid_attribute]),
                    tools.messages.AbstractMessage, **json_invalid_attribute)


class SimulationStateMessage(unittest.TestCase):
    """Unit tests for the SimulationStateMessage class."""

    def test_message_creation(self):
        """Unit test for creating instances of SimulationStateMessage class."""

        # When message is created without a Timestamp attribute,
        # the current time in millisecond precision is used as the default value.
        utcnow1 = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
        utcnow1 -= datetime.timedelta(microseconds=utcnow1.microsecond % 1000)
        message_full = tools.messages.SimulationStateMessage.from_json(FULL_JSON)
        message_timestamp = to_utc_datetime_object(message_full.timestamp)
        utcnow2 = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
        utcnow1 -= datetime.timedelta(microseconds=utcnow2.microsecond % 1000)

        self.assertGreaterEqual(message_timestamp, utcnow1)
        self.assertLessEqual(message_timestamp, utcnow2)
        self.assertEqual(message_full.message_type, DEFAULT_TYPE)
        self.assertEqual(message_full.simulation_id, DEFAULT_SIMULATION_ID)
        self.assertEqual(message_full.source_process_id, DEFAULT_SOURCE_PROCESS_ID)
        self.assertEqual(message_full.message_id, DEFAULT_MESSAGE_ID)
        self.assertEqual(message_full.simulation_state, DEFAULT_SIMULATION_STATE)

        # Test with explicitely set timestamp
        message_timestamped = tools.messages.SimulationStateMessage(Timestamp=DEFAULT_TIMESTAMP, **FULL_JSON)
        self.assertEqual(message_timestamped.timestamp, DEFAULT_TIMESTAMP)
        self.assertEqual(message_timestamped.message_type, DEFAULT_TYPE)
        self.assertEqual(message_timestamped.simulation_id, DEFAULT_SIMULATION_ID)
        self.assertEqual(message_timestamped.source_process_id, DEFAULT_SOURCE_PROCESS_ID)
        self.assertEqual(message_timestamped.message_id, DEFAULT_MESSAGE_ID)
        self.assertEqual(message_timestamped.simulation_state, DEFAULT_SIMULATION_STATE)

    def test_message_json(self):
        """Unit test for testing that the json from a message has correct attributes."""
        message_full_json = tools.messages.SimulationStateMessage.from_json(FULL_JSON).json()

        self.assertIn(MESSAGE_TYPE_ATTRIBUTE, message_full_json)
        self.assertIn(SIMULATION_ID_ATTRIBUTE, message_full_json)
        self.assertIn(SOURCE_PROCESS_ID_ATTRIBUTE, message_full_json)
        self.assertIn(MESSAGE_ID_ATTRIBUTE, message_full_json)
        self.assertIn(TIMESTAMP_ATTRIBUTE, message_full_json)
        self.assertIn(SIMULATION_STATE_ATTRIBUTE, message_full_json)
        self.assertEqual(len(message_full_json), 6)

    def test_message_bytes(self):
        """Unit test for testing that the bytes conversion works correctly."""
        # Convert to bytes and back to Message instance
        message_full = tools.messages.SimulationStateMessage.from_json(FULL_JSON)
        message_copy = tools.messages.SimulationStateMessage.from_json(
            json.loads(message_full.bytes().decode("UTF-8"))
        )

        self.assertEqual(message_copy.timestamp, message_full.timestamp)
        self.assertEqual(message_copy.message_type, message_full.message_type)
        self.assertEqual(message_copy.simulation_id, message_full.simulation_id)
        self.assertEqual(message_copy.source_process_id, message_full.source_process_id)
        self.assertEqual(message_copy.message_id, message_full.message_id)
        self.assertEqual(message_copy.simulation_state, message_full.simulation_state)

    def test_invalid_values(self):
        """Unit tests for testing that invalid attribute values are recognized."""
        message_full = tools.messages.SimulationStateMessage.from_json(FULL_JSON)
        message_full_json = message_full.json()

        allowed_message_types = [
            "Epoch",
            "Error",
            "General",
            "Result",
            "SimState",
            "Status"
        ]
        for message_type_str in allowed_message_types:
            message_full.message_type = message_type_str
            self.assertEqual(message_full.message_type, message_type_str)

        allowed_simulation_states = [
            "running",
            "stopped"
        ]
        for simulation_state_str in allowed_simulation_states:
            message_full.simulation_state = simulation_state_str
            self.assertEqual(message_full.simulation_state, simulation_state_str)

        invalid_attribute_exceptions = {
            MESSAGE_TYPE_ATTRIBUTE: tools.exceptions.messages.MessageTypeError,
            SIMULATION_ID_ATTRIBUTE: tools.exceptions.messages.MessageDateError,
            SOURCE_PROCESS_ID_ATTRIBUTE: tools.exceptions.messages.MessageSourceError,
            MESSAGE_ID_ATTRIBUTE: tools.exceptions.messages.MessageIdError,
            TIMESTAMP_ATTRIBUTE: tools.exceptions.messages.MessageDateError,
            SIMULATION_STATE_ATTRIBUTE: tools.exceptions.messages.MessageStateValueError
        }
        invalid_attribute_values = {
            MESSAGE_TYPE_ATTRIBUTE: ["Test", 12, ""],
            SIMULATION_ID_ATTRIBUTE: ["simulation-id", 12, "2020-07-31T24:11:11.123Z", ""],
            SOURCE_PROCESS_ID_ATTRIBUTE: [12, ""],
            MESSAGE_ID_ATTRIBUTE: ["process", 12, "process-", "-12", ""],
            TIMESTAMP_ATTRIBUTE: ["timestamp", 12, "2020-07-31T24:11:11.123Z", ""],
            SIMULATION_STATE_ATTRIBUTE: ["waiting", 12, ""]
        }
        for invalid_attribute in invalid_attribute_exceptions:
            if invalid_attribute != TIMESTAMP_ATTRIBUTE:
                json_invalid_attribute = copy.deepcopy(message_full_json)
                json_invalid_attribute.pop(invalid_attribute)
                self.assertRaises(
                    invalid_attribute_exceptions[invalid_attribute],
                    tools.messages.SimulationStateMessage, **json_invalid_attribute)

            for invalid_value in invalid_attribute_values[invalid_attribute]:
                json_invalid_attribute = copy.deepcopy(message_full_json)
                json_invalid_attribute[invalid_attribute] = invalid_value
                self.assertRaises(
                    (ValueError, invalid_attribute_exceptions[invalid_attribute]),
                    tools.messages.SimulationStateMessage, **json_invalid_attribute)


if __name__ == '__main__':
    unittest.main()
