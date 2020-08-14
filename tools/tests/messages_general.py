# -*- coding: utf-8 -*-

"""Unit test for the GeneralMessage class."""

import copy
import datetime
import json
import unittest

import tools.exceptions.messages
import tools.messages
from tools.datetime_tools import to_utc_datetime_object

from tools.tests.messages_common import MESSAGE_TYPE_ATTRIBUTE
from tools.tests.messages_common import TIMESTAMP_ATTRIBUTE
from tools.tests.messages_common import SIMULATION_ID_ATTRIBUTE
from tools.tests.messages_common import SOURCE_PROCESS_ID_ATTRIBUTE
from tools.tests.messages_common import MESSAGE_ID_ATTRIBUTE
from tools.tests.messages_common import EPOCH_NUMBER_ATTRIBUTE
from tools.tests.messages_common import LAST_UPDATED_IN_EPOCH_ATTRIBUTE
from tools.tests.messages_common import TRIGGERING_MESSAGE_IDS_ATTRIBUTE
from tools.tests.messages_common import WARNINGS_ATTRIBUTE
from tools.tests.messages_common import SIMULATION_STATE_ATTRIBUTE
from tools.tests.messages_common import START_TIME_ATTRIBUTE
from tools.tests.messages_common import END_TIME_ATTRIBUTE
from tools.tests.messages_common import VALUE_ATTRIBUTE
from tools.tests.messages_common import DESCRIPTION_ATTRIBUTE
from tools.tests.messages_common import DEFAULT_TYPE
from tools.tests.messages_common import DEFAULT_TIMESTAMP
from tools.tests.messages_common import DEFAULT_SIMULATION_ID
from tools.tests.messages_common import DEFAULT_SOURCE_PROCESS_ID
from tools.tests.messages_common import DEFAULT_MESSAGE_ID
from tools.tests.messages_common import DEFAULT_EPOCH_NUMBER
from tools.tests.messages_common import DEFAULT_LAST_UPDATED_IN_EPOCH
from tools.tests.messages_common import DEFAULT_TRIGGERING_MESSAGE_IDS
from tools.tests.messages_common import DEFAULT_WARNINGS
from tools.tests.messages_common import DEFAULT_SIMULATION_STATE
from tools.tests.messages_common import DEFAULT_START_TIME
from tools.tests.messages_common import DEFAULT_END_TIME
from tools.tests.messages_common import DEFAULT_VALUE
from tools.tests.messages_common import DEFAULT_DESCRIPTION
from tools.tests.messages_common import DEFAULT_EXTRA_ATTRIBUTES
from tools.tests.messages_common import FULL_JSON


class TestGeneralMessage(unittest.TestCase):
    """Unit tests for the GeneralMessage class."""

    def test_message_creation(self):
        """Unit test for creating instances of GeneralMessage class."""

        # When message is created without a Timestamp attribute,
        # the current time in millisecond precision is used as the default value.
        utcnow1 = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
        utcnow1 -= datetime.timedelta(microseconds=utcnow1.microsecond % 1000)
        message_full = tools.messages.GeneralMessage.from_json(FULL_JSON)
        message_timestamp = to_utc_datetime_object(message_full.timestamp)
        utcnow2 = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
        utcnow1 -= datetime.timedelta(microseconds=utcnow2.microsecond % 1000)

        self.assertGreaterEqual(message_timestamp, utcnow1)
        self.assertLessEqual(message_timestamp, utcnow2)
        self.assertEqual(message_full.message_type, DEFAULT_TYPE)
        self.assertEqual(message_full.simulation_id, DEFAULT_SIMULATION_ID)
        self.assertEqual(message_full.source_process_id, DEFAULT_SOURCE_PROCESS_ID)
        self.assertEqual(message_full.message_id, DEFAULT_MESSAGE_ID)
        self.assertEqual(message_full.general_attributes[EPOCH_NUMBER_ATTRIBUTE], DEFAULT_EPOCH_NUMBER)
        self.assertEqual(message_full.general_attributes[LAST_UPDATED_IN_EPOCH_ATTRIBUTE],
                         DEFAULT_LAST_UPDATED_IN_EPOCH)
        self.assertEqual(message_full.general_attributes[TRIGGERING_MESSAGE_IDS_ATTRIBUTE],
                         DEFAULT_TRIGGERING_MESSAGE_IDS)
        self.assertEqual(message_full.general_attributes[WARNINGS_ATTRIBUTE], DEFAULT_WARNINGS)
        self.assertEqual(message_full.general_attributes[SIMULATION_STATE_ATTRIBUTE], DEFAULT_SIMULATION_STATE)
        self.assertEqual(message_full.general_attributes[START_TIME_ATTRIBUTE], DEFAULT_START_TIME)
        self.assertEqual(message_full.general_attributes[END_TIME_ATTRIBUTE], DEFAULT_END_TIME)
        self.assertEqual(message_full.general_attributes[VALUE_ATTRIBUTE], DEFAULT_VALUE)
        self.assertEqual(message_full.general_attributes[DESCRIPTION_ATTRIBUTE], DEFAULT_DESCRIPTION)
        for extra_attribute_name, extra_attribute_value in DEFAULT_EXTRA_ATTRIBUTES.items():
            self.assertEqual(message_full.general_attributes[extra_attribute_name], extra_attribute_value)

        # Test with explicitely set timestamp
        message_timestamped = tools.messages.GeneralMessage(Timestamp=DEFAULT_TIMESTAMP, **FULL_JSON)
        self.assertEqual(message_timestamped.timestamp, DEFAULT_TIMESTAMP)
        self.assertEqual(message_timestamped.message_type, DEFAULT_TYPE)
        self.assertEqual(message_timestamped.simulation_id, DEFAULT_SIMULATION_ID)
        self.assertEqual(message_timestamped.source_process_id, DEFAULT_SOURCE_PROCESS_ID)
        self.assertEqual(message_timestamped.message_id, DEFAULT_MESSAGE_ID)
        self.assertEqual(message_timestamped.general_attributes[EPOCH_NUMBER_ATTRIBUTE], DEFAULT_EPOCH_NUMBER)
        self.assertEqual(message_timestamped.general_attributes[LAST_UPDATED_IN_EPOCH_ATTRIBUTE],
                         DEFAULT_LAST_UPDATED_IN_EPOCH)
        self.assertEqual(message_timestamped.general_attributes[TRIGGERING_MESSAGE_IDS_ATTRIBUTE],
                         DEFAULT_TRIGGERING_MESSAGE_IDS)
        self.assertEqual(message_timestamped.general_attributes[WARNINGS_ATTRIBUTE], DEFAULT_WARNINGS)
        self.assertEqual(message_timestamped.general_attributes[SIMULATION_STATE_ATTRIBUTE], DEFAULT_SIMULATION_STATE)
        self.assertEqual(message_timestamped.general_attributes[START_TIME_ATTRIBUTE], DEFAULT_START_TIME)
        self.assertEqual(message_timestamped.general_attributes[END_TIME_ATTRIBUTE], DEFAULT_END_TIME)
        self.assertEqual(message_timestamped.general_attributes[VALUE_ATTRIBUTE], DEFAULT_VALUE)
        self.assertEqual(message_timestamped.general_attributes[DESCRIPTION_ATTRIBUTE], DEFAULT_DESCRIPTION)
        for extra_attribute_name, extra_attribute_value in DEFAULT_EXTRA_ATTRIBUTES.items():
            self.assertEqual(message_timestamped.general_attributes[extra_attribute_name], extra_attribute_value)

        # Test message creation without the optional attributes.
        stripped_json = copy.deepcopy(FULL_JSON)
        stripped_json.pop(EPOCH_NUMBER_ATTRIBUTE)
        stripped_json.pop(LAST_UPDATED_IN_EPOCH_ATTRIBUTE)
        stripped_json.pop(TRIGGERING_MESSAGE_IDS_ATTRIBUTE)
        stripped_json.pop(WARNINGS_ATTRIBUTE)
        stripped_json.pop(SIMULATION_STATE_ATTRIBUTE)
        stripped_json.pop(START_TIME_ATTRIBUTE)
        stripped_json.pop(END_TIME_ATTRIBUTE)
        stripped_json.pop(VALUE_ATTRIBUTE)
        stripped_json.pop(DESCRIPTION_ATTRIBUTE)
        for extra_attribute_name in DEFAULT_EXTRA_ATTRIBUTES:
            stripped_json.pop(extra_attribute_name)
        message_stripped = tools.messages.GeneralMessage(Timestamp=DEFAULT_TIMESTAMP, **stripped_json)
        self.assertEqual(message_stripped.timestamp, DEFAULT_TIMESTAMP)
        self.assertEqual(message_stripped.message_type, DEFAULT_TYPE)
        self.assertEqual(message_stripped.simulation_id, DEFAULT_SIMULATION_ID)
        self.assertEqual(message_stripped.source_process_id, DEFAULT_SOURCE_PROCESS_ID)
        self.assertEqual(message_stripped.message_id, DEFAULT_MESSAGE_ID)
        self.assertEqual(message_stripped.general_attributes, {})

    def test_message_json(self):
        """Unit test for testing that the json from a message has correct attributes."""
        message_full_json = tools.messages.GeneralMessage.from_json(FULL_JSON).json()

        self.assertIn(MESSAGE_TYPE_ATTRIBUTE, message_full_json)
        self.assertIn(SIMULATION_ID_ATTRIBUTE, message_full_json)
        self.assertIn(SOURCE_PROCESS_ID_ATTRIBUTE, message_full_json)
        self.assertIn(MESSAGE_ID_ATTRIBUTE, message_full_json)
        self.assertIn(TIMESTAMP_ATTRIBUTE, message_full_json)
        self.assertIn(EPOCH_NUMBER_ATTRIBUTE, message_full_json)
        self.assertIn(LAST_UPDATED_IN_EPOCH_ATTRIBUTE, message_full_json)
        self.assertIn(TRIGGERING_MESSAGE_IDS_ATTRIBUTE, message_full_json)
        self.assertIn(WARNINGS_ATTRIBUTE, message_full_json)
        self.assertIn(SIMULATION_STATE_ATTRIBUTE, message_full_json)
        self.assertIn(START_TIME_ATTRIBUTE, message_full_json)
        self.assertIn(END_TIME_ATTRIBUTE, message_full_json)
        self.assertIn(VALUE_ATTRIBUTE, message_full_json)
        self.assertIn(DESCRIPTION_ATTRIBUTE, message_full_json)
        for extra_attribute_name in DEFAULT_EXTRA_ATTRIBUTES:
            self.assertIn(extra_attribute_name, message_full_json)
        self.assertEqual(len(message_full_json), 16)

    def test_message_bytes(self):
        """Unit test for testing that the bytes conversion works correctly."""
        # Convert to bytes and back to Message instance
        message_full = tools.messages.GeneralMessage.from_json(FULL_JSON)
        message_copy = tools.messages.GeneralMessage.from_json(
            json.loads(message_full.bytes().decode("UTF-8"))
        )

        self.assertEqual(message_copy.timestamp, message_full.timestamp)
        self.assertEqual(message_copy.message_type, message_full.message_type)
        self.assertEqual(message_copy.simulation_id, message_full.simulation_id)
        self.assertEqual(message_copy.source_process_id, message_full.source_process_id)
        self.assertEqual(message_copy.message_id, message_full.message_id)
        general_attributes_original = message_full.general_attributes
        general_attributes_copy = message_copy.general_attributes
        self.assertEqual(len(general_attributes_copy), len(general_attributes_original))
        for attribute_name in general_attributes_original:
            self.assertEqual(general_attributes_copy[attribute_name], general_attributes_original[attribute_name])

    def test_invalid_values(self):
        """Unit tests for testing that invalid attribute values are recognized."""
        message_full = tools.messages.GeneralMessage.from_json(FULL_JSON)
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
            MESSAGE_ID_ATTRIBUTE: tools.exceptions.messages.MessageIdError
        }
        invalid_attribute_values = {
            MESSAGE_TYPE_ATTRIBUTE: ["Test", 12, ""],
            SIMULATION_ID_ATTRIBUTE: ["simulation-id", 12, "2020-07-31T24:11:11.123Z", ""],
            SOURCE_PROCESS_ID_ATTRIBUTE: [12, ""],
            MESSAGE_ID_ATTRIBUTE: ["process", 12, "process-", "-12", ""]
        }
        for invalid_attribute in invalid_attribute_exceptions:
            if invalid_attribute != TIMESTAMP_ATTRIBUTE:
                json_invalid_attribute = copy.deepcopy(message_full_json)
                json_invalid_attribute.pop(invalid_attribute)
                self.assertRaises(
                    invalid_attribute_exceptions[invalid_attribute],
                    tools.messages.GeneralMessage, **json_invalid_attribute)

            for invalid_value in invalid_attribute_values[invalid_attribute]:
                json_invalid_attribute = copy.deepcopy(message_full_json)
                json_invalid_attribute[invalid_attribute] = invalid_value
                self.assertRaises(
                    (ValueError, invalid_attribute_exceptions[invalid_attribute]),
                    tools.messages.GeneralMessage, **json_invalid_attribute)

        message_full.general_attributes = {}
        self.assertEqual(len(message_full.json()), 5)
        self.assertEqual(message_full.general_attributes, {})
        self.assertRaises(
            tools.exceptions.messages.MessageValueError,
            setattr, *(message_full, "general_attributes", []))
        self.assertRaises(
            tools.exceptions.messages.MessageValueError,
            setattr, *(message_full, "general_attributes", "general"))


if __name__ == '__main__':
    unittest.main()
