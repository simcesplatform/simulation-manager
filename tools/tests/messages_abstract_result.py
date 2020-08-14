# -*- coding: utf-8 -*-

"""Unit test for the AbstractResultMessage class."""

import copy
import datetime
import json
import unittest

import tools.exceptions.messages
import tools.messages
from tools.datetime_tools import to_utc_datetime_object
from tools.tools import FullLogger

from tools.tests.messages_common import MESSAGE_TYPE_ATTRIBUTE
from tools.tests.messages_common import TIMESTAMP_ATTRIBUTE
from tools.tests.messages_common import SIMULATION_ID_ATTRIBUTE
from tools.tests.messages_common import SOURCE_PROCESS_ID_ATTRIBUTE
from tools.tests.messages_common import MESSAGE_ID_ATTRIBUTE
from tools.tests.messages_common import EPOCH_NUMBER_ATTRIBUTE
from tools.tests.messages_common import LAST_UPDATED_IN_EPOCH_ATTRIBUTE
from tools.tests.messages_common import TRIGGERING_MESSAGE_IDS_ATTRIBUTE
from tools.tests.messages_common import WARNINGS_ATTRIBUTE
from tools.tests.messages_common import DEFAULT_TYPE
from tools.tests.messages_common import DEFAULT_TIMESTAMP
from tools.tests.messages_common import DEFAULT_SIMULATION_ID
from tools.tests.messages_common import DEFAULT_SOURCE_PROCESS_ID
from tools.tests.messages_common import DEFAULT_MESSAGE_ID
from tools.tests.messages_common import DEFAULT_EPOCH_NUMBER
from tools.tests.messages_common import DEFAULT_LAST_UPDATED_IN_EPOCH
from tools.tests.messages_common import DEFAULT_TRIGGERING_MESSAGE_IDS
from tools.tests.messages_common import DEFAULT_WARNINGS
from tools.tests.messages_common import FULL_JSON

LOGGER = FullLogger(__name__)


class TestAbstractResultMessage(unittest.TestCase):
    """Unit tests for the AbstractResultMessage class."""

    def test_message_creation(self):
        """Unit test for creating instances of AbstractResultMessage class."""

        # When message is created without a Timestamp attribute,
        # the current time in millisecond precision is used as the default value.
        utcnow1 = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
        utcnow1 -= datetime.timedelta(microseconds=utcnow1.microsecond % 1000)
        message_full = tools.messages.AbstractResultMessage.from_json(FULL_JSON)
        message_timestamp = to_utc_datetime_object(message_full.timestamp)
        utcnow2 = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
        utcnow1 -= datetime.timedelta(microseconds=utcnow2.microsecond % 1000)

        self.assertGreaterEqual(message_timestamp, utcnow1)
        self.assertLessEqual(message_timestamp, utcnow2)
        self.assertEqual(message_full.message_type, DEFAULT_TYPE)
        self.assertEqual(message_full.simulation_id, DEFAULT_SIMULATION_ID)
        self.assertEqual(message_full.source_process_id, DEFAULT_SOURCE_PROCESS_ID)
        self.assertEqual(message_full.message_id, DEFAULT_MESSAGE_ID)
        self.assertEqual(message_full.epoch_number, DEFAULT_EPOCH_NUMBER)
        self.assertEqual(message_full.last_updated_in_epoch, DEFAULT_LAST_UPDATED_IN_EPOCH)
        self.assertEqual(message_full.triggering_message_ids, DEFAULT_TRIGGERING_MESSAGE_IDS)
        self.assertEqual(message_full.warnings, DEFAULT_WARNINGS)

        # Test with explicitely set timestamp
        message_timestamped = tools.messages.AbstractResultMessage(Timestamp=DEFAULT_TIMESTAMP, **FULL_JSON)
        self.assertEqual(message_timestamped.timestamp, DEFAULT_TIMESTAMP)
        self.assertEqual(message_timestamped.message_type, DEFAULT_TYPE)
        self.assertEqual(message_timestamped.simulation_id, DEFAULT_SIMULATION_ID)
        self.assertEqual(message_timestamped.source_process_id, DEFAULT_SOURCE_PROCESS_ID)
        self.assertEqual(message_timestamped.message_id, DEFAULT_MESSAGE_ID)
        self.assertEqual(message_timestamped.epoch_number, DEFAULT_EPOCH_NUMBER)
        self.assertEqual(message_timestamped.last_updated_in_epoch, DEFAULT_LAST_UPDATED_IN_EPOCH)
        self.assertEqual(message_timestamped.triggering_message_ids, DEFAULT_TRIGGERING_MESSAGE_IDS)
        self.assertEqual(message_timestamped.warnings, DEFAULT_WARNINGS)

        # Test message creation without the optional attributes.
        stripped_json = copy.deepcopy(FULL_JSON)
        stripped_json.pop(LAST_UPDATED_IN_EPOCH_ATTRIBUTE)
        stripped_json.pop(WARNINGS_ATTRIBUTE)
        message_stripped = tools.messages.AbstractResultMessage(Timestamp=DEFAULT_TIMESTAMP, **stripped_json)
        self.assertEqual(message_stripped.timestamp, DEFAULT_TIMESTAMP)
        self.assertEqual(message_stripped.message_type, DEFAULT_TYPE)
        self.assertEqual(message_stripped.simulation_id, DEFAULT_SIMULATION_ID)
        self.assertEqual(message_stripped.source_process_id, DEFAULT_SOURCE_PROCESS_ID)
        self.assertEqual(message_stripped.message_id, DEFAULT_MESSAGE_ID)
        self.assertEqual(message_stripped.epoch_number, DEFAULT_EPOCH_NUMBER)
        self.assertEqual(message_stripped.last_updated_in_epoch, None)
        self.assertEqual(message_stripped.triggering_message_ids, DEFAULT_TRIGGERING_MESSAGE_IDS)
        self.assertEqual(message_stripped.warnings, None)

    def test_message_json(self):
        """Unit test for testing that the json from a message has correct attributes."""
        message_full_json = tools.messages.AbstractResultMessage.from_json(FULL_JSON).json()

        self.assertIn(MESSAGE_TYPE_ATTRIBUTE, message_full_json)
        self.assertIn(SIMULATION_ID_ATTRIBUTE, message_full_json)
        self.assertIn(SOURCE_PROCESS_ID_ATTRIBUTE, message_full_json)
        self.assertIn(MESSAGE_ID_ATTRIBUTE, message_full_json)
        self.assertIn(TIMESTAMP_ATTRIBUTE, message_full_json)
        self.assertIn(EPOCH_NUMBER_ATTRIBUTE, message_full_json)
        self.assertIn(LAST_UPDATED_IN_EPOCH_ATTRIBUTE, message_full_json)
        self.assertIn(TRIGGERING_MESSAGE_IDS_ATTRIBUTE, message_full_json)
        self.assertIn(WARNINGS_ATTRIBUTE, message_full_json)
        self.assertEqual(len(message_full_json), 9)

    def test_message_bytes(self):
        """Unit test for testing that the bytes conversion works correctly."""
        # Convert to bytes and back to Message instance
        message_full = tools.messages.AbstractResultMessage.from_json(FULL_JSON)
        message_copy = tools.messages.AbstractResultMessage.from_json(
            json.loads(message_full.bytes().decode("UTF-8"))
        )

        self.assertEqual(message_copy.timestamp, message_full.timestamp)
        self.assertEqual(message_copy.message_type, message_full.message_type)
        self.assertEqual(message_copy.simulation_id, message_full.simulation_id)
        self.assertEqual(message_copy.source_process_id, message_full.source_process_id)
        self.assertEqual(message_copy.message_id, message_full.message_id)
        self.assertEqual(message_copy.epoch_number, message_full.epoch_number)
        self.assertEqual(message_copy.last_updated_in_epoch, message_full.last_updated_in_epoch)
        self.assertEqual(message_copy.triggering_message_ids, message_full.triggering_message_ids)
        self.assertEqual(message_copy.warnings, message_full.warnings)

    def test_invalid_values(self):
        """Unit tests for testing that invalid attribute values are recognized."""
        message_full = tools.messages.AbstractResultMessage.from_json(FULL_JSON)
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

        allowed_warning_types = [
            "warning.convergence",
            "warning.input",
            "warning.input.range",
            "warning.input.unreliable",
            "warning.internal",
            "warning.other"
        ]
        message_full.warnings = allowed_warning_types
        self.assertEqual(message_full.warnings, allowed_warning_types)
        message_full.warnings = []
        self.assertEqual(message_full.warnings, None)
        for allowed_warning_str in allowed_warning_types:
            message_full.warnings = [allowed_warning_str]
            self.assertEqual(message_full.warnings, [allowed_warning_str])

        optional_attributes = [
            LAST_UPDATED_IN_EPOCH_ATTRIBUTE,
            WARNINGS_ATTRIBUTE
        ]

        invalid_attribute_exceptions = {
            MESSAGE_TYPE_ATTRIBUTE: tools.exceptions.messages.MessageTypeError,
            SIMULATION_ID_ATTRIBUTE: tools.exceptions.messages.MessageDateError,
            SOURCE_PROCESS_ID_ATTRIBUTE: tools.exceptions.messages.MessageSourceError,
            MESSAGE_ID_ATTRIBUTE: tools.exceptions.messages.MessageIdError,
            TIMESTAMP_ATTRIBUTE: tools.exceptions.messages.MessageDateError,
            EPOCH_NUMBER_ATTRIBUTE: tools.exceptions.messages.MessageEpochValueError,
            LAST_UPDATED_IN_EPOCH_ATTRIBUTE: tools.exceptions.messages.MessageEpochValueError,
            TRIGGERING_MESSAGE_IDS_ATTRIBUTE: tools.exceptions.messages.MessageIdError,
            WARNINGS_ATTRIBUTE: tools.exceptions.messages.MessageValueError
        }
        invalid_attribute_values = {
            MESSAGE_TYPE_ATTRIBUTE: ["Test", 12, ""],
            SIMULATION_ID_ATTRIBUTE: ["simulation-id", 12, "2020-07-31T24:11:11.123Z", ""],
            SOURCE_PROCESS_ID_ATTRIBUTE: [12, ""],
            MESSAGE_ID_ATTRIBUTE: ["process", 12, "process-", "-12", ""],
            TIMESTAMP_ATTRIBUTE: ["timestamp", 12, "2020-07-31T24:11:11.123Z", ""],
            EPOCH_NUMBER_ATTRIBUTE: [-1, "epoch", "12", ""],
            LAST_UPDATED_IN_EPOCH_ATTRIBUTE: [-1, "epoch", "12", ""],
            TRIGGERING_MESSAGE_IDS_ATTRIBUTE: [["process-12", "process2-"], ["process-"], []],
            WARNINGS_ATTRIBUTE: [["warning.convergence", "warning"], ["warning."], ["warning.random"]]
        }
        for invalid_attribute in invalid_attribute_exceptions:
            if invalid_attribute != TIMESTAMP_ATTRIBUTE and invalid_attribute not in optional_attributes:
                json_invalid_attribute = copy.deepcopy(message_full_json)
                json_invalid_attribute.pop(invalid_attribute)
                self.assertRaises(
                    invalid_attribute_exceptions[invalid_attribute],
                    tools.messages.AbstractResultMessage, **json_invalid_attribute)

            for invalid_value in invalid_attribute_values[invalid_attribute]:
                json_invalid_attribute = copy.deepcopy(message_full_json)
                json_invalid_attribute[invalid_attribute] = invalid_value
                self.assertRaises(
                    (ValueError, invalid_attribute_exceptions[invalid_attribute]),
                    tools.messages.AbstractResultMessage, **json_invalid_attribute)


if __name__ == '__main__':
    unittest.main()
