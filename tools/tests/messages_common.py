# -*- coding: utf-8 -*-

"""Common variable values for the message module unit tests."""

import unittest
from tools.datetime_tools import get_utcnow_in_milliseconds
from tools.messages import get_next_message_id

MESSAGE_TYPE_ATTRIBUTE = "Type"
TIMESTAMP_ATTRIBUTE = "Timestamp"
SIMULATION_ID_ATTRIBUTE = "SimulationId"
SOURCE_PROCESS_ID_ATTRIBUTE = "SourceProcessId"
MESSAGE_ID_ATTRIBUTE = "MessageId"
EPOCH_NUMBER_ATTRIBUTE = "EpochNumber"
LAST_UPDATED_IN_EPOCH_ATTRIBUTE = "LastUpdatedInEpoch"
TRIGGERING_MESSAGE_IDS_ATTRIBUTE = "TriggeringMessageIds"
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
DEFAULT_WARNINGS = ["warning.convergence"]
DEFAULT_SIMULATION_STATE = "running"
DEFAULT_START_TIME = "2001-01-01T00:00:00.000Z"
DEFAULT_END_TIME = "2001-01-01T01:00:00.000Z"
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
        id_generator1 = get_next_message_id("dummy")
        id_generator2 = get_next_message_id("manager", 7)

        self.assertEqual(next(id_generator1), "dummy-1")
        self.assertEqual(next(id_generator1), "dummy-2")
        self.assertEqual(next(id_generator2), "manager-7")
        self.assertEqual(next(id_generator1), "dummy-3")
        self.assertEqual(next(id_generator2), "manager-8")
        self.assertEqual(next(id_generator2), "manager-9")
        self.assertEqual(next(id_generator1), "dummy-4")
        self.assertEqual(next(id_generator2), "manager-10")


if __name__ == '__main__':
    unittest.main()
