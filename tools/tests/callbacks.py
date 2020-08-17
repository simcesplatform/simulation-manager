# -*- coding: utf-8 -*-

"""Unit test for the MessageCallback class."""

import json

import aiounittest

from tools.callbacks import MessageCallback
from tools.messages import EpochMessage, ErrorMessage, GeneralMessage, \
                           ResultMessage, SimulationStateMessage, StatusMessage
from tools.tests.messages_abstract import ALTERNATE_JSON, DEFAULT_TIMESTAMP, FULL_JSON, MESSAGE_TYPE_ATTRIBUTE

FAIL_TEST_JSON = {
    "test": "fail"
}
FAIL_TEST_STR = '{"test" "fail"}'


class DummyIncomingMessage:
    """Dummy class for the unit tests to create objects that have the attributes expected by the MessageCallback."""
    def __init__(self, body, routing_key):
        self.body = body
        self.routing_key = routing_key


class DummyHandler:
    """Class for dummy message handler."""
    def __init__(self):
        self.last_message = None
        self.last_topic = None

    async def message_handler(self, message_object, message_topic):
        """Stores the received message and topic."""
        self.last_message = message_object
        self.last_topic = message_topic


HANDLER = DummyHandler()


class TestMessageCallback(aiounittest.AsyncTestCase):
    """Unit tests for the MessageCallback class."""
    GENERAL_MESSAGE = GeneralMessage(Timestamp=DEFAULT_TIMESTAMP, **FULL_JSON)
    GENERAL_JSON = GENERAL_MESSAGE.json()
    ALTERNATE_MESSAGE = GeneralMessage(**ALTERNATE_JSON)
    TEST_TOPIC1 = "unit_test"
    TEST_TOPIC2 = "alternate"

    def helper_equality_tester(self, callback_object, expected_message, expected_topic):
        """Helper function to test that the last recorded messages are equal to the expected ones."""
        self.assertEqual(callback_object.last_message, expected_message)
        self.assertEqual(HANDLER.last_message, expected_message)

        self.assertEqual(callback_object.last_topic, expected_topic)
        self.assertEqual(HANDLER.last_topic, expected_topic)

    async def test_simulation_state_message(self):
        """Unit test for the callback handling simulation state messages."""
        callback_object = MessageCallback(HANDLER.message_handler, "SimState")

        state_message = SimulationStateMessage.from_json(
            {**TestMessageCallback.GENERAL_JSON, **{MESSAGE_TYPE_ATTRIBUTE: "SimState"}})
        alternate_message = SimulationStateMessage.from_json(
            {**ALTERNATE_JSON, **{MESSAGE_TYPE_ATTRIBUTE: "SimState"}})
        epoch_message = EpochMessage.from_json(
            {**TestMessageCallback.GENERAL_JSON, **{MESSAGE_TYPE_ATTRIBUTE: "Epoch"}})

        # test the simulation state callback with proper simulation state messages
        await callback_object.callback(
            DummyIncomingMessage(state_message.bytes(), TestMessageCallback.TEST_TOPIC1))
        self.helper_equality_tester(callback_object, state_message, TestMessageCallback.TEST_TOPIC1)

        await callback_object.callback(
            DummyIncomingMessage(alternate_message.bytes(), TestMessageCallback.TEST_TOPIC2))
        self.helper_equality_tester(callback_object, alternate_message, TestMessageCallback.TEST_TOPIC2)

        # test the simulation state callback with an epoch message => should be given as JSON object
        await callback_object.callback(
            DummyIncomingMessage(epoch_message.bytes(), TestMessageCallback.TEST_TOPIC1))
        self.helper_equality_tester(callback_object, epoch_message.json(), TestMessageCallback.TEST_TOPIC1)

    async def test_epoch_callback(self):
        """Unit test for the callback handling epoch messages."""
        callback_object = MessageCallback(HANDLER.message_handler, "Epoch")

        epoch_message = EpochMessage.from_json(
            {**TestMessageCallback.GENERAL_JSON, **{MESSAGE_TYPE_ATTRIBUTE: "Epoch"}})
        alternate_message = EpochMessage.from_json(
            {**ALTERNATE_JSON, **{MESSAGE_TYPE_ATTRIBUTE: "Epoch"}})
        state_message = SimulationStateMessage.from_json(
            {**TestMessageCallback.GENERAL_JSON, **{MESSAGE_TYPE_ATTRIBUTE: "SimState"}})

        # test the epoch callback with proper epoch messages
        await callback_object.callback(
            DummyIncomingMessage(epoch_message.bytes(), TestMessageCallback.TEST_TOPIC1))
        self.helper_equality_tester(callback_object, epoch_message, TestMessageCallback.TEST_TOPIC1)

        await callback_object.callback(
            DummyIncomingMessage(alternate_message.bytes(), TestMessageCallback.TEST_TOPIC2))
        self.helper_equality_tester(callback_object, alternate_message, TestMessageCallback.TEST_TOPIC2)

        # test the epoch callback with a simulation state message => should be given as JSON object
        await callback_object.callback(
            DummyIncomingMessage(state_message.bytes(), TestMessageCallback.TEST_TOPIC1))
        self.helper_equality_tester(callback_object, state_message.json(), TestMessageCallback.TEST_TOPIC1)

    async def test_error_message(self):
        """Unit test for the callback handling error messages."""
        callback_object = MessageCallback(HANDLER.message_handler, "Error")

        error_message = ErrorMessage.from_json(
            {**TestMessageCallback.GENERAL_JSON, **{MESSAGE_TYPE_ATTRIBUTE: "Error"}})
        alternate_message = ErrorMessage.from_json(
            {**ALTERNATE_JSON, **{MESSAGE_TYPE_ATTRIBUTE: "Error"}})
        state_message = SimulationStateMessage.from_json(
            {**TestMessageCallback.GENERAL_JSON, **{MESSAGE_TYPE_ATTRIBUTE: "SimState"}})

        # test the error callback with proper error messages
        await callback_object.callback(
            DummyIncomingMessage(error_message.bytes(), TestMessageCallback.TEST_TOPIC1))
        self.helper_equality_tester(callback_object, error_message, TestMessageCallback.TEST_TOPIC1)

        await callback_object.callback(
            DummyIncomingMessage(alternate_message.bytes(), TestMessageCallback.TEST_TOPIC2))
        self.helper_equality_tester(callback_object, alternate_message, TestMessageCallback.TEST_TOPIC2)

        # test the error callback with a simulation state message => should be given as JSON object
        await callback_object.callback(
            DummyIncomingMessage(state_message.bytes(), TestMessageCallback.TEST_TOPIC1))
        self.helper_equality_tester(callback_object, state_message.json(), TestMessageCallback.TEST_TOPIC1)

    async def test_status_message(self):
        """Unit test for the callback handling status messages."""
        callback_object = MessageCallback(HANDLER.message_handler, "Status")

        status_message = StatusMessage.from_json(
            {**TestMessageCallback.GENERAL_JSON, **{MESSAGE_TYPE_ATTRIBUTE: "Status"}})
        alternate_message = StatusMessage.from_json(
            {**ALTERNATE_JSON, **{MESSAGE_TYPE_ATTRIBUTE: "Status"}})
        state_message = SimulationStateMessage.from_json(
            {**TestMessageCallback.GENERAL_JSON, **{MESSAGE_TYPE_ATTRIBUTE: "SimState"}})

        # test the status callback with proper status messages
        await callback_object.callback(
            DummyIncomingMessage(status_message.bytes(), TestMessageCallback.TEST_TOPIC1))
        self.helper_equality_tester(callback_object, status_message, TestMessageCallback.TEST_TOPIC1)

        await callback_object.callback(
            DummyIncomingMessage(alternate_message.bytes(), TestMessageCallback.TEST_TOPIC2))
        self.helper_equality_tester(callback_object, alternate_message, TestMessageCallback.TEST_TOPIC2)

        # test the status callback with a simulation state message => should be given as JSON object
        await callback_object.callback(
            DummyIncomingMessage(state_message.bytes(), TestMessageCallback.TEST_TOPIC1))
        self.helper_equality_tester(callback_object, state_message.json(), TestMessageCallback.TEST_TOPIC1)

    async def test_result_message(self):
        """Unit test for the callback handling status messages."""
        callback_object = MessageCallback(HANDLER.message_handler, "Result")

        result_message = ResultMessage.from_json(
            {**TestMessageCallback.GENERAL_JSON, **{MESSAGE_TYPE_ATTRIBUTE: "Result"}})
        alternate_message = ResultMessage.from_json(
            {**ALTERNATE_JSON, **{MESSAGE_TYPE_ATTRIBUTE: "Result"}})
        state_message = SimulationStateMessage.from_json(
            {**TestMessageCallback.GENERAL_JSON, **{MESSAGE_TYPE_ATTRIBUTE: "SimState"}})

        # test the status callback with proper status messages
        await callback_object.callback(
            DummyIncomingMessage(result_message.bytes(), TestMessageCallback.TEST_TOPIC1))
        self.helper_equality_tester(callback_object, result_message, TestMessageCallback.TEST_TOPIC1)

        await callback_object.callback(
            DummyIncomingMessage(alternate_message.bytes(), TestMessageCallback.TEST_TOPIC2))
        self.helper_equality_tester(callback_object, alternate_message, TestMessageCallback.TEST_TOPIC2)

        # test the status callback with a simulation state message => should be given as JSON object
        await callback_object.callback(
            DummyIncomingMessage(state_message.bytes(), TestMessageCallback.TEST_TOPIC1))
        self.helper_equality_tester(callback_object, state_message.json(), TestMessageCallback.TEST_TOPIC1)

    async def test_general_message(self):
        """Unit test for the handling of a general simulation platform message."""
        callback_object = MessageCallback(HANDLER.message_handler)
        callback_object_general = MessageCallback(HANDLER.message_handler, "General")

        epoch_message = EpochMessage.from_json(
            {**TestMessageCallback.GENERAL_JSON, **{MESSAGE_TYPE_ATTRIBUTE: "Epoch"}})
        state_message = SimulationStateMessage.from_json(
            {**TestMessageCallback.GENERAL_JSON, **{MESSAGE_TYPE_ATTRIBUTE: "SimState"}})
        status_message = StatusMessage.from_json(
            {**TestMessageCallback.GENERAL_JSON, **{MESSAGE_TYPE_ATTRIBUTE: "Status"}})
        error_message = ErrorMessage.from_json(
            {**TestMessageCallback.GENERAL_JSON, **{MESSAGE_TYPE_ATTRIBUTE: "Error"}})
        result_message = ResultMessage.from_json(
            {**TestMessageCallback.GENERAL_JSON, **{MESSAGE_TYPE_ATTRIBUTE: "Result"}})
        general_message = GeneralMessage.from_json(
            {**TestMessageCallback.GENERAL_JSON, **{MESSAGE_TYPE_ATTRIBUTE: "General"}})

        # By default the test message should be a simulation state message because
        # DEFAULT_MESSAGE_TYPE == "SimState" as is defined in messages_common.py.
        await callback_object.callback(
            DummyIncomingMessage(TestMessageCallback.GENERAL_MESSAGE.bytes(), TestMessageCallback.TEST_TOPIC1))
        self.helper_equality_tester(
            callback_object, state_message, TestMessageCallback.TEST_TOPIC1)

        await callback_object_general.callback(
            DummyIncomingMessage(TestMessageCallback.GENERAL_MESSAGE.bytes(), TestMessageCallback.TEST_TOPIC1))
        self.helper_equality_tester(
            callback_object_general, TestMessageCallback.GENERAL_MESSAGE, TestMessageCallback.TEST_TOPIC1)

        await callback_object.callback(
            DummyIncomingMessage(TestMessageCallback.ALTERNATE_MESSAGE.bytes(), TestMessageCallback.TEST_TOPIC2))
        self.helper_equality_tester(
            callback_object, TestMessageCallback.ALTERNATE_MESSAGE, TestMessageCallback.TEST_TOPIC2)

        # Tests for the different message types.
        await callback_object.callback(
            DummyIncomingMessage(epoch_message.bytes(), TestMessageCallback.TEST_TOPIC1))
        self.helper_equality_tester(callback_object, epoch_message, TestMessageCallback.TEST_TOPIC1)

        await callback_object.callback(
            DummyIncomingMessage(state_message.bytes(), TestMessageCallback.TEST_TOPIC2))
        self.helper_equality_tester(callback_object, state_message, TestMessageCallback.TEST_TOPIC2)

        await callback_object.callback(
            DummyIncomingMessage(status_message.bytes(), TestMessageCallback.TEST_TOPIC1))
        self.helper_equality_tester(callback_object, status_message, TestMessageCallback.TEST_TOPIC1)

        await callback_object.callback(
            DummyIncomingMessage(error_message.bytes(), TestMessageCallback.TEST_TOPIC2))
        self.helper_equality_tester(callback_object, error_message, TestMessageCallback.TEST_TOPIC2)

        await callback_object.callback(
            DummyIncomingMessage(result_message.bytes(), TestMessageCallback.TEST_TOPIC1))
        self.helper_equality_tester(callback_object, result_message, TestMessageCallback.TEST_TOPIC1)

        await callback_object.callback(
            DummyIncomingMessage(general_message.bytes(), TestMessageCallback.TEST_TOPIC1))
        self.helper_equality_tester(callback_object, general_message, TestMessageCallback.TEST_TOPIC1)

        # Test for a JSON not conforming to the simulation platform message schema.
        await callback_object.callback(
            DummyIncomingMessage(bytes(json.dumps(FAIL_TEST_JSON), encoding="UTF-8"), TestMessageCallback.TEST_TOPIC2))
        self.helper_equality_tester(callback_object, FAIL_TEST_JSON, TestMessageCallback.TEST_TOPIC2)

        # Test for a non-JSON message.
        await callback_object.callback(
            DummyIncomingMessage(bytes(FAIL_TEST_STR, encoding="UTF-8"), TestMessageCallback.TEST_TOPIC1))
        self.helper_equality_tester(callback_object, FAIL_TEST_STR, TestMessageCallback.TEST_TOPIC1)
