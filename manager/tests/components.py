# -*- coding: utf-8 -*-

"""Unit test for the components module."""

import unittest

from manager.components import SimulationComponents

NO_MESSAGES = -1


class TestSimulationComponents(unittest.TestCase):
    """Unit tests for the SimulationComponents class."""

    def test_new_class(self):
        """Unit test for an empty component list."""
        self.assertEqual(SimulationComponents.NO_MESSAGES, NO_MESSAGES)

        components = SimulationComponents()
        self.assertEqual(components.get_component_list(), [])
        self.assertEqual(components.get_latest_full_epoch(), NO_MESSAGES)
        self.assertEqual(str(components), "")
        self.assertEqual(components.get_latest_epoch_for_component("test"), None)

    def test_add_and_remove_components(self):
        """Tests adding new components."""
        new_component_names = ["dummy", "generator", "extra", "planner", "logger"]
        removed_component_names = ["dummy", "extra", "logger"]
        remaining_component_names = ["generator", "planner"]

        # add new components
        components = SimulationComponents()
        for component_name in new_component_names:
            components.add_component(component_name)

        self.assertEqual(components.get_component_list(), new_component_names)
        self.assertEqual(components.get_latest_full_epoch(), NO_MESSAGES)
        self.assertEqual(str(components), ", ".join([
            "{:s} ({:d})".format(component_name, NO_MESSAGES)
            for component_name in new_component_names
        ]))

        self.assertEqual(components.get_latest_epoch_for_component("test"), None)
        for component_name in new_component_names:
            self.assertEqual(components.get_latest_epoch_for_component(component_name), NO_MESSAGES)

        # remove some components
        for component_name in removed_component_names:
            components.remove_component(component_name)

        self.assertEqual(components.get_component_list(), remaining_component_names)
        self.assertEqual(components.get_latest_full_epoch(), NO_MESSAGES)
        self.assertEqual(str(components), ", ".join([
            "{:s} ({:d})".format(component_name, NO_MESSAGES)
            for component_name in remaining_component_names
        ]))

        for component_name in removed_component_names:
            self.assertEqual(components.get_latest_epoch_for_component(component_name), None)
        for component_name in remaining_component_names:
            self.assertEqual(components.get_latest_epoch_for_component(component_name), NO_MESSAGES)

    def test_simulation_phases(self):
        """Tests the latest epoch calculations."""
        new_component_names = ["generator", "planner", "logger", "extra", "watcher"]
        component_names_expect_first = new_component_names[1:]
        component_names_expect_last = new_component_names[:len(new_component_names)-1]
        component_names_expect_middle = new_component_names[:len(new_component_names) // 2] + \
            new_component_names[len(new_component_names) // 2 + 1:]
        ready_messages = {
            0: (new_component_names, 0),
            1: (component_names_expect_first, 0),
            2: (new_component_names, 2),
            3: (new_component_names, 3),
            4: (component_names_expect_first, 3),
            5: (component_names_expect_first, 3),
            6: (component_names_expect_middle, 5),
            7: (component_names_expect_last, 6),
            8: (new_component_names, 8)
        }

        components = SimulationComponents()
        for component_name in new_component_names:
            components.add_component(component_name)

        for epoch_number, (component_names, full_epoch) in ready_messages.items():
            for epoch_component_name in component_names:
                components.register_ready_message(epoch_component_name, epoch_number, epoch_component_name)

            self.assertEqual(components.get_latest_full_epoch(), full_epoch)
            self.assertEqual(components.get_latest_status_message_ids(), ready_messages[epoch_number][0])

            for actual_component_name in new_component_names:
                self.assertGreaterEqual(components.get_latest_epoch_for_component(actual_component_name), full_epoch)
                if actual_component_name in component_names:
                    self.assertEqual(components.get_latest_epoch_for_component(actual_component_name), epoch_number)
                else:
                    self.assertLess(components.get_latest_epoch_for_component(actual_component_name), epoch_number)


if __name__ == '__main__':
    unittest.main()
