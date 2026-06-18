import unittest
from unittest.mock import MagicMock

from dragon.stream.GrowingWindow import GrowingWindow
from dragon.stream.Structure import Pipeline
from dragon.stream.SlidingWindow import SlidingWindow
from dragon.stream.WindowChunker import WindowChunker
from dragon.util.DataTypes import StreamSample


class TestGraphValidation(unittest.TestCase):

    def test_dag_prevention(self):
        parent1 = SlidingWindow(max_size=10, name="Parent1")
        parent2 = SlidingWindow(max_size=10, name="Parent2")
        child = SlidingWindow(max_size=10, name="Child")


        parent1.add_listener("out_flow", child.push)

        with self.assertRaises(ValueError) as context:
            parent2.add_listener("out_flow", child.push)

        self.assertIn("You are trying to build a cyclic structure.", str(context.exception))

    def test_cycle_prevention(self):
        node_a = SlidingWindow(max_size=10)
        node_b = SlidingWindow(max_size=10)
        node_c = SlidingWindow(max_size=10)

        node_a.add_listener("out_flow", node_b.push)
        node_b.add_listener("out_flow", node_c.push)

        # cycle: A -> B -> C -> A
        with self.assertRaises(RecursionError) as context:
            node_c.add_listener("out_flow", node_a.push)

        self.assertIn("You are trying to build a cyclic structure.", str(context.exception))


class TestDataNodes(unittest.TestCase):
    def setUp(self):
        self.s1 = StreamSample(features={'val': 10}, metadata={'id': 1})
        self.s2 = StreamSample(features={'val': 20}, metadata={'id': 2})
        self.s3 = StreamSample(features={'val': 15}, metadata={'id': 3})

    def test_map(self):
        pass
        # map test is changing in some time

    def test_filter(self):
        pass
        # filter test is changing in some time


class TestPropertyObservation(unittest.TestCase):

    def test_observable_property_emits_event(self):
        mock_listener = MagicMock()

        obj = SlidingWindow(max_size=10, name="OldName")
        obj.add_listener("property_changed", mock_listener)
        obj.name = "NewName"

        mock_listener.assert_called_once()
        kwargs = mock_listener.call_args.kwargs
        self.assertEqual(kwargs['property_name'], "name")
        self.assertEqual(kwargs['old_value'], "OldName")
        self.assertEqual(kwargs['new_value'], "NewName")


class TestConcreteWindows(unittest.TestCase):


    def test_window_chunker_buffering(self):
        chunker = WindowChunker(chunk_size=2)
        mock_listener = MagicMock()
        chunker.add_listener("out_flow", mock_listener)

        chunker.push(["A"])

        mock_listener.assert_not_called()
        self.assertEqual(chunker.buffer, ["A"])

        chunker.push(["B", "C"])

        mock_listener.assert_called_once()

        self.assertEqual(mock_listener.call_args.kwargs["samples"], ["A", "B"])
        self.assertEqual(chunker.buffer, ["C"], "Remaining element did not stay in buffer!")

    def test_window_chunking_correct(self):
        chunker = WindowChunker(chunk_size=2)
        mock_listener = MagicMock()
        chunker.add_listener("out_flow", mock_listener)

        chunker.push([1, 2, 3, 4, 5])

        self.assertEqual(mock_listener.call_count, 2, "Chunking did not fire exactly two events!")

        call_1_args = mock_listener.call_args_list[0].kwargs["samples"]
        call_2_args = mock_listener.call_args_list[1].kwargs["samples"]

        self.assertEqual(call_1_args, [1, 2])
        self.assertEqual(call_2_args, [3, 4])
        self.assertEqual(chunker.buffer, [5])


class TestGrowingWindow(unittest.TestCase):

    def test_growing_window_filling(self):
        win = GrowingWindow(max_size=3)
        mock_listener = MagicMock()
        win.add_listener("out_flow", mock_listener)

        win.push(["A", "B"])
        self.assertEqual(win.get_data(), ["A", "B"])
        self.assertEqual(win.n, 2)

        if mock_listener.call_count > 0:
            mock_listener.assert_called_once()
            self.assertEqual(mock_listener.call_args.kwargs["samples"], [])
        else:
            mock_listener.assert_not_called()

    def test_growing_window_reset(self):
        win = GrowingWindow(max_size=3)
        win.push(["A", "B", "C", "D"])
        self.assertGreater(win.n, 0)

        win.reset()
        self.assertEqual(win.n, 0, "n was not correctly reset!")
        self.assertEqual(win.get_data(), [])


class TestClosedWindow(unittest.TestCase):
    def setUp(self):
        self.window1 = SlidingWindow(max_size=5)
        self.window2 = SlidingWindow(max_size=5)
        self.pipe = Pipeline(self.window2, self.window1)

    def test_closed_window1(self):
        self.window2.close()
        self.pipe.push(["A", "B", "C", "D", "E"])
        self.assertEqual(self.window1.get_data(), ["A", "B", "C", "D", "E"])

    def test_closed_window2(self):
        self.window1.close()
        self.pipe.push(["A", "B", "C", "D", "E"])
        self.assertEqual(self.window2.get_data(), ["A", "B", "C", "D", "E"])

    def test_open_window(self):
        self.pipe.push(["A", "B", "C", "D", "E", "F", "G", "H", "I", "J"])
        self.assertEqual(self.window1.get_data(), ["F", "G", "H", "I", "J"])
        self.assertEqual(self.window2.get_data(), ["A", "B", "C", "D", "E"])