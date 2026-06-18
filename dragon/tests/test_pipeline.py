import unittest
from unittest.mock import MagicMock

from dragon import Filter
from dragon.stream.Structure import Pipeline
from dragon.stream.SlidingWindow import SlidingWindow
from dragon.stream.StaticWindow import StaticWindow
from dragon.util.DataTypes import StreamSample


class TestPipeline(unittest.TestCase):

    def setUp(self):


        self.source_win = SlidingWindow(max_size=100)
        self.target_win = StaticWindow(max_size=5)

        self.pipe = Pipeline(self.source_win, self.target_win)
        self.data = ["data_" + str(i) for i in range(100)]
        self.source_win.push(self.data)


    def test_initialization_invalid_stage(self):
        def just_a_function(x):
            return x

        with self.assertRaises(TypeError) as context:
            bad_pipe = Pipeline(self.source_win, just_a_function)

        self.assertIn("not a StreamObject", str(context.exception))

    def test_pipeline_emits_own_out_flow(self):

        w1 = SlidingWindow(max_size=2)
        pipe = Pipeline(w1)

        mock_outflow = MagicMock()
        pipe.event_handler.add_listener(pipe._get_event_name("out_flow"), mock_outflow)

        s1 = StreamSample(features={'val': 1}, metadata={})
        s2 = StreamSample(features={'val': 2}, metadata={})
        s3 = StreamSample(features={'val': 3}, metadata={})


        pipe.push([s1, s2, s3])

        mock_outflow.assert_called_once()
        args, kwargs = mock_outflow.call_args
        out_samples = kwargs.get('samples', args[0] if args else None)

        self.assertEqual(out_samples, [s1], "Pipeline did not fire the out_flow of the last stage correctly!")


class TestPipelineDeltaLogic(unittest.TestCase):

    def setUp(self):
        self.win1 = SlidingWindow(max_size=2)
        self.filter = Filter(lambda x: x.get('value', 0) > 0)
        self.win2 = SlidingWindow(max_size=1)

        self.pipe = Pipeline(self.win2, self.filter, self.win1)

        self.s_in1 = StreamSample(features={'value': 1}, metadata={})
        self.s_in2 = StreamSample(features={'value': 2}, metadata={})
        self.s_out1 = StreamSample(features={'value': 10}, metadata={})
        self.s_out_drop = StreamSample(features={'value': -5}, metadata={})
        self.s_end = StreamSample(features={'value': 99}, metadata={})


        self.win1._data = [self.s_out1, self.s_out_drop] # [10,-5]
        self.win2._data = [self.s_end]                   # [99]

        self.mock_listener = MagicMock()
        self.pipe.event_handler.add_listener(
            self.pipe._get_event_name("state_changed"),
            self.mock_listener
        )

    def test_pipeline_delta_calculation(self):
        """
        w2<-filter<-w1 <-- 1,2
        should be:
                    w2    filter  w1
        old_state:  99    //      10,-5
        in:         10    10,-5   1,2
        out:        99    10      10,-5

        full in is 1, 2, 10, -5, 10
        full out is 99, 10, 10, -5
        remaining: in: 1,2 out: 99
        """
        self.pipe.push([self.s_in1, self.s_in2])

        self.mock_listener.assert_called_once()
        args, kwargs = self.mock_listener.call_args
        event = kwargs.get('state_change', args[0] if args else None)

        self.assertIsNotNone(event, "Pipeline did not fire the StateChangeEvent!")

        self.assertEqual(event.data_in, [self.s_in1, self.s_in2], "delta_in calculated wrong!")
        self.assertEqual(event.data_out, [self.s_end], "delta_out calculated wrong!")
