from dragon.detectors import MMD
from dragon.detectors.BaseDetector import Detector
from dragon.stream.GrowingWindow import GrowingWindow
from dragon.stream.SlidingWindow import SlidingWindow
from dragon.stream.WindowChunker import WindowChunker
from dragon.util.Stream import from_file, Stream
from dragon.stream.Structure import Pipeline
import time


# I did intentionally not use the assertions to get easier readable results
# code looks like shit but is more readable for the user

def check1_1(stream):
    print("This test checks, if you did load the data correctly.")

    if stream is None:
        print("Your dataset is empty. Consider looking in the DataHandling class if this failed.")
        return

    data = stream.take(5)

    if not isinstance(stream, Stream):
        print(
            "Your loaded DataSet should be a StreamDataset, if using the from_file() method this should automatically be the case. "
            "Consider looking in the DataHandling class if this failed.")
        return

    elif len(data) != 5:
        print(
            "Your dataset does not have the right amount of elements. Consider looking in the DataHandling class if this failed.")
        return

    # load test data as expected to check if data from user is correct
    test_data = from_file("test_data.csv")
    expected_data = test_data.take(5)

    if data.data != expected_data.data:
        print("Your dataset does not contain the correct datapoints.")
        return
    print("Well done! You did successfully load the data correctly! On to the next step!")


def check1_2(pipe: Pipeline | None):
    print("This test checks, if your pipeline is correctly implemented.")
    expected_pipe = Pipeline(
        SlidingWindow(max_size=250, name="reference"),
        SlidingWindow(max_size=250, name="current")
    )
    if verify_pipe(pipe, expected_pipe):
        print("Great! Your pipeline is correct!")


def check1_3(detector: Detector | None):
    print("This test checks, if this detector is correctly implemented.")
    if detector is None:
        print("Your detector is not initialized, check you detector.")
    if not isinstance(detector, MMD):
        print("Your detector should be an MMD detector. But was: ", type(detector), ".")
        return
    print("Great! Your detector is ready!")


def check1_4(pipe):
    print("This test checks if the stream actually flowed through your pipeline.\n")

    if not verify(pipe is not None, "Pipeline is missing."):
        return

    data = pipe.get_data()
    if not verify(len(data) > 0,
                  "Your pipeline is completely empty. Did you forget to call `stream.run()` or add the pipeline as a listener?"):
        return

    print("Success! Your pipeline processed the stream and currently holds ", len(data), " samples in memory.")


def check1_5(pipe, detector, stream):
    print(
        "This test checks, if your pipeline is correctly implemented and if your MMD implementation found one or some possible drifts.")
    expected_pipe = Pipeline(Pipeline(GrowingWindow(max_size=150), SlidingWindow(max_size=100)),
                             SlidingWindow(max_size=250), WindowChunker(chunk_size=10))
    if not verify_pipe(pipe, expected_pipe):
        return
    else:
        print("Great! Your pipeline is correct!")

    results = {"drift_count": 0}

    def count_drift(**payload):
        results["drift_count"] += 1

    print("Now checking your stream and MMD...")
    detector.add_listener("drift_detected", count_drift)
    stream.add_listener(pipe)
    stream.run()

    if results['drift_count'] > 0:
        print("Great! Your MMD drift detector found", results['drift_count'], "possible drifts!")
    else:
        print(
            "Check your implementation. Did you forget to call `stream.run()` or add the pipeline as a listener?")


def check1_6(parsed_pipe):
    print("This test checks if your WAL code compiled to the correct pipeline.")

    expected_pipe = Pipeline(
        Pipeline(GrowingWindow(max_size=150), SlidingWindow(max_size=100)),
        SlidingWindow(max_size=250),
        WindowChunker(chunk_size=10)
    )

    if not verify(parsed_pipe is not None, "The parser returned nothing. Check your syntax."):
        return
    if not verify(is_same_structure(parsed_pipe, expected_pipe),

                  error_message="The parsed structure doesn't match the goal.\nExpected: " + str(
                      expected_pipe) + "\nGot: " + str(parsed_pipe)):
        return

    print("Great! You mastered the Window Annotation Language (WAL)!")


def verify_pipe(pipe, expected):
    if pipe is None:
        print("Your pipeline is empty, check you initialization.")
        print("Your pipeline should look like this:")
        print(expected)
        return False
    if not is_same_structure(pipe, expected):
        print("It seems, that your pipeline is not correct.")
        print("Your pipeline should look like this:")
        print(expected)
        print("But your pipeline is: ", pipe)
        return False
    return True


def is_same_structure(obj1, obj2):
    if type(obj1) != type(obj2):
        return False

    if hasattr(obj1, 'stages'):
        if len(obj1.stages) != len(obj2.stages):
            return False
        return all(is_same_structure(s1, s2) for s1, s2 in zip(obj1.stages, obj2.stages))

    if hasattr(obj1, 'get_params'):
        p1 = obj1.get_params().copy()
        p2 = obj2.get_params().copy()

        p1.pop('name', None)  # ignore names for the test
        p2.pop('name', None)

        return p1 == p2

    return True


def full_test(pipe, detector, stream):
    print("This is the Final Integration Test! Let's see if everything works together.\n")
    if not verify(stream is not None,
                  "Your stream is missing.",
                  test_id="final_stream",
                  hint="Did you call `from_file('test_data.csv')` and assign it to a variable?"):
        return
    expected_pipe = Pipeline(
        Pipeline(GrowingWindow(max_size=150), SlidingWindow(max_size=100)),
        SlidingWindow(max_size=250),
        WindowChunker(chunk_size=10)
    )
    if not verify(is_same_structure(pipe, expected_pipe),
                  "Your pipeline structure is not correct yet.",
                  test_id="final_pipe",
                  hint="Check the inner Pipeline. Remember the order: the data comes from the RIGHT."):
        return

    has_two_windows = hasattr(detector, 'ref_window') and hasattr(detector, 'cur_window')
    has_window_list = hasattr(detector, 'windows') and len(getattr(detector, 'windows', [])) >= 2

    if not verify(detector is not None and (has_two_windows or has_window_list),
                  "The detector is not configured correctly.",
                  test_id="final_detector",
                  hint="Ensure you passed exactly two windows (current and reference) from your pipeline to the MMD detector."):
        return

    data = pipe.get_data()
    if not verify(len(data) > 0,
                  "The pipeline is completely empty.",
                  test_id="final_execution",
                  hint="Did you forget to add the pipeline as a listener to the stream (`stream.add_listener(pipe)`) and run `stream.gen_stream()`?"):
        return

    print("You have successfully built, connected and executed a full DRAGON drift detection system! Congrats!")


_failure_counts = {}


def verify(condition: bool, error_message: str, test_id: str = None, hint: str = None) -> bool:
    global _failure_counts

    if not condition:
        print("Oh no: {}".format(error_message))
        if test_id and hint:
            _failure_counts[test_id] = _failure_counts.get(test_id, 0) + 1
            if _failure_counts[test_id] >= 2:
                print("Hint: {}".format(hint))
        return False
    if test_id and test_id in _failure_counts:
        _failure_counts[test_id] = 0
    return True


def format_window(window, size):
    padded = ["-"] * (size - len(window)) + window
    return "[" + ", ".join("{:>2}".format(str(x)) for x in padded) + "]"


def window_filling():
    """
    Small Window animation for SlidingWindow, WindowChunker and GrowingWindow
    """
    print("Take a look and see how the different windows behave on new datapoints. "
          "Each window in this animation is acting independently of each other.\n")
    stream = [1, 2, 3, 4, 5, 6, 7, 8, 9]
    window_size = 3

    sliding = []
    chunker = []
    growing = []

    print(
        "{:<6} | {:<5} | {:<26} | {:<30} | {}".format('T', 'New', 'Sliding Window', 'Window Chunker', 'Growing Window')
    )
    print("-" * 115)

    for step, val in enumerate(stream, 1):
        # Sliding Window
        sliding.append(val)
        if len(sliding) > window_size:
            sliding.pop(0)

        # Chunker
        chunker.append(val)
        if len(chunker) == window_size:
            chunker_display = list(chunker)
            chunker.clear()
        else:
            chunker_display = list(chunker)

        # Growing Window
        growing.append(val)

        sliding_str = format_window(sliding, window_size)
        chunker_str = format_window(chunker_display, window_size)
        growing_str = "[" + ", ".join("{:>2}".format(str(x)) for x in growing) + "]"

        print("t={:<4} | [ {}]  | {:<26} | {:<30} | {}".format(step, val, sliding_str, chunker_str, growing_str))

        time.sleep(0.5)


def pipeline_animation():
    stream_data = [10, 20, 30, 40, 50, 60, 70, 80, 90]
    win_cur = []
    win_ref = []
    print(
        "Take a look at how a pipeline lets data flow through itself.\n"
        "Data comes in from the right side and flows through both windows in it.")
    print("-" * 100)

    print(
        "{:<4} | {:<32} | {:<30} | {:<28} | {}".format('T', 'Stream (Remaining)', 'Ref Window(max_size=3)',
                                                       'Cur Window(max_size=3)', 'Event (Push)')
    )
    for step in range(len(stream_data)):
        current_val = stream_data[step]
        remaining = stream_data[step + 1:]

        # Format the remaining data in the generator
        queue_str = "[{}]".format(', '.join(map(str, remaining)))

        # The Broadcast Event
        event_str = "<== [{}] <==".format(current_val)

        win_cur.append(current_val)
        if len(win_cur) > 3:  # Just showing a small window size of 3
            win_ref.append(win_cur[0])
            win_cur.pop(0)
        if len(win_ref) > 3:  # Just showing a small window size of 3
            win_ref.pop(0)

        wind_cur_str = "Current window: " + str(win_cur)
        wind_ref_str = "Reference Window: " + str(win_ref)

        print("t={:<2} | {:<32} | {:<30} | {:<28} | {}".format(step + 1, queue_str, wind_ref_str,
                                                               wind_cur_str, event_str))

        time.sleep(0.5)


def stream_generator_animation():
    """
    Small stream generator animation to see how the generator pushes data into pipes or other stuff
    """

    print("Take a look at how the StreamGenerator works in DRAGON.")
    print("Instead of you pulling data in a for-loop, the StreamGenerator PUSHES")
    print("new datapoints as events to all connected listeners at the same time.\n")

    stream_data = [10, 20, 30, 40, 50, 60, 70, 80, 90]

    pipe_a = []  # a SlidingWindow in your Pipeline
    pipe_b = []  # another SlidingWindow

    print(
        "{:<4} | {:<32} | {:<24} | {:<20} | {}".format('T', 'Stream (Remaining)', 'Listener 1 (Pipe a)',
                                                       'Listener 2 (Pipe b)', 'Event (Push)')
    )
    print("-" * 100)

    # Calling gen_stream() essentially starts this loop internally
    for step in range(len(stream_data)):
        current_val = stream_data[step]
        remaining = stream_data[step + 1:]

        # Format the remaining data in the generator
        queue_str = "[{}]".format(', '.join(map(str, remaining)))

        # The Broadcast Event
        event_str = "<== [{}] <==".format(current_val)

        # Listeners reacting to the event simultaneously
        pipe_a.append(current_val)
        if len(pipe_a) > 3:  # Just showing a small window size of 3
            pipe_a.pop(0)

        pipe_b.append(current_val)
        if len(pipe_b) > 3:  # Just showing a small window size of 3
            pipe_b.pop(0)

        pipe_a_str = "Pipe A: " + str(pipe_a)
        pipe_b_str = "Pipe B: " + str(pipe_b)

        print("t={:<2} | {:<32} | {:<24} | {:<20} | {}".format(step + 1, queue_str, pipe_a_str, pipe_b_str, event_str))

        time.sleep(0.5)

    print("\nSimulation finished! The StreamGenerator is now empty.")


