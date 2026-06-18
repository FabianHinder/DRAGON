import numpy as np
import pandas as pd
from matplotlib import pyplot as plt

from river import datasets
from river.datasets.synth import SEA
from sklearn.datasets import make_blobs, make_classification

from dragon import Filter, Map, parse
from dragon.detectors import MMD, VirtualClassifier, KS


from dragon.stream.StaticWindow import StaticWindow
from dragon.stream.StreamObject import Peek
from dragon.stream.WindowChunker import WindowChunker
from dragon.util.Stream import RiverConcept, SKlearnConcept, StreamGenerator,Stream ,from_numpy_array, \
    from_list, from_river, save_file

from dragon.stream.SlidingWindow import SlidingWindow
from dragon.stream.Structure import Pipeline



# data
def create_stream(n_samples=200):
    # a default data stream with arbitrary drift
    concept_a = SKlearnConcept(make_blobs, concept_id=0, n_samples=n_samples * 2, centers=[[0, 0]], cluster_std=0.5)
    concept_b = SKlearnConcept(make_blobs, concept_id=1, n_samples=n_samples * 2, centers=[[5, 0]], cluster_std=0.5)
    concept_c = SKlearnConcept(make_blobs, concept_id=2, n_samples=n_samples * 2, centers=[[10, 0]], cluster_std=3.0)

    builder = StreamGenerator()
    builder.add_abrupt(concept_a, n_samples=n_samples)
    builder.add_abrupt(concept_b, n_samples=n_samples)
    builder.add_abrupt(concept_c, n_samples=n_samples)
    return builder.build()


w_size = 50



def check_detector_impl():
    # detector implementation
    stream = create_stream()
    sw_new = SlidingWindow(max_size=w_size)
    sw_old = SlidingWindow(max_size=w_size)

    pipe = Pipeline(sw_old, sw_new)
    stream.add_listener(pipe)
    # detector = MMD(sw_old, sw_new, threshold=0.01)
    detector = VirtualClassifier(sw_old, sw_new)
    # detector = KS(sw_old, sw_new)

    def listening_method(**payload):
        print(f"payload: {payload}")

    detector.add_listener("drift_detected", listening_method)
    stream.run()



def data_generation():
    # data generation from sk-learn concepts, with arbitrary drift
    from sklearn.datasets import make_blobs, make_moons

    concept_1 = SKlearnConcept(make_blobs, concept_id=1, n_samples=2000, centers=[[0, 0], [0, 0]], cluster_std=0.5)
    concept_2 = SKlearnConcept(make_blobs, concept_id=2, n_samples=2000, centers=[[5, 0], [5, 0]], cluster_std=0.5)
    concept_3 = SKlearnConcept(make_moons, concept_id=3, n_samples=2000, noise=0.1)

    builder = StreamGenerator()

    builder.add_abrupt(concept_1, n_samples=500)
    builder.add_gradual(concept_from=concept_1, concept_to=concept_2, transition_width=400, random_state=42)
    builder.add_abrupt(concept_2, n_samples=300)
    builder.add_gradual(concept_from=concept_2, concept_to=concept_3, transition_width=400, random_state=42)
    builder.add_abrupt(concept_3, n_samples=500)

    samples = builder.build().take(2000)
    df = pd.DataFrame([{**s.features, **s.metadata} for s in samples.data])

    plt.figure(figsize=(12, 4))
    scatter1 = plt.scatter(df.index, df["feature_0"], c=df["concept_id"], cmap="viridis", alpha=0.6, s=15)
    plt.title("Feature 0 over time")
    plt.xlabel("Index")
    plt.ylabel("Feature value")
    plt.colorbar(scatter1, label="Concept ID")
    plt.grid(True, linestyle="--", alpha=0.5)
    plt.tight_layout()
    plt.show()

    plt.figure(figsize=(12, 3))

    rolling_concept = df["concept_id"].rolling(window=50, center=True).mean()

    plt.plot(df.index, rolling_concept, color="crimson", linewidth=2.5, label="Sliding average")

    plt.title("Gradual drift")
    plt.xlabel("Index")
    plt.ylabel("Concept")
    plt.yticks([1, 2, 3])
    plt.legend()
    plt.grid(True, linestyle="--", alpha=0.5)
    plt.tight_layout()
    plt.show()


def river_data_generation():
    # data generation from river concepts
    concept_a = RiverConcept(datasets.synth.SEA(variant=0, seed=42), concept_id=0)
    concept_b = RiverConcept(datasets.synth.SEA(variant=1, seed=42), concept_id=1)
    concept_c = RiverConcept(datasets.synth.SEA(variant=2, seed=42), concept_id=2)

    test_data = []

    test_data.extend(concept_a.take(100))
    test_data.extend(concept_b.take(100))
    test_data.extend(concept_c.take(100))

    df = pd.DataFrame([{**s.features, **s.metadata} for s in test_data])

    plt.figure(figsize=(12, 5))
    plt.scatter(df["feature_0"], df["feature_1"], c=df["concept_id"], cmap="viridis", alpha=0.6, s=15)
    plt.show()

    print(df.head())

    stream = Stream(test_data)
    stream.run()


def pipe_in_pipe():
    # pipe building
    cleanse_filter = Filter(lambda x: x.get('value', 0) > 0)
    scale_map = Map(lambda x: {'value': x.get('value', 0) * 10})
    sw = SlidingWindow(max_size=50)
    peeker_in = Peek()
    peeker_out = Peek()
    pipe_inner = Pipeline(scale_map, cleanse_filter, peeker_in)
    pipe_outer = Pipeline(peeker_out, pipe_inner, sw)

    test_data = [
        {'value': -1},
        {'value': 2},
        {'value': 5},
        {'value': -3},
        {'value': 8}
    ]
    stream = from_list(test_data).take(len(test_data))

    stream.add_listener(pipe_outer)
    stream.run()

    print(sw.get_data())


def object_listening_to_stream():
    # object directly listening to stream
    stream = create_stream(n_samples=10)
    sw_new = SlidingWindow(max_size=1)

    pipe = Pipeline(Peek(), sw_new)

    # Let the stream figure out the correct hook dynamically
    stream.add_listener(pipe)
    stream.run()

def check_from_river():
    # direct data generation from river stream
    river_stream = SEA()
    stream = from_river(river_stream, 50)

    sw_new = SlidingWindow(max_size=5)
    sw_old = SlidingWindow(max_size=5)
    pipe = Pipeline(sw_old, sw_new)
    stream.add_listener(pipe)
    stream.run()



def from_xyz_method():
    n_length, n_dimensions = 500, 2

    # Creating the data as a numpy array
    X = np.random.normal(size=(n_length, n_dimensions))
    X[n_length // 2:] += 1  # adding some drift in the middle of the stream

    # Creating the StreamGenerator
    stream = from_numpy_array(X, "feature 1", "feature 2")
    my_stream = stream.take(50)


    pipe = Pipeline(Peek())
    my_stream.add_listener(pipe)
    my_stream.run()


def save_stream():
    # this is the test data stream used in the first tutorial
    n_length = 500
    concept_1 = SKlearnConcept(make_blobs, concept_id=1, n_samples=1000,
                               centers=[[0, 0]], cluster_std=0.5)

    concept_2 = SKlearnConcept(make_blobs, concept_id=2, n_samples=1000,
                               centers=[[10, 10]], cluster_std=0.5)

    builder = StreamGenerator()
    builder.add_abrupt(concept_1, n_length)
    builder.add_abrupt(concept_2, n_length)
    dataset = builder.build()
    save_file(dataset, "test_data.csv")


def wal_language():
    # syntax of wal
    pipe = parse("[my_pipe: [slide max_size=250] <- [slide max_size=250]]")
    print(pipe)
    current = SlidingWindow(name="current", max_size=250)
    reference = SlidingWindow(name="reference", max_size=250)

    cur_wal = parse("[slide max_size=250]")
    ref_wal = parse("[reference: slide max_size=250]")
    print(cur_wal, " ", ref_wal)
    print(current)


# =========================================================
# test different scenarios here
# =========================================================

## stream building and generation
# river_data_generation()
# data_generation()
# save_stream()
# from_xyz_method()
# check_from_river()

## stream listener construction
# pipe_in_pipe()
# object_listening_to_stream()

## detector usage
# check_detector_impl()

