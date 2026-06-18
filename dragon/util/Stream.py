import csv
import itertools
import random
import time
from abc import ABC, abstractmethod
from typing import Any

import numpy as np
import pandas as pd
from river import datasets

from dragon.util.DataTypes import StreamSample
from dragon.util.EventHandler import EventHandler


class Stream:
    """
    A generator that generates data streams from a given data set.

    :param data: The data set. This generator accepts DRAGON StreamDatasets, pandas DataFrames and lists as input.

    """

    def __init__(self, data: pd.DataFrame | list):
        super().__init__()
        self.event_handler = EventHandler()

        if isinstance(data, pd.DataFrame):
            self.data = from_pandas_dataframe(data).data
        elif isinstance(data, list):
            if not data:
                self.data = []
            elif isinstance(data[0], StreamSample):
                self.data = data
            else:
                self.data = from_list(data).data
        else:
            raise TypeError(
                f"Error: Data format not supported. Data must be DataFrame or list of dicts, but was {type(data)}")

    def __len__(self):
        return len(self.data)

    def run(self):
        """
        Generates a stream from the data provided.

        This means every new data point fires an event named "new_data_point".
        When the stream is finished there is also an event "stream_finished" fired.

        Every fired event has an index, a timestamp (incrementing and starting from 0) and the data itself.

        Each sample wraps data into two different dictionaries: metadata and features.

        Metadata contains an index (incrementing with each event and starting from 0), a timestamp and true_label data if existing itself.
        Keys are: "index", "timestamp", "true_label"
        Note that if you generated a dataset with other true label descriptor this will change to your selected key.

        Features contains all feature data of the current sample.
        """

        num_samples = 0

        for index, sample in enumerate(self.data):
            current_time = time.time()

            sample.metadata["index"] = index
            sample.metadata["timestamp"] = current_time
            # all subscribing listeners get this datapoint
            self.event_handler.emit("new_data_point", samples=[sample])
            num_samples += 1
        self.event_handler.emit("stream_finished", total_samples=num_samples)

    def take(self, n_samples):
        """
        takes a number of samples from a Stream and returns a Stream of given length.
        :param n_samples: length of stream
        """
        if len(self.data) < n_samples:
            print("Stream does not contain enough samples.")
        return Stream(self.data[:n_samples])

    def add_listener(self, target, event_name="new_data_point"):
        """
        Add an object as listener to the stream.

        Every new data point is emitted with the "new_data_point" event.

        If the object is a StreamObject it automatically hooks itself to the event system of the stream
        and listens to the "new_data_point" event.
        In addition to that the same hook is possible for the "stream_finished" event.
        The stream automatically fires an event if the stream finished.

        Parameters
        ----------
        target:
            The object that is listening. If this is a StreamObject, it will automatically listen to the "new_data_point"
            and/or "stream_finished" events if these methods are implemented.
            Otherwise, it will add your listener
        event_name:
            Name of the event the object would be listening for. This is set as "new_data_point" per default.
        """
        from dragon.stream.StreamObject import StreamObject
        if isinstance(target, StreamObject):
            if hasattr(target, "push"):
                self.event_handler.add_listener("new_data_point", target.push)
            if hasattr(target, 'on_stream_finished'):
                self.event_handler.add_listener("stream_finished", target.on_stream_finished)
        else:
            self.event_handler.add_listener(event_name, target)


class Concept(ABC):
    """
    Abstract class for concept data.
    If you wish to add another concept class please implement this base class.
    """

    @abstractmethod
    def get_next_sample(self) -> StreamSample:
        """
        Get exactly one datapoint from any source.
        Should return a dict with features and their corresponding labels.
        """
        pass

    # make iterable from concepts
    def __iter__(self):
        return self

    def __next__(self):
        return self.get_next_sample()

    def take(self, n_samples: int) -> list[dict]:
        """
        Takes a number of samples from the concept and returns them as a list of dicts.
        :param n_samples: number of samples to take.
        """
        data = list(itertools.islice(self, n_samples))

        if len(data) < n_samples:
            print(f"Current concept was exhausted after {len(data)} samples.")

        return data


class RiverConcept(Concept):
    """
    Concept class for concept building from river generators.

    Parameters
    ----------
    stream
        river stream object from which you like to take data points.
    concept_id : int
        ID of your concept. Make sure to make your ID unique to avoid concepts overlapping.

    Notes
    -----

    See river synth dataset creation for more information on stream generation.
    (e.g. https://riverml.xyz/latest/api/datasets/synth/Agrawal/ or
    https://riverml.xyz/latest/api/datasets/synth/SEA/)

    Example
    -------

    >>> concept_a = RiverConcept(datasets.synth.SEA(variant=0, seed=42), concept_id=0)
    >>> test_data = []
    ...
    >>> test_data.extend(concept_a.take(100))

    """

    def __init__(self, stream, concept_id):
        self._iterator = iter(stream)
        self.concept_id = concept_id

    def get_next_sample(self) -> StreamSample:
        X, y = next(self._iterator)
        features = {f"feature_{i}": v for i, (k, v) in enumerate(X.items())}
        metadata = {
            "true_label": y,
            "concept_id": self.concept_id
        }
        return StreamSample(features=features, metadata=metadata)


class FileConcept(Concept):
    """
    Concept class for concept building from files.

    Parameters
    ----------
    file_path : str
        Path to your file. In best case use your absolute file path. Relative path should work as well.
    concept_id : int
        ID of your concept. Make sure to make your ID unique to avoid concepts overlapping.
    target_col : str
        Name of the target label column. This framework uses "true_label" for all explanation methods.
        If you did add a custom explanation method with other target, just leave this value as is.

    Notes
    -----
    Use the get_next_label method to get a number of datapoints from any given concept source.
    """

    def __init__(self, file_path: str, concept_id: int, target_col: str = "true_label"):
        self._file_path = file_path
        self._concept_id = concept_id
        self._target_col = target_col

        self._file = open(self._file_path, "r")
        self._reader = csv.DictReader(self._file)

    def get_next_sample(self) -> StreamSample:
        """
        Get exactly one datapoint from given source file.
        :returns: dict with features and their corresponding labels.

        Notes
        -----
        * Features are named "feature_n" with n as corresponding number of feature.
        * Target label is named "true_label"
        """
        try:
            raw_row = next(self._reader)
        except StopIteration:
            self._file.close()
            raise StopIteration("End of file.")

        y = raw_row.pop(self._target_col)
        features = {}

        for i, (k, v) in enumerate(raw_row.items()):
            try:
                features[f"feature_{i}"] = float(v)
            except ValueError:
                features[f"feature_{i}"] = v
        metadata = {
            "true_label": int(y),
            "concept_id": self._concept_id
        }

        return StreamSample(features=features, metadata=metadata)

    def __del__(self):
        if hasattr(self, "_file") and not self._file.closed:
            self._file.close()


class SKlearnConcept(Concept):
    """
    Concept class for building concepts from sklearn solutions.

    Parameters
    ----------
    generator_func
        Data generation class to generate synthetic data using sample generators from sklearn kit.
    concept_id : int
        ID of your concept. Make sure to make your ID unique to avoid concepts overlapping.
    n_samples : int
        Initially generated samples. Other than the river implementation Sklearn pre-generates a number of samples
        based on given random seed. If this pre-generated data points are exhausted you need to generate more.
    random_seed : int
        Random state for reproducible data sets.
    kwargs : dict
        If the generator function needs more arguments you can pass these through the keyword arguments.
    Notes
    -----

    See https://scikit-learn.org/stable/api/sklearn.datasets.html (sample generators)
    for further information on concept-building.

    * Features are named "feature_n" with n as corresponding number of feature.
    * Target label is "true_label"
    * The drifting point is labeled "is_drift_point"
    """

    def __init__(self, generator_func, concept_id: int, n_samples: int = 10000, random_state: int = 42, **kwargs: Any):
        self._concept_id = concept_id

        X, y = generator_func(n_samples=n_samples, random_state=random_state, **kwargs)

        if len(X.shape) == 1:
            X = X.reshape(-1, 1)

        self.buffer = []
        for j in range(n_samples):
            features = {f"feature_{col}": X[j][col] for col in range(X.shape[1])}
            metadata = {
                "true_label": int(y[j]),
                "concept_id": self._concept_id
            }

            self.buffer.append(StreamSample(features=features, metadata=metadata))
        self._iterator = iter(self.buffer)

    def get_next_sample(self) -> StreamSample:
        return next(self._iterator)


class StreamGenerator:
    """
    Builder class for building streamable data with generated drift.

    You can create drift with provided methods from this class.
    Adding a concept with the add_abrupt method just adds n data points from a given concept.

    Example
    -------
    >>> concept_1 = RiverConcept(datasets.synth.SEA(variant=0, seed=42), concept_id=0)
    >>> concept_2 = RiverConcept(datasets.synth.SEA(variant=0, seed=44), concept_id=1)
    >>> concept_3 = RiverConcept(datasets.synth.SEA(variant=0, seed=46), concept_id=2)

    >>> builder = StreamGenerator()
    ...
    >>> builder.add_abrupt(concept_1, n_samples=500)
    >>> builder.add_gradual(concept_from=concept_1, concept_to=concept_2, transition_width=400, random_state=42)
    >>> builder.add_abrupt(concept_2, n_samples=300)
    >>> builder.add_gradual(concept_from=concept_2, concept_to=concept_3, transition_width=400, random_state=42)
    >>> builder.add_abrupt(concept_3, n_samples=500)

    If you use the SKLearn concept or use data from a file note that you should not exhaust their data if not neccessary.
    """

    def __init__(self):
        self.data = []

    def add_abrupt(self, concept: Concept, n_samples: int):
        """
        Adds n data points from a given concept.

        :param concept: concept source.
        :param n_samples: number of data points.
        """
        self.data.extend(itertools.islice(concept, n_samples))
        return self

    def add_gradual(self, concept_from: Concept, concept_to: Concept, transition_width: int, random_state: int = 42):
        """
        Adds gradual drifting data points from to given concepts.
        The gradual drift is implemented by using a random take from either the first or the second concept.

        The chance to take from the second concept is raising with the number of datapoints to simulate a gradual drift
        between these concepts.

        :param concept_from: concept starting from drift
        :param concept_to: concept target from drift
        :param transition_width: number of transitioning data points
        :param random_state: random state for reproducible data sets.

        """

        rng = random.Random(random_state)

        for i in range(transition_width):
            prob = i / transition_width

            if rng.random() < prob:
                self.data.append(next(concept_to))
            else:
                self.data.append(next(concept_from))
        return self

    def build(self):
        """
        Builds a streamable data set and converts into a pandas dataframe.
        """
        return Stream(self.data)


def induce_drift_from_data(X, y,
                           classes_before_drift: list,
                           classes_after_drift: list,
                           shared_classes: list = None,
                           samples_per_concept=None,
                           feature_prefix="feature_",
                           random_seed=42):
    """
    Creates a data stream with concept drift from an existing dataset.

    :param X: The features of the dataset.
    :param y: The corresponding labels for the dataset.
    :param classes_before_drift: List of class labels that represent the concept before the drift.
    :param classes_after_drift: List of class labels that represent the concept after the drift.
    :param shared_classes: List of class labels that exist in both concepts.
    These samples will be randomly (50/50) distributed between the before- and after-drift windows.
    :param samples_per_concept: A list of two integers specifying the exact number of
    samples to draw for the first concept and the second concept.
    :param feature_prefix: The prefix used to name the feature columns in the resulting stream (
    e.g., use "px_" for pixel data).
    :param random_seed: Seed for reproducible shuffling and random assignment of shared classes.

    :return: Buildable data stream.

    Note: Any label not
    present in these three lists (classes_before_drift, classes_after_drift, shared_classes) will be automatically ignored.

    Example:

    # Initialize generator
    builder = DataGenerator()

    # Create a stream where the concept drifts from digits '0' and '1' to '2' and '3'
    # Digits '7' and '9' are present throughout the whole stream
    generated_data = builder.induce_drift_from_data(
        X=X,
        y=y,
        classes_before_drift=[0, 1],
        classes_after_drift=[2, 3],
        shared_classes=[7, 9],
        samples_per_concept=[1000, 1000],  # 1000 samples before drift, 1000 after
        feature_prefix="px_"
    ).build()

    """

    if samples_per_concept is None:
        samples_per_concept = [1000, 1000]

    np.random.seed(random_seed)

    shared_classes = shared_classes or []

    # data conversion so that it accepts either dataframes or arrays
    if hasattr(X, "values"):
        X_cols = X.columns
        X_values = X.values
    else:
        X_cols = [f"{feature_prefix}{i}" for i in range(X.shape[1])]
        X_values = X

    concept_before_data = []
    concept_after_data = []

    for i in range(len(X_values)):
        label = y[i]

        # ignore labels which not in lists
        if label not in classes_before_drift and label not in classes_after_drift and label not in shared_classes:
            continue

        features = {col: X_values[i][c_idx] for c_idx, col in enumerate(X_cols)}
        metadata = {"true_label": label}

        # Randomly assign labels of 1 or 2 to samples within shared classes
        #  (i.e., digits that occur both before and after the change point)
        if label in classes_before_drift:
            metadata["concept_id"] = 0
            concept_before_data.append(StreamSample(features=features, metadata=metadata))
        elif label in classes_after_drift:
            metadata["concept_id"] = 1
            concept_after_data.append(StreamSample(features=features, metadata=metadata))
        elif label in shared_classes:
            if np.random.rand() < 0.5:
                metadata["concept_id"] = 0
                concept_before_data.append(StreamSample(features=features, metadata=metadata))
            else:
                metadata["concept_id"] = 1
                concept_after_data.append(StreamSample(features=features, metadata=metadata))

    np.random.shuffle(concept_before_data)
    np.random.shuffle(concept_after_data)

    part_1 = concept_before_data[:samples_per_concept[0]]
    part_2 = concept_after_data[:samples_per_concept[1]]

    data = part_1 + part_2
    return Stream(data)


def save_file(stream: Stream, filepath: str):
    """
    Saves a StreamDataset to a file.
    :param stream: The data stream to save.
    :param filepath: File path to save to.
    """
    # this is more or less for internal use
    if not stream:
        print("Stream is empty")
        return
    data = []
    for sample in stream.data:
        row = {**sample.features, **sample.metadata}
        data.append(row)
    df = pd.DataFrame(data)
    df.to_csv(filepath, index=False)
    print(f"Stream successfully saved to {filepath}")


def from_river(stream: Any, n_samples: int) -> Stream:
    """
    generates a streamable dataset of length n from a river-datastream
    """
    data = []
    iterator = iter(stream)
    for _ in range(n_samples):
        try:
            X, y = next(iterator)
            features = X.copy()
            metadata = {"true_label": y}
            data.append(StreamSample(features=features, metadata=metadata))
        except StopIteration:
            break
    return Stream(data)


def from_file(filepath: str, target_col: str = "true_label") -> Stream:
    """
    Generates a streamable dataset from a .csv-file.
    A streamable data set can be used to generate a data stream using the StreamGenerator-class.

    If you want a specific length for your stream use the take() method to get a certain number of samples from your Stream.


    Example:
    --------
    >>> stream = from_file("my_file.csv")
    >>> # my_stream = stream.take(100) # optional, alternatively just use data instead
    ... # some listeners here
    >>> stream.run()

    """
    df = pd.read_csv(filepath)
    return from_pandas_dataframe(df, target_col)


def from_pandas_dataframe(dataframe: pd.DataFrame, target_col: str = "true_label") -> Stream:
    """
    Returns a streamable dataset of the input dataframe
    A streamable data set can be used to generate a data stream using the StreamGenerator-class.

    If you want a specific length for your stream use the take() method to get a certain number of samples from your Stream.


    Parameters
    ----------
    dataframe : pandas.DataFrame
        Dataset to read from
    target_col : str default="true_label", name of the label. Only change this if implementing custom explainers or detectors.


    Example
    --------
    >>> stream = from_pandas_dataframe(dataframe, target_col)
    >>> # my_stream = stream.take(100) # optional, alternatively just use data instead
    ... # some listeners here
    >>> stream.run()
    """
    data = []

    meta_cols = [target_col, "concept_id"]
    for _, row in dataframe.iterrows():
        row_dict = row.to_dict()
        metadata = {}

        for meta_key in meta_cols:
            if meta_key in row_dict:
                metadata[meta_key] = row_dict.pop(meta_key)
        data.append(StreamSample(features=row_dict, metadata=metadata))
    return Stream(data)


def from_numpy_array(X: np.ndarray, *feature_names: str, y: np.ndarray = None) -> Stream:
    """
    Generates a streamable dataset from a numpy array.
    A streamable data set can be used to generate a data stream using the StreamGenerator-class.

    If you want a specific length for your stream use the take() method to get a certain number of samples from your Stream.

    Parameters
    ----------
    X:
        Feature data
    feature_names:
        feature names, optional. If no names are given they are automatically named
        in the pattern "feature_n" where n is the current feature index
    y:
        Label data

    Example
    --------
    >>> stream = from_numpy_array(X, y=y)
    >>> # my_stream = stream.take(100) # optional, alternatively just use data instead
    ... # some listeners here
    >>> stream.run()

    """
    if len(X.shape) == 1:
        X = X.reshape(-1, 1)
    num_features = X.shape[1]

    if not feature_names:
        feature_names = [f"feature_{i}" for i in range(num_features)]
    elif num_features != len(feature_names):
        raise ValueError(
            f"Number of features({num_features}) does not match number of feature names ({len(feature_names)})")

    data = []
    for i in range(X.shape[0]):
        features = {feature_names[j]: X[i, j] for j in range(num_features)}
        metadata = {}
        if y is not None:
            metadata["true_label"] = y[i]
        data.append(StreamSample(features=features, metadata=metadata))

    return Stream(data)


def from_list(X: list, *feature_names: str, y: list = None) -> Stream:
    """
    Generates a streamable dataset from a python list.
    A streamable data set can be used to generate a data stream using the StreamGenerator-class.

    If you want a specific length for your stream use the take() method to get a certain number of samples from your Stream.

    Parameters
    ----------
        X: list
            Data points
        y: list
            Label data

    Example
    -------
    >>> stream = from_list(X)
    >>> # my_stream = stream.take(100) # optional, alternatively just use data instead
    ... # some listeners here
    >>> stream.run()

    """
    if not X:
        return Stream([])

    # no one does this haha
    if isinstance(X[0], StreamSample):
        return Stream(X)

    # list of dicts
    if isinstance(X[0], dict):
        data = []
        for i, row in enumerate(X):
            metadata = {}
            if y is not None and i < len(y):
                metadata["true_label"] = y[i]
            data.append(StreamSample(features=row, metadata=metadata))
        return Stream(data)

    # just a list
    X_np = np.array(X)
    y_np = np.array(y) if y is not None else None

    return from_numpy_array(X_np, *feature_names, y=y_np)
