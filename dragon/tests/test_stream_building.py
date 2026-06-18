import unittest
import numpy as np
import pandas as pd

from dragon.util.Stream import from_numpy_array, StreamGenerator, induce_drift_from_data, Concept, from_list
from dragon.util.DataTypes import StreamSample


class DummyConcept(Concept):
    def __init__(self, concept_id: int, feature_value: float):
        self.concept_id = concept_id
        self.feature_value = feature_value

    def get_next_sample(self) -> StreamSample:
        return StreamSample(
            features={"feature_0": self.feature_value},
            metadata={
                "true_label": self.concept_id,
                "concept_id": self.concept_id
            }
        )


class TestStreamFramework(unittest.TestCase):

    def test_from_numpy_array_auto_names(self):
        X = np.array([[1.0, 2.0], [3.0, 4.0]])
        dataset = from_numpy_array(X)
        data = dataset.take(2)
        df = pd.DataFrame([{**d.features, **d.metadata} for d in data.data])
        self.assertListEqual(list(df.columns), ["feature_0", "feature_1"])
        self.assertEqual(len(df), 2)

    def test_from_numpy_array_with_labels_and_names(self):
        X = np.array([[1.0], [2.0], [3.0]])
        y = np.array([0, 1, 0])
        dataset = from_numpy_array(X, "feature_A", y=y)
        data = dataset.take(3)
        df = pd.DataFrame([{**d.features, **d.metadata} for d in data.data])

        self.assertIn("feature_A", df.columns)
        self.assertIn("true_label", df.columns)
        self.assertEqual(df["true_label"].iloc[1], 1)

    def test_from_numpy_array_name_mismatch(self):
        X = np.array([[1.0, 2.0]])
        with self.assertRaises(ValueError):
            from_numpy_array(X, "only_one_feature")

    def test_take_from_list(self):
        raw_list = [{"feat": 1}, {"feat": 2}, {"feat": 3}, {"feat": 4}]
        dataset = from_list(raw_list)
        data = dataset.take(2)

        self.assertIsInstance(data.data, list)
        self.assertIsInstance(data.data[0], StreamSample)
        self.assertEqual(len(data), 2)
        self.assertEqual(data.data[-1].features["feat"], 2)

    def test_take_from_concept(self):
        concept = DummyConcept(concept_id=0, feature_value=99.0)
        builder = StreamGenerator()
        builder.add_abrupt(concept, n_samples=5)
        dataset = builder.build()

        data = dataset.take(5)
        df = pd.DataFrame([{**d.features, **d.metadata} for d in data.data])

        self.assertEqual(len(df), 5)
        self.assertEqual(df["feature_0"].iloc[0], 99.0)

    def test_stream_builder_abrupt_and_gradual(self):
        concept_a = DummyConcept(concept_id=0, feature_value=1.0)
        concept_b = DummyConcept(concept_id=1, feature_value=2.0)

        builder = StreamGenerator()
        builder.add_abrupt(concept_a, n_samples=10)
        builder.add_gradual(concept_a, concept_b, transition_width=10, random_state=42)
        builder.add_abrupt(concept_b, n_samples=10)

        dataset = builder.build()
        data = dataset.take(30)
        df = pd.DataFrame([{**d.features, **d.metadata} for d in data.data])

        self.assertEqual(len(df), 30)

    def test_induce_drift_from_data(self):
        X = np.random.rand(100, 2)
        y = np.array([0] * 50 + [1] * 50)

        dataset = induce_drift_from_data(
            X=X, y=y,
            classes_before_drift=[0],
            classes_after_drift=[1],
            samples_per_concept=[10, 10],
            feature_prefix="feat_"
        )

        data = dataset.take(20)
        df = pd.DataFrame([{**d.features, **d.metadata} for d in data.data])

        self.assertEqual(len(df), 20)
        self.assertIn("feat_0", df.columns)

        # check first half is concept 0 and second half is concept 1
        self.assertEqual(df["concept_id"].iloc[0], 0)
        self.assertEqual(df["concept_id"].iloc[19], 1)
