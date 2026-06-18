import csv
import os
import tempfile
import unittest

import pandas as pd
from river.datasets import synth
from sklearn.datasets import make_blobs

from dragon.util.Stream import Concept, RiverConcept, SKlearnConcept, FileConcept

class DummyConcept(Concept):
    """Dummy concept for testing"""

    def __init__(self, max_samples=5):
        self.counter = 0
        self.max_samples = max_samples

    def get_next_sample(self):
        if self.counter >= self.max_samples:
            raise StopIteration("Empty!")

        row = {"feature_0": self.counter, "true_label": 1, "concept_id": 99}
        self.counter += 1
        return row


class TestBaseConcept(unittest.TestCase):
    def test_take_exact_amount(self):
        concept = DummyConcept(max_samples=10)
        data = concept.take(3)


        self.assertEqual(len(data), 3)
        self.assertEqual(data[0]["feature_0"], 0)
        self.assertEqual(data[2]["feature_0"], 2)

    def test_take_exhausted_source(self):
        concept = DummyConcept(max_samples=2)
        data = concept.take(5)
        self.assertEqual(len(data), 2, "take() should have aborted after 2 samples!")
        self.assertEqual(data[-1]["feature_0"], 1)


class TestRiverConcept(unittest.TestCase):
    def test_river_translation(self):
        sea_stream = synth.SEA(variant=0, seed=42)
        concept = RiverConcept(sea_stream, concept_id=7)

        sample = concept.get_next_sample()

        self.assertEqual(sample.metadata["concept_id"], 7)
        self.assertIn("true_label", sample.metadata)

        self.assertIn("feature_0", sample.features)
        self.assertIn("feature_1", sample.features)
        self.assertIn("feature_2", sample.features)
        self.assertIsInstance(sample.features["feature_0"], float)


class TestSklearnConcept(unittest.TestCase):

    def test_sklearn_translation_and_shape(self):
        concept = SKlearnConcept(
            make_blobs,
            concept_id=5,
            n_samples=10,
            n_features=3,
            centers=2,
            random_state=42
        )

        sample = concept.get_next_sample()

        self.assertEqual(sample.metadata["concept_id"], 5)
        self.assertIn("true_label", sample.metadata)

        self.assertIn("feature_0", sample.features)
        self.assertIn("feature_1", sample.features)
        self.assertIn("feature_2", sample.features)
        self.assertNotIn("feature_3", sample.features)
        self.assertIsInstance(sample.features["feature_0"], float)

    def test_buffer_exhaustion(self):
        concept = SKlearnConcept(make_blobs, concept_id=1, n_samples=5, random_state=42)

        data_part1 = concept.take(3)
        self.assertEqual(len(data_part1), 3)

        data_part2 = concept.take(5)
        self.assertEqual(len(data_part2), 2)

        data_empty = concept.take(1)
        self.assertEqual(len(data_empty), 0)


class TestFileConcept(unittest.TestCase):

    def setUp(self):
        """dummy file for testing"""
        self.temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, newline='')
        self.file_path = self.temp_file.name

        writer = csv.writer(self.temp_file)
        writer.writerow(["f0", "f1", "f2", "target"])
        writer.writerow(["1.5", "2.0", "A", "0"])
        writer.writerow(["-6.9", "4.2", "B", "1"])
        self.temp_file.close()

    def tearDown(self):
        os.remove(self.file_path)

    def test_csv_reading(self):
        concept = FileConcept(self.file_path, concept_id=3, target_col="target")

        sample1 = concept.get_next_sample()
        self.assertEqual(sample1.metadata["concept_id"], 3)
        self.assertEqual(sample1.metadata["true_label"], 0)

        self.assertEqual(sample1.features["feature_0"], 1.5)

        self.assertEqual(sample1.features["feature_2"], "A")

        sample2 = concept.get_next_sample()
        self.assertEqual(sample2.metadata["true_label"], 1)
        self.assertEqual(sample2.features["feature_0"], -6.9)

    def test_csv_exhaustion(self):
        concept = FileConcept(self.file_path, concept_id=3, target_col="target")

        data = concept.take(3)  # take one more than in file

        self.assertEqual(len(data), 2, "Reader should have aborted after 2 samples!")
        self.assertTrue(concept._file.closed, "File should be closed!")
