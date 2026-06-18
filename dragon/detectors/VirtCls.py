import numpy as np
from sklearn.model_selection import StratifiedKFold
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score as roc
from sklearn.base import clone

from dragon.detectors.BaseDetector import TwoWindowBasedDriftDetector


class VirtualClassifier(TwoWindowBasedDriftDetector):
    """
    Basic VirtualClassifier-Drift detector implementation.

    :param ref_window: The reference window, usually the older data.
    :param cur_window: The current test window (newer data).
    :param threshold: Strictness of the metric.
    :param model: default is Logistic regression model.
    :param pre_process: pre_process function.

    If a drift is detected the detector emits an event named "drift_detected"
    This event holds a payload with following format:

    payload = {
    "metric": "VirtClassifier",
    "model": self.model,
    "auc_score": auc_score,
    }

    """

    def __init__(self, ref_window, cur_window, threshold=0.05, model=LogisticRegression(solver='liblinear'),
                 pre_process=None):
        super().__init__(ref_window=ref_window, cur_window=cur_window, pre_process=pre_process)
        self.threshold = threshold
        self.model = model

    def test(self, ref_window, cur_window):

        if len(ref_window) < 2 or len(cur_window) < 2:
            return False, None

        X = np.vstack([ref_window, cur_window])
        y = np.ones(X.shape[0])
        y[:ref_window.shape[0]] = 0

        predictions = np.zeros(y.shape)

        skf = StratifiedKFold(n_splits=2, shuffle=True)
        try:
            for train_idx, test_idx in skf.split(X, y):
                X_train, X_test = X[train_idx], X[test_idx]
                y_train, y_test = y[train_idx], y[test_idx]
                clf = clone(self.model)
                clf.fit(X_train, y_train)
                probs = clf.predict_proba(X_test)[:, 1]
                predictions[test_idx] = probs
            auc_score = roc(y, predictions)

            if auc_score - 0.5 > self.threshold:
                payload = {
                    "metric": "VirtClassifier",
                    "model": self.model,
                    "auc_score": float(auc_score),
                }
                return True, payload
        except Exception as e:
            print(f"Skipping evaluation due to model error: {e}")
        return False, None
