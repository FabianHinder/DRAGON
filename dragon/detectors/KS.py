import numpy as np
from scipy.stats import ks_2samp

from dragon.detectors.BaseDetector import TwoWindowBasedDriftDetector


class KS(TwoWindowBasedDriftDetector):
    """
    Basic KS-Drift detector implementation.

    This detector can be passed as an argument to an explainer and hooks itself automatically to a data stream.

    :param ref_window: The reference window, usually the older data.
    :param cur_window: The current test window (newer data).
    :param threshold: Strictness of the metric.
    :param aggregate: How to aggregate scores.
    :param pre_process: Pre-processing function.

    If a drift is detected the detector emits an event named "drift_detected"
    This event holds a payload with following format:

    payload = {
    "metric": "KS",
    "p_value": p_val,
    }

    """

    def __init__(self, ref_window, cur_window, threshold=0.05, aggregate=np.min, pre_process=None):
        super().__init__(ref_window=ref_window, cur_window=cur_window, pre_process=pre_process)
        self.threshold = threshold
        self.aggregate = aggregate

    def test(self, ref_window, cur_window):
        if len(ref_window) < 2 or len(cur_window) < 2:
            return False, None

        num_features = ref_window.shape[1]

        p_val = self.aggregate([ks_2samp(ref_window[:, i], cur_window[:, i]).pvalue for i in range(num_features)])

        if p_val < self.threshold:
            payload = {
                "metric": "KS",
                "p_value": float(p_val),
            }
            return True, payload
        return False, None
