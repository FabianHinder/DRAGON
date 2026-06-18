import numpy as np

from dragon.detectors.BaseDetector import TwoWindowBasedDriftDetector
from dragon.util.Kernels import apply_kernel


class MMD(TwoWindowBasedDriftDetector):
    """
    Basic MMD-Drift detector implementation.

    This detector can be passed as an argument to an explainer and hooks itself automatically to a data stream.

    :param ref_window: The reference window, usually the older data.
    :param cur_window: The current test window (newer data).
    :param threshold: Strictness of the metric.
    :param n_perm: Number of permutations.
    :param kernel: kernel type (str).
    :param pre_process: pre_process function.

    If a drift is detected the detector emits an event named "drift_detected"
    This event holds a payload with following format:

    payload = {
    "metric": "mmd",
    "score": mmd_score,
    "p_value": p_val,
    }

    You can access this event by subscribing to this using the add_listener method.

    Example
    -------
    >>> def handle_drift(**payload):
    ...     print(f"Found drift! Score: {payload['score']}, P-Val: {payload['p_value']}")

    >>> my_detector = MMD(my_pipe["ref"], my_pipe["cur"])
    >>> my_detector.add_listener(handle_drift)




    """

    def __init__(self, ref_window, cur_window, threshold=0.05, n_perm=2500, kernel="rbf", pre_process=None):
        super().__init__(ref_window=ref_window, cur_window=cur_window, pre_process=pre_process)
        self.threshold = threshold
        self.n_perm = n_perm
        self.kernel = kernel

    def test(self, ref_window, cur_window):
        if ref_window.size == 0 or cur_window.size == 0:
            return False, None
        if ref_window.shape[1] != cur_window.shape[1]:
            return False, None

        combined_x = np.vstack([ref_window, cur_window])
        s_size = ref_window.shape[0]
        mmd_score, p_val = self._mmd(combined_x, s_size, self.n_perm, self.kernel)
        if p_val < self.threshold:
            payload = {
                "metric": "mmd",
                "score": float(mmd_score),
                "p_value": float(p_val),
            }
            return True, payload
        return False, None

    def gen_window_matrix(self, l1, l2, n_perm, cache=None):
        if cache is None:
            cache = dict()
        if (l1, l2, n_perm) not in cache.keys():
            w = np.array(l1 * [1. / l1] + (l2) * [-1. / (l2)])
            W = np.array([w] + [np.random.permutation(w) for _ in range(n_perm)])
            cache[(l1, l2, n_perm)] = W
        return cache[(l1, l2, n_perm)]

    def _mmd(self, X, s, n_perm, X_kernel):
        if isinstance(X_kernel, list):
            K_X = apply_kernel(X, metric=X_kernel)
        elif type(X_kernel) is str:
            K_X = apply_kernel(X, metric=X_kernel)
        elif hasattr(X_kernel, "fit_transform"):
            K_X = X_kernel.fit_transform(X)
        elif hasattr(X_kernel, "transform"):
            K_X = X_kernel.transform(X)
        else:
            raise ValueError("invalid X kernel")

        if s is None:
            s = int(X.shape[0] / 2)

        W = self.gen_window_matrix(s, K_X.shape[0] - s, n_perm)
        s = np.einsum('ij,ij->i', np.dot(W, K_X), W)
        p = (s[0] < s).sum() / n_perm
        return s[0], p

