from dragon.detectors.BaseDetector import BlockBasedDriftDetector

import numpy as np
from dragon.util.Kernels import apply_kernel
from sklearn.metrics.pairwise import euclidean_distances, pairwise_kernels


def get_time_kernel(n_size, n_perm, kernel, cache=None):
    if cache is None:
        cache = dict()
    default_config = dict(
        [(name, {"type": "standard", "name": name}) for name in
         ["additive_chi2", "chi2", "linear", "poly", "polynomial", "rbf", "laplacian", "sigmoid", "cosine",
          "normalized_rbf"]] +
        [(name, {"type": "design", "name": name}) for name in ["in-group", "between-group"]] +
        [("mmd", {"type": "design", "name": "between-group"}),
         ("no_weight_mmd", {"type": "design", "name": "between-group", "weight": False})]
    )
    if type(kernel) is str and kernel in default_config.keys():
        kernel = default_config[kernel]
    kernel_name = repr(kernel)

    if (n_size, n_perm, kernel_name) not in cache.keys():
        T = np.linspace(-1, 1, n_size).reshape(-1, 1)
        H = np.eye(n_size) - (1 / n_size) * np.ones((n_size, n_size))

        if kernel["type"] == "design":
            diff = (kernel["name"] == "between-group")
            weight = kernel.get("weight", diff)
            shift = kernel.get("shift", 1 if weight else T.shape[0] // 10)

            assert not (weight and not diff)
            ws = 0
            K = np.zeros((n_size, n_size))
            for i in range(shift, n_size - shift):
                w = (i * (n_size - i)) ** 0.5 if weight else 1
                v = np.hstack((np.ones(i), np.zeros(n_size - i)))
                if diff:
                    v = v / i - (1 - v) / (n_size - i)
                K += w * np.outer(v, v)
                ws += w
            K /= ws
        elif kernel["type"] == "standard" and kernel["name"] == "normalized_rbf":
            D = euclidean_distances(T, squared=True)
            D /= np.median(D)
            K = np.exp(-D)
        elif kernel["type"] == "standard":
            K = pairwise_kernels(T, metric=kernel["name"])
        else:
            raise ValueError("kernel not found")

        Kc = H @ K @ H
        Ks = [Kc]
        for _ in range(n_perm):
            perm = np.random.permutation(T.shape[0])
            Ks.append(Kc[:, perm][perm, :])
        Ks = np.array(Ks)
        Ks = Ks.reshape(n_perm + 1, n_size * n_size)
        cache[(n_size, n_perm, kernel_name)] = Ks
    return cache[(n_size, n_perm, kernel_name)]


class HSIC(BlockBasedDriftDetector):
    """
    Basic implementation of HSIC
    """

    def __init__(self, window, data_feature=None, n_perm=2500, kernel="normalized_rbf", T_kernel="normalized_rbf"):
        super().__init__(window=window, data_feature=data_feature, time_feature=None)

        self.n_perm = n_perm
        self.X_kernel = kernel
        self.T_kernel = T_kernel

    def test(self, s, time=None):
        X = s
        n_size = X.shape[0]

        if type(self.X_kernel) is str:
            K_X = apply_kernel(X, metric=self.X_kernel)
        elif hasattr(self.X_kernel, "fit_transform"):
            K_X = self.X_kernel.fit_transform(X)
        elif hasattr(self.X_kernel, "transform"):
            K_X = self.X_kernel.transform(X)
        else:
            raise ValueError("invalid X kernel")

        s = get_time_kernel(n_size, self.n_perm, self.T_kernel) @ K_X.ravel()
        p = (s[0] < s).sum() / self.n_perm

        return p, {}
