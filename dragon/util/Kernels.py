import numpy as np
from scipy.stats import ortho_group

from sklearn.base import clone
from sklearn.metrics.pairwise import euclidean_distances
from sklearn.metrics.pairwise import pairwise_kernels, PAIRWISE_KERNEL_FUNCTIONS
from sklearn.ensemble import RandomTreesEmbedding

from sklearn.random_projection import GaussianRandomProjection as skGaussianRandomProjection
from sklearn.random_projection import SparseRandomProjection as skSparseRandomProjection
from sklearn.decomposition import PCA as skPCA

from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomTreesEmbedding
from sklearn.cluster import MiniBatchKMeans


def apply_kernel(X, metric="rbf", size=-1):
    if isinstance(metric, list):
        current_data = X
        for step in metric:
            if hasattr(step, "fit_transform"):
                current_data = step.fit_transform(current_data)
            else:
                current_data = step.transform(current_data)
        return current_data

    return build_kernel(metric, size)[-1].fit_transform(X)


def build_kernel(metric="rbf", size=-1):
    pipeline = []
    for met in metric.split("->"):
        if met == "Id":
            pass
        elif met == "Scale":
            pipeline.append(Scaler(size=size))
        elif met in PAIRWISE_KERNEL_FUNCTIONS.keys():
            pipeline.append(SklearnKernel(size=size, kernel=met))
        elif met == "PCA":
            pipeline.append(PCA(size=size))
        elif met == "GaussRandProj":
            pipeline.append(GaussianRandomProjection(size=size))
        elif met == "SparseRandProj":
            pipeline.append(SparseRandomProjection(size=size))
        elif met == "normalized_rbf":
            pipeline.append(NormalizedRBFKernel(size=size))
        elif met == "rbf": # just to remove the error, this may be wrong here
            pipeline.append(NormalizedRBFKernel(size=size))
        elif met == "rand_tree":
            pipeline.append(RandomTreeKernel(size=size))
        elif met == "rand_bin":
            pipeline.append(RandomBinningKernel(size=size))
        elif met == "RandRot":
            pipeline.append(RandRot())

        else:
            raise ValueError(f"unkown transformation {met}")
    if not (len(pipeline) > 0 and hasattr(pipeline[-1], 'is_kernel') and pipeline[-1].is_kernel()):
        raise ValueError("described object is no kernel")
    return pipeline


class KernelPipeline:
    def __init__(self):
        self.pipeline = []
        self.fitted = True

    def append(self, kernel_or_transformer):
        if self.is_kernel:
            raise ValueError("cannot build upon a kernel")
        self.pipeline.append(kernel_or_transformer)
        self.fitted = False
        return self

    def fit(self, X, y=None):
        for elm in self.pipeline:
            X = elm.fit_transform(X)
        return self

    def transform(self, X):
        if not self.fitted:
            raise ValueError("Pipeline not fitted yet")
        for elm in self.pipeline:
            X = elm.transform(X)

    def fit_transform(self, X, y=None):
        return self.fit(X, y).transform(X)

    def is_kernel(self):
        return len(self.pipeline) > 0 and self.pipeline[-1].is_kernel


class RandRot:
    def __init__(self):
        pass

    def fit(self, X, y=None):
        self.R = ortho_group.rvs(dim=X.shape[1])
        return self

    def transform(self, X):
        return X @ self.R

    def fit_transform(self, X, y=None):
        return self.fit(X, y).transform(X)

    def is_kernel(self):
        return False


class PCA:
    def __init__(self, n_components=10, size=-1):
        self.size = size
        self.n_components = n_components

    def fit(self, X, y=None):
        self.proj = skPCA(n_components=min(X.shape[1], self.n_components)).fit(X[:self.size])
        return self

    def transform(self, X):
        return self.proj.transform(X)

    def fit_transform(self, X, y=None):
        return self.fit(X, y).transform(X)

    def is_kernel(self):
        return False


class GaussianRandomProjection:
    def __init__(self, n_components=10, size=-1):
        self.size = size
        self.n_components = n_components

    def fit(self, X, y=None):
        self.proj = skGaussianRandomProjection(n_components=min(X.shape[1], self.n_components)).fit(X[:self.size])
        return self

    def transform(self, X):
        return self.proj.transform(X)

    def fit_transform(self, X, y=None):
        return self.fit(X, y).transform(X)

    def is_kernel(self):
        return False


class SparseRandomProjection:
    def __init__(self, n_components=10, size=-1):
        self.size = size
        self.n_components = n_components

    def fit(self, X, y=None):
        self.proj = skSparseRandomProjection(n_components=min(X.shape[1], self.n_components)).fit(X[:self.size])
        return self

    def transform(self, X):
        return self.proj.transform(X)

    def fit_transform(self, X, y=None):
        return self.fit(X, y).transform(X)

    def is_kernel(self):
        return False


class Scaler:
    def __init__(self, size=-1):
        self.size = size

    def fit(self, X, y=None):
        self.scaler = StandardScaler().fit(X[:self.size])
        return self

    def transform(self, X):
        return self.scaler.transform(X)

    def fit_transform(self, X, y=None):
        return self.fit(X, y).transform(X)

    def is_kernel(self):
        return False


class SklearnKernel:
    def __init__(self, size=-1, kernel="linear"):
        self.size = size
        self.kernel = kernel

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        return pairwise_kernels(X, metric=self.kernel)

    def fit_transform(self, X, y=None):
        return self.fit(X, y).transform(X)

    def is_kernel(self):
        return True


class NormalizedRBFKernel:
    def __init__(self, size=-1):
        self.size = size
        self.med = 1

    def fit(self, X, y=None):
        self.med = np.median(euclidean_distances(X[:self.size], squared=True))
        if self.med <= 1e-16:
            print("WARN small median")
            self.med = 1

        return self

    def transform(self, X):
        return np.exp(-euclidean_distances(X, squared=True) / self.med)

    def fit_transform(self, X, y=None):
        return self.fit(X, y).transform(X)

    def is_kernel(self):
        return True


class RandomTreeKernel:
    def __init__(self, size=-1):
        self.size = size

    def fit(self, X, y=None):
        self.trees = RandomTreesEmbedding(sparse_output=False).fit(X[:self.size])
        return self

    def transform(self, X):
        L = self.trees.apply(X)

        K = np.zeros(shape=(X.shape[0], X.shape[0]))
        for l in L.T:
            K += 1. * (l[:, None] == l[None, :])
        K /= L.shape[0]

        return K

    def fit_transform(self, X, y=None):
        return self.fit(X, y).transform(X)

    def is_kernel(self):
        return True


class RandomBinningKernel:
    def __init__(self, size=-1, n_proj=250, n_bins=3, cdf_transform=True):
        self.size = size
        self.n_proj = n_proj
        self.n_bins = n_bins
        self.cdf_transform = cdf_transform

    def fit(self, X):
        W = np.random.normal(size=(self.n_proj, X.shape[1]))
        self.W = (W / (((W ** 2).sum(axis=1)) ** 0.5)[:, None]).T
        self.P = X[:self.size] @ self.W
        return self

    def transform(self, X):
        P = X @ self.W

        if self.cdf_transform:
            P = (P[:, None, :] <= self.P[None, :, :]).mean(axis=1)
        else:
            mi, ma = self.P.min(axis=0), self.P.max(axis=0)
            P = (P - mi[None, :]) / (ma - mi)[None, :]

        l = (P * self.n_bins).round().astype(int)
        l[l < 0] = -1
        l[l > self.n_bins] = -2

        return (l[None, :, :] == l[:, None, :]).mean(axis=2)

    def fit_transform(self, X, y=None):
        return self.fit(X, y).transform(X)

    def is_kernel(self):
        return True
