from abc import ABC, abstractmethod
from typing import Optional, Dict, Any

from dragon.Base import Base
from dragon.util.EventHandler import EventHandler

import pandas as pd
import numpy as np


class Detector(Base, ABC):
    """
    Shared base class for drift- and changepoint-detectors.
    This class handles basic event handling since both detector subclasses use this mechanism.
    if an explainer is hooked to any detector this explainer will set itself as a listener to this detector instance.
    (see Explanation constructor)

    Please do not implement this class directly and use the specific base classes DriftDetector and ChangepointDetector.

    Parameters
    ----------
    event_name
        Name of output event that is emitted.
        If any explainer or another detector instance needs to listen to this implementation it needs this name to listen to its event.

    """

    def __init__(self, event_name):
        super().__init__()
        self.event_handler = EventHandler()
        self.event_name = event_name

    def emit(self, payload: Optional[Dict[str, Any]] = None, **kwargs):
        if payload is None:
            payload = {}
        payload['detector_name'] = self.__class__.__name__
        payload.update(kwargs)
        self.event_handler.emit(self.event_name, **payload)


class DriftDetector(Detector, ABC):
    _supported_events = {"drift_detected"}

    """
    Base class for drift detectors.

    Implement this class if adding custom drift detectors.

    Parameters
    ----------
    event_name
        Name of output event that is emitted.
        If any explainer or another detector instance needs to listen to this implementation it needs this name to listen to its event.

    Notes
    -----
    This class is specifically tailored to drift detectors.
    If implementing a Changepoint Detector please implement its respective base class.
    """

    def __init__(self, event_name: str = "drift_detected"):
        super().__init__(event_name=event_name)

    @abstractmethod
    def detect(self, *args, **kwargs):
        """
        Base method for detecting drift
        This method should be implemented by subclasses.

        Parameters are completely up to your implementation but this method must return a dictionary with at least following keys:

        * metric: name of the metric

        """
        pass


class BlockBasedDriftDetector(DriftDetector, ABC):
    def __init__(self, window, time_feature=None, data_feature=None, event_name: str = "drift_detected"):
        super().__init__(event_name)
        self.window = window
        self.time_feature = time_feature
        self.data_feature = data_feature

        self.window.add_listener("state_changed", self.detect)

    def _extract_data(self):

        feature_data = np.array([list(d.features.values()) for d in self.window.get_data()])
        df = pd.DataFrame(feature_data)

        time_feature = self.time_feature
        data_feature = self.data_feature

        if time_feature is None:
            time = np.linspace(0, 1, df.shape[0])
            data = df.values

        if type(time_feature) is str:
            time_feature = [time_feature]
        if type(time_feature) is list:
            assert len(time_feature) > 0 and all([type(feat) is str for feat in time_feature])
            data_feature = [c for c in df.columns if c not in time_feature]
            time = df[time_feature].values
            data = df[data_feature].values

        if callable(time_feature):
            time = time_feature(df)

        if data_feature is not None:
            if type(data_feature) is str:
                data_feature = [data_feature]
            if type(data_feature) is list:
                data = df[data_feature].values
            if callable(data_feature):
                data = data_feature(df)

        return data, time

    def detect(self, *args, **kwargs):
        data, time = self._extract_data()
        found_drift, payload = self.test(data, time)
        if found_drift:
            self.emit(payload)

    @abstractmethod
    def test(self, data, time):
        """
        implement this method in your concrete detector implementation.
        should return a tuple: (bool: drift found, dict: payload)
        """
        pass


class TwoWindowBasedDriftDetector(DriftDetector, ABC):
    def __init__(self, ref_window, cur_window, pre_process=None, event_name: str = "drift_detected"):
        super().__init__(event_name)
        self.ref_window = ref_window
        self.cur_window = cur_window
        self.pre_process = pre_process

        self.cur_window.add_listener("state_changed", self.detect)

    def detect(self, *args, **kwargs):

        w2 = np.array([list(d.features.values()) for d in self.cur_window.get_data()])
        w1 = np.array([list(d.features.values()) for d in self.ref_window.get_data()])

        pre_process = self.pre_process
        if pre_process is not None:
            if type(pre_process) is str:
                pre_process = [pre_process]
            if type(pre_process) is list:
                assert len(pre_process) > 0 and all([type(feat) is str for feat in pre_process])
                w1, w2 = w1[pre_process], w2[pre_process]

            if callable(pre_process):
                w1, w2 = pre_process(w1), pre_process(w2)

        assert type(w1) == type(w2)
        if type(w1) is pd.DataFrame:
            w1, w2 = w1.values, w2.values
        assert type(w1) == np.ndarray

        found_drift, payload = self.test(w1, w2)
        if found_drift:
            self.emit(payload=payload)

    @abstractmethod
    def test(self, w1, w2):
        """
        implement this method in your concrete detector implementation.
        should return a tuple: (bool: drift found, dict: payload)
        """
        pass


class ChangepointDetector(Detector, ABC):
    _supported_events = {"changepoint_localized"}

    """
    Base class for changepoint detectors.

    Implement this class if adding custom changepoint detectors.

    Parameters
    ----------
    windows
        List of windows in reversed chronological order meaning the first window with the oldest data is input as argument
        in the constructor first but processed in reversed order.
        You do not to worry about pre-processing since the base class does all the heavy lifting.

        Example

        win1 (older data)

        win2 (newer data)

        yourDet = YourDetClass(windows=[win1, win2], (other arguments))

    Notes
    -----
    Pre- and Post-Processing of data is handled in this class.
    (i.e. converting global indices to local indices and event handling)
    """

    def __init__(self, windows: list,
                 event_name: str = "changepoint_localized"):
        super().__init__(event_name=event_name)
        self.windows = windows

    def _get_combined_data(self):
        """
        You do not need to implement or call this method.
        """
        return [data for window in self.windows for data in window.get_data()]

    def _on_drift_detected(self, **payload):
        """
        You do not need to implement or call this method.
        """
        self._process_and_emit(payload)

    def _process_and_emit(self, payload_kwargs):
        """
        You do not need to implement or call this method.
        """

        combined_data = self._get_combined_data()
        total_length = len(combined_data)

        if total_length == 0:
            return

        cp_indices = self.detect(combined_data)

        if not cp_indices:
            return

        # safety check if the user returns integers
        if not isinstance(cp_indices, list):
            cp_indices = [cp_indices]

        payload = payload_kwargs.copy()
        # if dd -> cp then change metric name
        payload['metric'] = 'changepoint'
        if 'score' in payload:
            payload['trigger_score'] = payload.pop('score')

        payload['split_indices'] = cp_indices

        self.emit(payload=payload)

    @abstractmethod
    def detect(self, combined_data) -> list:
        """
        Base method for localizing change points.

        After detection and returning a list of change points these points will be automatically converted
        to local and global indices and returned as an event with default name "changepoint_localized".

        Parameters
        ----------
        combined_data
            pre-processed data of input windows. The windows are selected in reversed chronological order
            meaning the first window with the oldest data is input as argument in the constructor
            first but processed in reversed order.
            You do not to worry about pre-processing since the base class does all the heavy lifting.

            Example

            sw1 (older data)

            sw2 (newer data)

            yourDet = YourDetClass(windows=[sw1, sw2], (other arguments))

        Needs to return a list of indices where drift is localized.
        """
        pass
