from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import List, Any, Callable, Optional

import pandas as pd

from dragon.Base import Base
from dragon.util.DataTypes import StreamSample
from dragon.util.EventHandler import EventHandler
from dragon.util.DataTypes import StateChangeEvent


class ObservablePropertyMixin(ABC):
    """
    add whatever attribute needs to be observed in list
    add this list (with its respective attribute names (e.g. max_size in window)) to any child class,
    that needs to be observed
    """
    _observable_properties = set()

    def __init__(self, **kwargs):
        if not hasattr(self, 'event_handler'):
            self.event_handler = EventHandler()
        super().__init__(**kwargs)

    def __setattr__(self, key, value):
        """
        if any attribute in observables list in any child class is set to a new value,
        an event with its respective value
        old_value is None for example in initialization. in any other case an old value should exist
        therefore it fires in init with params, or only when the value changes
        """
        if key in getattr(self, "_observable_properties", set()):

            old_value = getattr(self, key, None)

            if old_value != value:
                super().__setattr__(key, value)
                if hasattr(self, "event_handler"):
                    self.event_handler.emit(
                        "property_changed",
                        property_name=key,
                        old_value=old_value,
                        new_value=value
                    )
                return
        super().__setattr__(key, value)


class DRAGONDataHandler(ABC):
    def __init__(self, **kwargs):
        pass

    def is_dataprovider(self):
        return False

    def is_flow_transformer(self):
        return False

    def is_linear_flow(self):
        return False


class StreamObject(ObservablePropertyMixin, DRAGONDataHandler, Base, ABC):
    """
    Superclass for all StreamObjects (i.e. objects that directly listen to the stream)
    This class is responsible for event handling for StreamObject subclasses.
    """

    _supported_events = {"out_flow", "state_changed", "property_changed", "structure_changed"}

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.event_handler = EventHandler()
        self.current_index = -1
        self._source_node = None
        self.open_state = True
        self._parent = None

    def get_root_node(self):
        if self._parent is None:
            return self
        else:
            return self._parent.get_root_node()

    def set_parent(self, parent):
        self._parent = parent

    def is_linear_flow(self):
        return True

    def _bind_listener(self, event_name, callback):
        if event_name == "out_flow":
            self._add_out_flow_listener(callback)
        elif event_name == "property_changed":
            self.event_handler.add_listener("property_changed", callback)
        else:
            self.event_handler.add_listener(self._get_event_name(event_name), callback)

    def _unbind_listener(self, event_name: str, callback):
        if event_name == "out_flow":
            self._remove_out_flow_listener(callback)
        elif event_name == "property_changed":
            self.event_handler.remove_listener("property_changed", callback)
        else:
            self.event_handler.remove_listener(self._get_event_name(event_name), callback)

    # make events unique since it recursively called itself resulting in massive errors
    def _get_event_name(self, base_name):
        return f"{base_name}_{id(self)}"

    def _add_out_flow_listener(self, callback):
        if hasattr(callback, "__self__") and isinstance(callback.__self__, StreamObject):
            is_flow_connection = (hasattr(callback, "__self__") and
                                  isinstance(callback.__self__, StreamObject) and
                                  getattr(callback, "__name__", "") == "push")
            if is_flow_connection:
                target_node = callback.__self__

                if self.is_linear_flow() and any(
                        map(lambda x: hasattr(x, "__self__") and isinstance(x.__self__, StreamObject) and getattr(x,
                                                                                                                  "__name__",
                                                                                                                  "") == "push",
                            self.event_handler.get_listeners())):
                    raise ValueError(
                        "You are trying to build a forking flow structure not covered by object structure. Use a Fork object")

                # This framework only allows tree structures so any applied listeners check themselves for any kind of cycle.
                if target_node._source_node is not None:
                    raise ValueError(
                        f"You are trying to build a cyclic structure. This framework only allows tree-like structures.")

                current_anc = self
                # checks all ancestor nodes for any cyclic occurrences
                while current_anc is not None:
                    if current_anc == target_node:
                        raise RecursionError(
                            f"You are trying to build a cyclic structure. This framework only allows tree-like structures.")
                    current_anc = current_anc._source_node

                target_node._source_node = self  
        else:
            print("Your listener is no StreamObject. It will only listen to the out_flow and not change any Structure!")
        self.event_handler.add_listener(self._get_event_name("out_flow"), callback)

    def _remove_out_flow_listener(self, callback):
        is_flow_connection = (hasattr(callback, "__self__") and
                              isinstance(callback.__self__, StreamObject) and
                              getattr(callback, "__name__", "") == "push")

        if is_flow_connection:
            target_node = callback.__self__
            if target_node._source_node == self:
                target_node._source_node = None

        self.event_handler.remove_listener(self._get_event_name("out_flow"), callback)

    def _emit_out_flow(self, data_out: List[Any]):
        self.event_handler.emit(self._get_event_name("out_flow"), samples=data_out)

    def open(self):
        """
        Opens the StreamObject to accept new data points.
        Only an open StreamObject accepts data points.
        """
        self.open_state = True

    def close(self):
        """
        Closes the StreamObject to decline new data points.
        Only an open StreamObject accepts data points.
        """
        self.open_state = False

    @abstractmethod
    def push(self, samples):
        """
        Base method for StreamObjects for pushing new samples in and/or out.

        This method is called by any new datapoint/s.
        :param samples: List of datapoints. These are automatically converted into a list by its calling method. (see _on_new_data)
        """
        pass

    def on_new_data(self, data):
        """
        if new data point is triggered, the StreamObject calls its respective push-method.
        :param data: incoming data.
        """
        self.push([data])


def get_data(source):
    if isinstance(source, DataProvider):
        return source.get_data()
    elif isinstance(source, pd.DataFrame):
        return source.to_dict(orient="records")
    elif isinstance(source, Iterable):
        data = []
        for s in source:
            data.extend(get_data(s))
        return data
    else:
        raise ValueError("Expected a single or a collection of DataProvider or DataFrames")


class DataProvider(ObservablePropertyMixin, DRAGONDataHandler, ABC):
    """
    Interface for any StreamObject class that needs to provide data.
    """
    _observable_properties = {"name"}

    def __init__(self, *, name: str = None, **kwargs):
        super().__init__(**kwargs)
        assert name is None or (type(name) is str and len(name) > 0)
        self.name = name

    def get_params(self):
        return {}

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return "[" + (self.name + ": " if self.name is not None else "") + self.__class__.__name__ + (
            " " + repr(self.get_params())[1:-1] if len(self.get_params()) > 0 else "") + "]"

    def __len__(self):
        assert self.is_dataprovider()
        return len(self.get_data())

    def get_name(self):
        """
        Returns the name of this object.
        """
        return self.name

    @abstractmethod
    def _emit_state_changed(self, state_change: StateChangeEvent):
        """
        emits an event if the current data state (i.e. self._data) changed in any way
        """
        pass

    @abstractmethod
    def get_data(self):
        """
        Returns the data currently held by this provider.
        Must be implemented by any concrete subclass (e.g., WindowImpl).
        """
        pass

    @abstractmethod
    def reset(self):
        """
        reset the data state of the DataProvider object to a well-defined state.
        """
        pass


class WindowImpl(StreamObject, DataProvider, ABC):
    """
    Implementation class for any Window object class.
    """

    _observable_properties = StreamObject._observable_properties | DataProvider._observable_properties

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._data = []

    def __len__(self):
        return len(self._data)

    def reset(self):
        """
        resets a window removing its content fully.
        """
        self._data = []

    def is_dataprovider(self):
        return True

    def get_data(self):
        """
        Returns a copy of currently stored data in this object.
        """
        return self._data[:]

    def _emit_state_changed(self, state_change: StateChangeEvent):
        """
        emits an event if the current state (i.e. self._data) changed in any way
        :param state_change: State changed event. (data_in, data_out)
        """
        self.event_handler.emit(self._get_event_name("state_changed"), state_change)


class FunctionRegistry:
    """
    registry for any functions you like to save.
    """

    def __init__(self):
        self._register: dict[str, Callable] = {}

    def add(self, name: str, func: Callable):
        """
        add a function to the registry.
        :param name: name of the function.
        :param func: the function.
        """
        func_name = name if name else func.__name__
        self._register[func_name] = func

    def remove(self, name: str):
        """
        remove a function from registry.
        :param name: name of the function.
        """
        self._register.pop(name, None)

    def get(self, name: str) -> Optional[Callable]:
        """
        get a function from the registry by name.
        :param name: name of the function.
        :return: returns the function or none if non exist.
        """
        return self._register.get(name)


class Map(StreamObject):
    """
    Maps any given function to all data points going to the following StreamObject in the pipeline.

    Example: map = Map(lambda x: {'value': x.get('value', 0) * 10})
    """

    def __init__(self, func):
        super().__init__()
        self.func = func

    def push(self, samples):
        mapped_samples = []
        for sample in samples:
            new_features = self.func(sample.features)
            new_sample = StreamSample(
                features=new_features,
                metadata=sample.metadata.copy()
            )
            mapped_samples.append(new_sample)
        self._emit_out_flow(data_out=mapped_samples)

    def is_flow_transformer(self):
        return True


class Filter(StreamObject):
    """
    Filters any incoming data points with given function going to the following StreamObject in the pipeline.

    Example: filter = Filter(lambda x: x.get('value', 0) > 0)
    """

    def __init__(self, predicate):
        super().__init__()
        self.filter = predicate

    def push(self, samples):
        if not samples:
            self._emit_out_flow(data_out=[])
            return

        filtered_samples = []

        for sample in samples:
            if self.filter(sample.features):
                filtered_samples.append(sample)

        if filtered_samples:
            self._emit_out_flow(data_out=filtered_samples)

        self.event_handler.emit(self._get_event_name("state_changed"),
                                StateChangeEvent(data_in=samples, data_out=filtered_samples))


class Peek(StreamObject):
    """
    Prints all incoming data points to console and hands them to the following StreamObject in the pipeline.

    Parameters:
    -----------
    message: str
        Any message to be printed to the console.
    print_metadata: bool
        True if metadata should also be printed to the console.
        False if only the features should be printed to the console.
    """

    def __init__(self, message: str = None, print_metadata=False):
        super().__init__()
        self.message = message
        self.print_metadata = print_metadata

    def push(self, samples):
        output = []
        if self.message:
            output.append(self.message)
            output.append('\n')
        if self.print_metadata:
            output.append(samples)
        else:
            clean_samples = [sample.features for sample in samples]
            output.append(clean_samples)
        print(output)
        self._emit_out_flow(data_out=samples)
