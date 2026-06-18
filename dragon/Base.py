"""
collection of base methods needed overall in dragon
"""
import abc

from dragon.util.EventHandler import EventHandler


class Base(abc.ABC):
    _supported_events = set()

    def __init__(self):
        if not hasattr(self, 'event_handler'):
            self.event_handler = EventHandler()

    def add_listener(self, event_name: str, callback):
        """
        adds listener to any object supporting event handling
        """
        if event_name not in self.get_supported_events():
            raise ValueError(f"The event '{event_name}' is not available for {self.__class__.__name__}. "
                             f"Supported events are: {self.get_supported_events()}")
        self._bind_listener(event_name, callback)

    def remove_listener(self, event_name: str, callback):
        """
        removes listener from any object supporting event handling
        """
        if event_name not in self.get_supported_events():
            raise ValueError(f"The event '{event_name}' is not available for {self.__class__.__name__}.")

        self._unbind_listener(event_name, callback)

    @classmethod
    def get_supported_events(cls):
        """
        collects all supported events.
        :return:
        """
        events = set()
        for base in cls.__mro__:
            if hasattr(base, '_supported_events'):
                events.update(base._supported_events)
        return events

    def _bind_listener(self, event_name: str, callback):
        """
        default binding method for adding listeners.
        Overwrite this method in subclasses if needed.
        """
        self.event_handler.add_listener(event_name, callback)

    def _unbind_listener(self, event_name: str, callback):
        """
        default method for removing listeners.
        Overwrite this method in subclasses if needed.
        """
        self.event_handler.remove_listener(event_name, callback)
