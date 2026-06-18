from datetime import datetime

import pandas as pd


## TODO: wir sollten auf einen worker demon umsteigen, ist wahrscheinlich besser (but discuss)

class EventHandler:
    """
    This class is responsible for all events.
    Every other object that this framework uses somehow uses this EventHandler emitting or listening to events.

    You do not need to handle any of this, if using the base classes provided.
    """

    def __init__(self):
        self.listeners = dict()

    def get_listeners(self):
        return [listener for listeners in self.listeners.values() for listener in listeners]
    
    def add_listener(self, event_name, listener):
        if event_name not in self.listeners:
            self.listeners[event_name] = []
        if listener not in self.listeners[event_name]:
            self.listeners[event_name].append(listener)

    def remove_listener(self, event_name, listener):
        if event_name in self.listeners:
            self.listeners[event_name].remove(listener)
            if len(self.listeners[event_name]) == 0:
                del self.listeners[event_name]

    def emit(self, event_name, *args, **kwargs):
        if event_name not in self.listeners:
            return
        # copy if any listener removes itself while emitting
        for listener in self.listeners[event_name][:]:
            try:
                listener(*args, **kwargs)
            except Exception as e:
                print(f"Error in Listener {event_name}: {e}")


class EventLogger:
    """
    This class is responsible for logging events.
    Its main purpose is more or less validating and checking property change events.
    However, you can use it to watch any event.

    If you wish to observe properties of other classes than StreamObjects and its child classes this needs preparation in __setattr__ call.
    see StreamObject-Class for implementation.

    Observed events are stored in a dictionary - accessible via get_logs method.

    """

    def __init__(self):
        self.history = []
        self._active_watches = set()

    def watch(self, event_object, event_name="property_changed"):
        """
        This function is used to observe events of any kind and logs these to a dictionary.

        :param event_object: Object that will be observed.
        :param event_name: Name of the event that the logger will observe.
        """
        if hasattr(event_object, "event_handler"):
            watch_id = (id(event_object), event_name)
            if watch_id in self._active_watches:
                return
            def callback_wrapper(**kwargs):
                self._record_event(event_type=event_name, **kwargs)

            self._active_watches.add(watch_id)
            event_object.event_handler.add_listener(event_name, callback_wrapper)
        else:
            print(f"Object {getattr(event_object, "name", "unknown")} has no event_handler.")

    def _record_event(self, event_type, **kwargs):
        record = {
            "timestamp": datetime.now(),
            "event_type": event_type
        }
        record.update(kwargs)
        self.history.append(record)

    def get_logs(self, clear=False):
        """
        Outputs a dataframe with stored events.
        :param clear: if set True all stored events are cleared after.
        :return: pandas dataframe with stored events.
        """
        if not self.history:
            df = pd.DataFrame()
        else:
            df = pd.DataFrame(self.history)
        if clear:
            self.history = []
        return df

    def clear(self):
        """
        Clears all stored events.
        """
        self.history = []
