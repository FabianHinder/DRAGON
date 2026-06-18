from dragon.stream.StreamObject import WindowImpl
from dragon.util.DataTypes import StateChangeEvent


class StaticWindow(WindowImpl):
    """
    Static Window that takes data from event until it the number of data points reaches max_size.
    After that any new data point is rejected.

    :param name: The name of the window. If leaving unchanged the name will automatically be generated.
    :param max_size: The maximum number of data points in this window.
    """

    _observable_properties = set(WindowImpl._observable_properties) | {"max_size"}

    def __init__(self, max_size, name: str = None):
        super().__init__(name=name)
        self.max_size = max_size

    def get_params(self):
        return {"max_size": self.max_size}

    def get_data(self):
        """
        returns a copy of the window data.
        """
        return self._data[:]

    def push(self, samples):
        if not self.open_state:
            self._emit_out_flow(data_out=samples)
            return
        if not samples:
            return

        data_in = []
        available_space = self.max_size - len(self._data)

        if available_space > 0:
            data_in = samples[:available_space]
            self._data.extend(data_in)
            out_flow = samples[available_space:]
        else:
            out_flow = samples
        if out_flow:
            self._emit_out_flow(data_out=out_flow)

        if data_in:
            self._emit_state_changed(StateChangeEvent(data_in=data_in, data_out=out_flow))
