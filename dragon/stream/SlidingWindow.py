from dragon.stream.StreamObject import WindowImpl
from dragon.util.DataTypes import StateChangeEvent


class SlidingWindow(WindowImpl):
    """
    Sliding Window that takes data from event until it the number of data points reaches max_size.
    After that for each new data point taken the oldest data point is pushed out of the window. (FI-FO)

    :param name: The name of the window. If leaving unchanged the name will automatically be generated.
    :param max_size: The maximum number of data points in this window.
    """
    _observable_properties = set(WindowImpl._observable_properties) | {"max_size"}

    def __init__(self, max_size, name: str = None):
        super().__init__(name=name)
        self.max_size = max_size
        self.total_samples_in = 0

    def get_params(self):
        return {"max_size": self.max_size}

    def push(self, samples):
        if not self.open_state:
            self._emit_out_flow(data_out=samples)
            return
        if not samples:
            return

        overflow = len(self._data) + len(samples) - self.max_size
        out_flow = []
        if overflow > 0:
            if overflow >= len(self._data):
                out_flow = self._data + samples[:overflow - len(self._data)]

                self._data = samples[-self.max_size:]
            else:
                out_flow = self._data[:overflow]
                del self._data[:overflow]
                self._data.extend(samples)

        else:
            self._data.extend(samples)

        self._emit_out_flow(data_out=out_flow)
        self._emit_state_changed(StateChangeEvent(data_in=samples, data_out=out_flow))
