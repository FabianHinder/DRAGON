import numpy as np
from dragon.stream.StreamObject import WindowImpl
from dragon.util.DataTypes import StateChangeEvent


class GrowingWindow(WindowImpl):
    """
    Growing Window that takes data from event until it the number of data points reaches max_size.
    After that for any new dataa chance is calculated based on the number of new incoming data points.

    The value for controlling if a new data point is accepted is incremented by the number of incoming samples.
    For each new data point a number between this value and max_size is determined.
    If this value is smaller than max_size, the new data point is accepted by
    substituting the value on an old value at this point.

    Otherwise, the new data point is rejected. A rejected data point will not emit any out_flow events.

    If (soft-)resetting all data points will be pushed out by new incoming data points and the control-value is reset to 0.

    :param name: The name of the window. If leaving unchanged the name will automatically be generated.
    :param max_size: The maximum number of data points in this window.
    """

    _observable_properties = set(WindowImpl._observable_properties) | {"max_size"}

    def __init__(self, max_size, name: str = None):
        super().__init__(name=name)
        self.n = 0
        self.max_size = max_size

    def get_params(self):
        return {"max_size": self.max_size}

    def push(self, samples, is_reset=False):
        if not self.open_state:
            self._emit_out_flow(data_out=samples)
            return
        data_in = []
        out_flow = []

        cut = self.max_size - len(self._data)
        if cut > 0:
            input_samples = samples[:cut]
            self._data.extend(input_samples)
            data_in.extend(input_samples)
            samples = samples[cut:]

        self.n = max(self.n, len(self._data))

        for i, e in enumerate(samples, start=self.n):
            k = np.random.randint(0, i + 1)
            if k < self.max_size:
                out_flow.append(self._data[k])
                self._data[k] = e
                data_in.append(e)
            else:
                out_flow.append(e)

        self.n += len(samples)

        self._emit_out_flow(
            data_out=out_flow)
        if data_in or out_flow:
            self._emit_state_changed(StateChangeEvent(data_in=data_in, data_out=out_flow))

    def reset(self):
        super().reset()
        self.n = 0
