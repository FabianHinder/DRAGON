from dragon.stream.StreamObject import StreamObject


class WindowChunker(StreamObject):
    """
    Window chunker that takes data points and outputs these in chunks with specified size.

    :param chunk_size: Size of chunks.

    """
    _observable_properties = set(StreamObject._observable_properties) | {"chunk_size"}

    def __init__(self, chunk_size):
        super().__init__()
        self.chunk_size = chunk_size
        self.buffer = []

    def __repr__(self):
        name = getattr(self, 'name', None)
        prefix = f"{name}: " if name else ""
        return f"[{prefix}chunker {self.chunk_size}]"

    def push(self, samples):
        self.buffer += samples

        while len(self.buffer) >= self.chunk_size:
            out_flow = self.buffer[:self.chunk_size]
            self.buffer = self.buffer[self.chunk_size:]
            self._emit_out_flow(
                data_out=out_flow)
