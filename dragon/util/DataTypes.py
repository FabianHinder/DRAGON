from dataclasses import dataclass
from typing import Any

# reduces this circular import stuff

@dataclass(frozen=True)
class StreamSample:
    features: dict[str, Any]
    metadata: dict[str, Any]


@dataclass
class StateChangeEvent:
    data_in: list
    data_out: list