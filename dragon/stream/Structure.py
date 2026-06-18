from abc import ABC

from dragon.stream.StreamObject import StreamObject, DataProvider
from dragon.stream.SlidingWindow import SlidingWindow
from dragon.util.DataTypes import StateChangeEvent

from typing import Any
import re

"""
Base class for fork and pipeline
"""


class StreamObjectStructure(StreamObject, ABC):
    _observable_properties = {"stages"}

    def __init__(self, *, indexer, **kwargs):
        super().__init__(**kwargs)
        self.stages = []
        self.indexer = indexer

        self._name_register = None

    def is_dataprovider(self):
        return False

    def is_flow_transformer(self):
        return any(map(lambda state: state.is_flow_transformer(), self.stages))

    def is_linear_flow(self):
        return False

    def _add_stage(self, stage):
        """
        Should be handled in constructor to build a valid structure object.
        """
        raise NotImplementedError()

    """
        gets any sub-object object by its name if present.
    """

    def __getitem__(self, key):
        return self.get_stage_by_name(key)

    def get_stage_by_name(self, name):
        name_register = self._get_name_register()
        if name not in name_register:
            raise ValueError("Unknown name: " + name)
        return name_register[name]

    def _get_name_register(self):
        if not self._name_register:
            self.get_root_node()._create_name_dict()
        return self._name_register

    def structure_changed(self):
        """
        emits an event if the current structure has changed.
        The structure is changed if either: 1) the name of an element is changed or 2) the structure (stages) of a pipe or fork are structure
        """
        self._name_register = None
        self.event_handler.emit(self._get_event_name("structure_changed"))

    def _add_structure_changed_listener(self, callback):
        if callback == getattr(self, "structure_changed", None):
            return
        self.event_handler.add_listener(self._get_event_name("structure_changed"), callback)

    def _remove_structure_changed_listener(self, callback):
        self.event_handler.remove_listener(self._get_event_name("structure_changed"), callback)

    def _create_name_dict(self):
        name_register = {}
        prefix_free = []

        def add_if_not_present(name_register, name, stage, ignore_none=False):
            if name is None:
                if not ignore_none:
                    raise ValueError("Name cannot be none")
            elif type(name) != str or len(name) == 0:
                raise ValueError(f"Invalid name {repr(name)}")
            else:
                if name in name_register.keys():
                    raise ValueError(f"Ambiguous name {name}")
                name_register[name] = stage

        stage_id = 1
        for stage in self.stages:
            logged_some = False
            if isinstance(stage, DataProvider):
                add_if_not_present(name_register, stage.get_name(), stage, ignore_none=True)
                prefix_free.append(([self.indexer(stage_id)], stage))
                logged_some = True
            if isinstance(stage, StreamObjectStructure):
                sub_name_register, sub_prefix_free = stage._create_name_dict()
                for name, sub_stage in sub_name_register.items():
                    add_if_not_present(name_register, name, sub_stage)
                for name, sub_stage in sub_prefix_free:
                    name.insert(0, self.indexer(stage_id))
                    prefix_free.append((name, sub_stage))
                logged_some = True
            if logged_some:
                stage_id += 1

        name = self.get_name() if hasattr(self, "get_name") else None
        if name:
            for prefix, stage in prefix_free:
                add_if_not_present(name_register, name + "_" + "-".join(prefix), stage)

        self__name_register = dict(name_register)
        for prefix, stage in prefix_free:
            add_if_not_present(self__name_register, "W_" + "-".join(prefix), stage)
        add_if_not_present(self__name_register, name, self, ignore_none=True)

        self._name_register = self__name_register

        return name_register, prefix_free


def numeric_index(n):
    return f"{int(n)}"


class Pipeline(StreamObjectStructure, DataProvider):
    """
    The central connector that chains multiple stream operations and windows together.
    Stream operations for example are Windows, Function(i.e. Map, Filter, Peek) or Pipelines.

    It automatically links the "out_flow" event of one stage to the input of the next stage.

    The Pipeline should be listening to any generated stream to work properly.

    :param stages: One or many StreamObjects that are to be chained.
    For each element in the pipeline its respective out_flow is chained as input event for the next stage.

    :param name: The name of this pipeline.

    Example:

    >>> processing_pipe = Pipeline(
    >>>     SlidingWindow(max_size=500),
    >>>     Map(lambda x: {**x, 'value': x.get('value', 0) * 10}),
    >>>     Filter(lambda x: x.get('value', 0) >= 0)
    >>>     )

    """

    _observable_properties = set(DataProvider._observable_properties) | set(
        StreamObjectStructure._observable_properties)

    def __init__(self, *stages, name: str = None, reset_code: str = None):
        super().__init__(name=name, indexer=numeric_index)

        self.reset_instruction_stack = None
        self.reset_instruction_set = None

        self._name_register = None

        self._event_collector = []

        stages = list(stages)
        for stage in reversed(stages):
            # data in stream learning canonically comes from the "right side", so a simple reverse is suitable here
            if not isinstance(stage, StreamObject):
                raise TypeError(f"Error: Argument is not a StreamObject. Got {type(stage)} instead.")
            self._add_stage(stage)

        self.set_reset_code(reset_code)

    def __repr__(self):
        inner = (self.name + ": " if self.name is not None else "") + "<-".join(map(repr, reversed(self.stages)))
        return "[" + inner + "]" if self.is_dataprovider() or self.name is not None else "[" + inner + "]"

    def __len__(self):
        if not self.is_dataprovider():
            raise ValueError("This pipe either contains a fork or a map. Therefore, it has no valid content state")
        return sum(map(len, filter(lambda stage: isinstance(stage, DataProvider), self.stages)))

    def get_data(self) -> list:
        """
        Returns the content of all contained data providers if in a valid state
        """
        if not self.is_dataprovider():
            raise ValueError("This pipe either contains a fork or a map. Therefore, it has no valid content state")

        return [data for stage in self.stages for data in (stage.get_data() if isinstance(stage, DataProvider) else [])]

    def is_dataprovider(self):
        return all(map(lambda state: not state.is_flow_transformer() or state.is_linear_flow(), self.stages))

    def is_linear_flow(self):
        return all(map(lambda state: state.is_linear_flow(), self.stages))

    def _add_stage(self, stage):
        """
        Manually add a stage to the pipeline if not already present.

        If you did not add stage in the constructor you can call this method to add a new stage.

        :param stage: Any instance of StreamObject to be added.
        """

        if self.stages:
            prev = self.stages[-1]
            prev.remove_listener("out_flow", self._emit_pipe_out_flow)

            # Successive stages connect to the outflow of the previous stage
            prev.add_listener("out_flow", stage.push)

        # + for __setattr__ call to get an event out here
        self.stages = self.stages + [stage]
        stage.set_parent(self)
        stage.add_listener("out_flow", self._emit_pipe_out_flow)
        stage.add_listener("state_changed", self._collect_child_events)

        self.structure_changed()

    def set_parent(self, parent: StreamObjectStructure):
        old_parent = self._parent
        if hasattr(super(), 'set_parent'):
            super().set_parent(parent)
        else:
            self._parent = parent
        if self._parent.structure_changed != getattr(self, "structure_changed", None):
            self.add_listener("structure_changed", self._parent.structure_changed)

        self.structure_changed()

        if isinstance(old_parent, StreamObjectStructure):
            self.remove_listener("structure_changed", old_parent.structure_changed)

    def _emit_pipe_out_flow(self, samples: list[Any]):
        self.event_handler.emit(self._get_event_name("out_flow"), samples=samples)

    def push(self, samples):
        if len(self._event_collector) > 0:
            raise Exception("Your pipeline is not in a well defined state. Check your events!")
        if self.stages:
            try:
                self.stages[0].push(samples)

                if self._event_collector:
                    delta_in, delta_out = self._calc_pipe_delta()
                    self._emit_state_changed(StateChangeEvent(data_in=delta_in, data_out=delta_out))
            finally:
                self._event_collector.clear()

    def _collect_child_events(self, state_change: StateChangeEvent, **kwargs):
        if state_change:
            self._event_collector.append(state_change)

    def _emit_state_changed(self, state_change: StateChangeEvent):
        self.event_handler.emit(self._get_event_name("state_changed"), state_change)

    def _calc_pipe_delta(self):
        """
        calculates delta for incoming and outgoing data events in a pipe.
        All state change event data is aggregated and data out only outputs the points which are not in data in at the same time
        """
        all_data_in = []
        all_data_out = []

        for event in self._event_collector:
            if event:
                all_data_in.extend(event.data_in)
                all_data_out.extend(event.data_out)

        delta_in = list(all_data_in)
        delta_out = []

        for item in all_data_out:
            if item in delta_in:
                delta_in.remove(item)
            else:
                delta_out.append(item)
        return delta_in, delta_out

    def set_reset_code(self, reset_code=None):
        assert reset_code is None or type(reset_code) is str
        if reset_code is None or len(reset_code) == 0:
            self.reset_instruction_stack = None
            return

        reg_lang = '([A-Za-z0-9_-]+)|("([^"]|\\")*")|([,()])|[ \n\t\r]+'
        assert re.match(f"({reg_lang})*", reset_code)
        reset_code = list(filter(lambda token: len(token) > 0,
                                 map(lambda token: token[1:-1] if token[0] == '"' else token,
                                     filter(lambda token: token is not None and len(token) > 0,
                                            re.split(reg_lang, reset_code)))))

        instruction_stack, close_stack = [], []
        LR_stack = []
        for pos, token in enumerate(reset_code):
            if token == "(":
                if len(LR_stack) > 0:
                    elm = LR_stack.pop()
                    instruction_stack.append((elm, "close"))
                    close_stack.append(elm)
                else:
                    raise ValueError(
                        f"Expected name token before close: '{"".join(reset_code[:pos])} *{token}* {"".join(reset_code[pos + 1:])}'")
            elif token == ",":
                if len(LR_stack) > 0:
                    instruction_stack.append((LR_stack.pop(), "reset"))
                else:
                    raise ValueError(
                        f"Expected name token before reset: '{"".join(reset_code[:pos])} *{token}* {"".join(reset_code[pos + 1:])}'")
            elif token == ")":
                if len(LR_stack) > 0:
                    instruction_stack.append((LR_stack.pop(), "reset"))
                if len(close_stack) > 0:
                    instruction_stack.append((close_stack.pop(), "open"))
                else:
                    raise ValueError(
                        f"Expected close before open: '{"".join(reset_code[:pos])} *{token}* {"".join(reset_code[pos + 1:])}'")
            else:
                LR_stack.append(token)
        if len(LR_stack) == 1:
            instruction_stack.append((LR_stack.pop(), "reset"))
        elif len(LR_stack) > 1:
            raise ValueError("File ended while parsing")
        assert len(LR_stack) == 0 and len(close_stack) == 0
        self.reset_instruction_stack = [(self[name], instr) for name, instr in instruction_stack]

    def get_reset_code(self):
        inv_name_registry = dict()
        name_registry = list(self._get_name_register().items())
        name_registry.sort(
            key=lambda name_stage: (name_stage[0][:2] == "W_", len(name_stage[0].split("-")), len(name_stage[0]),
                                    name_stage[0]))
        for name, stage in name_registry:
            stage_address = id(stage)
            if stage_address not in inv_name_registry.keys():
                inv_name_registry[stage_address] = name

        reset_instruction_stack = self.reset_instruction_stack if self.reset_instruction_stack else [(stage, "close")
                                                                                                     for stage in
                                                                                                     self.stages]
        reset_code = "".join(
            [inv_name_registry[id(stage)] + {"close": "(", "reset": ","}[instr] if instr != "open" else ")" for
             stage, instr in reset_instruction_stack]).replace(",)", ")")
        if reset_code[-1] == ",":
            reset_code = reset_code[:-1]
        return reset_code

    def get_reset_instructions(self):
        return self.reset_instruction_stack

    def reset(self):
        instructions = self.get_reset_instructions()
        if not instructions:
            for stage in self.stages:
                stage.reset()
        else:
            for stage, instr in instructions:
                if instr == "open":
                    stage.open()
                elif instr == "close":
                    stage.close()
                elif instr == "reset":
                    stage.reset()
                else:
                    raise ValueError(
                        "It seems someone played with the reset instructions! :D \n\n The window state is now corrupted... well shit")


def ABC_indexer(n):
    if n < 1:
        raise ValueError("Input must be >= 1")
    n -= 1

    result = []

    while n >= 0:
        result.append(chr(ord('A') + (n % 26)))
        n = n // 26 - 1

    return ''.join(reversed(result))


class Fork(StreamObjectStructure):
    def __init__(self, *forks):
        super().__init__(indexer=ABC_indexer)

        for fork in forks:
            self._add_stage(fork)

    def __repr__(self):
        return "|" + ",".join(map(repr, self.stages)) + "}"

    def is_linear_flow(self):
        return False

    def push(self, samples):
        for stage in self.stages:
            stage.push(samples)

    def _add_stage(self, fork):
        fork.set_parent(self)
        self.stages.append(fork)
