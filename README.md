# DRAGON

**DR**ift **A**nalysis using **G**eneric explanati**ON** techniques

DRAGON is a framework for designing, testing, and evaluating stream-analysis workflows.

Unlike stream-learning frameworks such as River or MOA, DRAGON does not primarily focus on predictive models. Instead, it focuses on the surrounding experimentation workflow: stream generation, memory management, monitoring, drift detection, and drift explanation.

The goal is simple:

> Spend less time implementing infrastructure and more time understanding your data.

---

## Why DRAGON?

Designing stream-analysis systems often requires repeatedly changing:

* memory architectures
* windowing strategies
* drift detectors
* adaptation policies
* monitoring components
* explanation methods

In many implementations, changing one of these requires rewriting large parts of the surrounding code.

DRAGON separates analysis logic from data management through reusable and event-driven components. Pipelines can be assembled from interchangeable building blocks, making it possible to rapidly explore and compare alternative designs.

This makes it possible to rapidly prototype many design variants and identify the most suitable solution for a given problem.

DRAGON approaches this problem through highly modular, event-driven components that can be combined and recombined with minimal effort.

---

## A Different Way of Thinking

Instead of asking:

> How do I implement a sliding window?

DRAGON encourages users to ask:

> What should I do with the data inside the window?

Instead of focusing on infrastructure, DRAGON focuses on experimentation.

Memory management, data flow, stream generation, detection, and explanation become reusable building blocks that can be combined as needed.

---

<!--

## Example

```python
pipe = DRAGON.parse(
    "[[ref:[static size:100] <- [slide size:100]] <- "
    "[cur:slide size:200] "
    "reset: ref_2(ref_1,cur),ref_2]"
)

dd = MMD(pipe["ref"], pipe["cur"])

pipe["ref"].add_stage_changed(dd.detect)
dd.add_detection(pipe.reset)

stream_distribution = SEAStreamLibrary().create_stream_generator([
    {"concept": 1, "length": 500},
    {"concept": 2, "length": 100},
    {"concept": 1, "length": 100}
])

stream = stream_distribution.generate(random_seed=100)

stream.add_new_sample_listener(pipe)
stream.run()
```

This example combines:

* concept-based stream generation
* dynamic memory architectures
* drift detection
* event-driven execution

within a single workflow.

-->


## Core Components

### Stream Generation

DRAGON treats stream generation as a first-class component.

Streams can be generated from reusable concepts, reconstructed from existing datasets, and used to create realistic and controlled drift benchmarks.

### Windows and Pipes

Build memory-management architectures from reusable components.

Simple sliding windows, complex multi-window detectors, adaptive reference structures, and custom reset strategies can all be expressed using the same abstractions.

### Event-Based Processing

DRAGON uses an event-driven architecture that unifies data flow and control flow.

This allows complex interactions between streams, detectors, explanations, and adaptation mechanisms without tightly coupling components.

### Drift Detection

DRAGON focuses primarily on distribution-based and unsupervised drift detection for monitoring applications.

Detectors are designed to integrate naturally into larger workflows rather than being isolated components.

### Drift Analysis and Explanation

DRAGON supports the model-based drift explanation paradigm, allowing explainable AI techniques to be used for understanding detected drift.

Detection is only the first step. Understanding the observed change is equally important.

---

## Learning DRAGON

DRAGON is accompanied by interactive tutorial notebooks.

Rather than requiring users to read large amounts of documentation before writing code, the tutorials provide:

* hands-on exercises
* guided examples
* immediate feedback
* incremental introduction of concepts

See the tutorials for the recommended learning path.

---

## Documentation

### First Steps

* [Getting Started](docs/getting-started.md)
* [Tutorials](docs/tutorials.md)

### Core Concepts

* [Stream Generation](docs/concepts/stream-generation.md)
* [Windows and Pipes](docs/concepts/windows-and-pipes.md)
* [Event System](docs/concepts/events.md)
* [Drift Detection](docs/concepts/drift-detection.md)
* [Drift Explanations](docs/concepts/drift-explanations.md)

### Background

* [Design Philosophy](docs/philosophy.md)
* [Publications](docs/publications.md)

---

## Current Status

DRAGON is currently in an early development stage.

The API is still evolving and some components are experimental. However, the core ideas, architecture, and workflows are already available and actively used for research and prototyping.

---

## Contributing

Contributions, feedback, bug reports, and discussions are welcome.

