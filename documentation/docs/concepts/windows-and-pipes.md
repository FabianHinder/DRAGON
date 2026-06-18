# Windows and Pipes

## Motivation

In stream learning, not only the data changes over time, but also the state of the processing system itself.

Memory management therefore becomes a central design decision.

DRAGON provides reusable abstractions that allow users to focus on analysis rather than implementation details.

## Stream Objects

Data processing in DRAGON is built around StreamObjects.

A StreamObject receives data, performs some operation, and may emit data for further processing.

Examples include:

- Data transformations
- Filters
- Chunking components
- Memory structures

## Windows

Windows are DRAGON's abstraction for dynamic memory management.

Different window types represent different strategies for storing and updating observations over time.

Examples include:

- Sliding windows
- Static windows
- Growing windows

The exact storage strategy can be exchanged without affecting the rest of the workflow.

## Pipes

Pipes connect StreamObjects into processing structures.

Data flows through the pipeline and is processed by each component in turn.

Pipes can themselves contain other pipes, allowing hierarchical workflow construction.

## Forks

Forks allow data to be processed by multiple branches simultaneously.

This makes it possible to execute multiple analysis strategies in parallel while observing the same stream.

## Naming

Windows and pipelines can be named.

Named components can be accessed directly inside larger structures.

This allows users to build complex systems while maintaining readability and transparency.

## Reset Behavior

One of the key ideas in DRAGON is that reset behavior should be configurable.

Different workflows require different adaptation strategies after drift detection.

DRAGON therefore treats reset behavior as a first-class concept rather than a fixed implementation detail.

## Window Annotation Language

As processing structures become larger, manually constructing them can become difficult.

The Window Annotation Language (WAL) provides a compact notation for describing windows, pipes, forks, and reset behavior.

WAL allows complex memory architectures to be represented as concise, readable specifications.

## Why This Matters

The goal is not to provide a single optimal memory architecture.

The goal is to make it easy to compare many alternatives and identify the design that works best for a given problem.