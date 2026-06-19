# Stream Generation

## Motivation

One of the major challenges in stream learning is balancing realism and control.

Synthetic benchmark streams are easy to control but often unrealistic. Real-world datasets are realistic but usually provide little control over the drift process itself.

DRAGON approaches stream generation as a framework in its own right and aims to bridge this gap.

## Core Idea

DRAGON distinguishes between four levels:

> Concept Library -> Stream Generator -> Stream

A concept represents a reusable data-generating pattern.

A stream library stores such concepts.

A stream generator describes how concepts are arranged over time.

A stream is a concrete realization sampled from a stream generator.

## Why Stream Generators?

A stream is not a dataset. While it consists a fixed collection of data points, it also describes a temporal component - a crucial aspect in monitoring and drift analysis.

Therefore, we have to specify not only a distribution but also a temporal order. Simple train-test-splits and k-fold approaches as commonly used in batch learning are therefore insufficient. They are replaced by stream generators which not only specify sampling behaviour but also how it changes over time.

This offers several advantages:

- Compact storage
- Reproducibility
- Controlled experimentation
- Easy generation of many benchmark variants

## Reconstruction

A distinguishing feature of DRAGON is the ability to move in both directions:

- Generate streams from concepts
- Recover concepts from existing streams

This makes it possible to create realistic but controlled benchmark scenarios from real-world data.

## Future Directions

The stream-generation framework is intended to grow beyond classical benchmark generation.

Planned extensions include:

- Gradual drift
- Recurring concepts
- Periodic drift
- Fairness-aware stream generation
- Domain-specific benchmark construction

## Related Work

The stream-generation framework builds upon:

https://github.com/FabianHinder/Creating-Drift-Benchmarks


