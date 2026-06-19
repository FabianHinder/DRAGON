# Design Philosophy

## The Main Idea

Most effort in stream-analysis projects is spent on infrastructure rather than analysis.

Researchers and practitioners repeatedly implement:

- Memory management
- Pipeline orchestration
- Event handling
- Benchmark generation
- Evaluation infrastructure

These tasks are necessary but rarely represent the actual problem being investigated.

DRAGON attempts to move attention away from infrastructure and toward experimentation and analysis.

## Core Principles

### Modularity

Components should be reusable and independently replaceable.

### Experimentation

Alternative designs should be easy to compare.

### Integration

Stream generation, monitoring, detection, and explanation should work together seamlessly.

### Readability

Code should be easy to understand, modify, and extend.

## What DRAGON Does Not Optimize For

DRAGON is not primarily designed for:

- Maximum execution speed
- Minimal memory consumption
- Implementing every published stream-learning algorithm

## Why?

The goal is to help users answer questions such as:

- Which detector works best?
- Which memory architecture works best?
- Which explanation method is most useful?

rather than spending time implementing infrastructure around those questions.

