# Drift Explanations

## Motivation

Detecting drift tells us that something changed.

For many applications, this is only the beginning. In practice, users often need to understand:

- what changed,
- where the change occurred,
- which features are involved,
- and whether the observed change requires intervention.

DRAGON therefore treats drift explanation as a central component of the analysis workflow rather than an optional post-processing step.

## Model-Based Drift Explanations

DRAGON follows the model-based drift explanation paradigm.

The central idea is to transform drift analysis into a model explanation problem. Once a suitable model has been trained to capture the differences between distributions, classical explainable AI techniques can be used to understand the drift itself.

This approach makes it possible to reuse a broad range of established XAI methods rather than developing entirely separate explanation techniques for every drift scenario. It also provides a unified framework that can be applied across different detectors and application domains.

## Beyond Detection

Drift explanation is closely connected to two additional tasks:

### Drift Localization

Localization focuses on identifying where drift manifests itself.

Depending on the application, this may refer to individual observations, subsets of the data that are particularly affected by the change, or regions of the feature space.

### Drift Segmentation

Segmentation aims to partition the observed drift into meaningful components.

Rather than treating drift as a single event, segmentation allows users to distinguish different change mechanisms and analyze them individually.

Together, detection, localization, segmentation, and explanation form a natural analysis pipeline that moves from identifying a change toward understanding it. 

## Examples of Drift Explanations

### Feature-Based Drift Analysis

An important direction is feature-based drift analysis.

Instead of asking only whether the whole distribution changed, this view asks which features are most relevant for detecting, monitoring, or interpreting the drift.

This is useful for semantic interpretation, because it can connect drift behavior to specific observable attributes rather than only to an abstract test statistic.

### Counterfactual Explanations

One particularly useful family of explanations are counterfactual explanations.

Instead of only describing where drift occurs, counterfactual explanations characterize the changes required for observations to move between pre-drift and post-drift behavior. This provides intuitive and human-readable descriptions of the observed change and often highlights the most relevant features involved in the drift. 

### Causal Explanations

Recent work extends model-based explanations toward causal explanations.

While traditional explanations identify features that are associated with the drift, causal explanations aim to identify features and mechanisms that are responsible for the observed change. This moves drift analysis closer to actionable monitoring by helping users understand not only what changed, but also which aspects of the system may need intervention. 

DRAGON's long-term vision aligns with this progression from detection to understanding and ultimately toward actionable explanations.

## Why This Matters

Many monitoring systems stop after detecting drift.

DRAGON aims to support the next steps as well:

- detecting drift,
- locating affected data,
- segmenting complex changes,
- explaining observed behavior,
- and ultimately supporting informed adaptation decisions.

This broader perspective is one of the main reasons DRAGON is designed as an analysis framework rather than only a collection of drift detectors.

## Further Reading

- Model-Based Explanations of Concept Drift
- One or Two Things We Know About Concept Drift — Part B: Locating and Explaining Concept Drift
- Causal Explanation of Concept Drift — A Truly Actionable Approach