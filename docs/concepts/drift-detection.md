# Drift Detection

## Motivation

Drift detection is one of the central tasks in monitoring streaming systems.

The goal is to determine whether the underlying data-generating process has changed.

## DRAGON's Perspective

DRAGON focuses primarily on distribution-based and unsupervised drift detection.

Rather than providing a fixed collection of detectors, the framework emphasizes modular construction and comparison of alternative detection strategies.

## Detection as Part of a Larger Workflow

In DRAGON, drift detection is only one stage of a broader analysis process.

Detection is often followed by:

- Localization
- Segmentation
- Explanation

For this reason detectors are designed to integrate naturally into larger processing pipelines.

## Modularity

A central design goal is rapid experimentation.

Users should be able to:

- Exchange windowing strategies
- Exchange statistical tests
- Compare detector variants

with minimal modifications to the surrounding workflow.

## Relationship to Monitoring

DRAGON views drift detection primarily as a monitoring problem.

The purpose is not only to identify change but also to support downstream analysis and understanding of the observed behavior.

## Further Reading

For a broader discussion of drift detection and monitoring see:

One or Two Things We Know About Concept Drift — Part A: Detecting Concept Drift