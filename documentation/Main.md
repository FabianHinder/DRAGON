# DRAGON 

## What is DRAGON?

DRAGON stands for
**DR**ift **A**nalysis using **G**eneric explanati**ON** techniques
and is a toolkit for prototyping of different drift detection and analysis tools.

With this toolkit we provide a variety of tools to implement fast, easy and modular drift examination, and explanation methods.

## Goals and Design Philosophy

The processing of continuous data streams differs fundamentally from static machine learning methods. Therefore, we offer several tools that allows for fast, and reliable prototpying, moving the focus away from software engeneering and memory management to data analysis and algorithm design. Our main goals are

* High Modularity and Reusability: Tools are completely decoupled. No tool depends on the internal implementation of another.
* Dynamic Pipelines: Every pipeline can be built and modified on the fly, much like assembling a Lego set.
* Practice similarity: Instead of relying on fixed, blocking while-loop inspired code, we make use of an event-based desing. This allows for more dynamically adaptable code and is more similar to how reactive systems 

Due to these factors, DRAGON offers ways for fast and easy implementation a large variety of stream learning and online analysis algorithms. Here, modularity is at the core: from changing the statistical test in a drift detector to changing from a simple sliding window to an advanced memory management to take care of special needs all in a few lines of code. This also alows the comparision of various approaches to find exactly the right one for your special problem.

**Non-Goals**

DRAGON's goals are not

* to implement *all* state-of-the-art stream learning algorithms
* to be optimized for maximal speed and minimal memory footprint
* MORE

## How to use DRAGON?

The use of DRAGON follows an easy and standardized lifecycle:

1. Stream generation
2. Windowing & Transformation
3. Detection
4. Explanation

For each of those steps, DRAGON provides solutions. 
Streams can easily be loaded and important form various libraries and formats. 
Organizing and controlling memory and data flow is one of the central aspects DRAGON focuses on allowing the creation of various memory architectures [(see Windows and Pipes)](../documentation/docs/concepts/windows-and-pipes.md). 
Furthermore, DRAGON implements a collection of distribution based drift and change point detection methods allowing for fast, easy, and reliable detection of concept drift. 
DRAGON is at core designed to support the implementation of the model-based drift explanation design pattern. This includes, not only detection, but also localization, segmentation, and explanation of concept drift. 

## Core Concepts and Components

DRAGON is centered around a handful of components that follow simple design concepts. The central idea of DRAGON that is crucial for admitting high modularity is the strict and systematic usage of event-based data handling. 

### Windows and Pipes

Where the batch setup uses dataset, stream learning uses various types of memory management that usually amount to various kinds of dynamic updates. We refer to such dynamic memory managment systems as 'windows' in resamblance of the popular sliding windows used in many stream learning algorithms.  DRAGON implemnts various kinds of windows for memory managment. Crucially, DRAGON offers easy and fast way to combine several of such components allowing the creation of complex memory management systems with a few lines of code. Furthermore, we provide tools to control the flow of data through the desing processing pieline. 

### Drift and Changepoint Detector

Drift and change point detection are central tasks in system monitoring and stream learning. DRAGON fucses primarily on the implementation of distribution based drift detectors. However, due to the various tools, implementing most classical drift detectors can be done in a few lines of reusable code [see example].

### Model-Based Drift Explanations

DRAGON is designed to implement the model-based drift explanation design pattern. Model-based drift explanations are a general and widely applicable type of data analysis tools that allow machine supported understanding of the ongoing drift by implying XAI tools. In particular, we provide implementations for several explanation approaches --- ranging form simple feature attributions to counterfactual explanations. 
Furthermore, DRAGON implements related, drift analysis tools like drift localization and segmentation tools. 

### Stream Management -- Creating and Controlling

DRAGON not only provides tools for designing stream analysis tools, but also to create streams for systematic testing. First and foremost, DRAGON supports various file formats and wrappers for other libraries like river. Aside that, DRAGON implements various standard stream generators. Furthermore, DRAGON implements tools for controlling the drift in real-world datastreams allowing for the creation of realistic and controlled drift. This allows the systematic and problem specific analysis of varous tools and better, systematic testing and development.

### Events 

In contrast to other toolboxes that use blocking loops, DRAGON is based on even handling. This allows for a unified system for the communication of data and the flow of control that is both simple to use, versatile, and heavily adjustable. Here, events serve as both: to control the flow of data, and to control the flow of action. This allows the realization of complex interlocking systems in a few lines of code.

<!-- ![Event Flow Diagram](diagrams/event_flow.png)

As seen in this image every new data point emits an event and every listener gets immediate access.
It does not matter if the data is preprocessed through a pipeline or listened directly as long as any listener
subscribes to an event it has access to provided data.
Also any listener may emit its own events and other listeners can subscribe to its events and so on.-->



[Learn More](../documentation/docs/concepts/events.md)
