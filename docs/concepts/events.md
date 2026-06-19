# Event System

## Motivation

Streaming systems are inherently dynamic.

Data arrives continuously, components react to it, and actions often trigger additional actions.

DRAGON therefore uses an event-driven architecture.

## Core Idea

Events serve two purposes:

- Transporting data
- Triggering actions

This creates a unified mechanism for both data flow and control flow.

## Event-Based Processing

Rather than relying on blocking processing loops, DRAGON uses listeners that react to events.

When data arrives, interested components are notified automatically.

The same mechanism can be used to trigger:

- Drift detection
- Resets
- Explanations
- Monitoring actions

## Advantages

The event-based design provides:

- Loose coupling
- High modularity
- Dynamic reconfiguration
- Clear separation of responsibilities

## Streams as Event Sources

Streams are active components.

Rather than pulling samples from a stream, DRAGON streams emit events that can be observed by other components.

This allows complete workflows to be assembled through subscriptions between components.

## Why This Matters

The event system is one of the main reasons DRAGON workflows remain highly modular even when they become large and complex.