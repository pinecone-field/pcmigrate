# ADR-013: Structured events, terminal view first

**Status:** Proposed

## Context

Stage progress needs to reach the terminal today and possibly CloudWatch dashboards or a web UI later. If stages print directly, every new consumer means touching stage code.

## Decision

Stages emit typed JSON events (`stage_started`, `partition_done`, `import_progress`, `validation_result`, and so on). Events go to stdout logs, to `manifest/events/`, and to a `rich` live view.

## Consequences

- The terminal UI ships in v1. CloudWatch dashboards or a web UI can subscribe later without touching stage code.
- No stage prints directly; everything goes through the event bus.
