# ADR-005: Introspection-first, with a hash-locked FDE approval gate

**Status:** Proposed

## Context

The source-to-target mapping is where silent data loss happens: dropped fields, wrong metrics, mis-typed metadata. A human has to check it once, and the tool must guarantee that the plan a human approved is the plan that runs.

## Decision

`pcmigrate introspect` is read-only. It writes three artifacts:

- `report.md` and `report.html`, the findings.
- `mapping.yaml`, the proposed mapping.
- `plan.lock`, a SHA-256 of the normalized mapping plus the source schema fingerprint.

`pcmigrate approve` records approver, time, and hash. Every later stage refuses to run unless an approval matches the current `plan.lock`.

## Consequences

- A human reviews the mapping once, and the tool enforces that the approved plan is the one that runs.
- Any mapping edit or source schema change invalidates approval, and a new review is needed.
