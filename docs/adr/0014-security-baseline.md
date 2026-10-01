# ADR-014: Least-privilege security baseline

**Status:** Proposed

## Context

Every customer deployment goes through an enterprise security review. A fixed baseline that clears standard reviews without exceptions avoids renegotiating controls per customer.

## Decision

Security rests on six rules:

- Source access uses read-only roles. CDC tiers 2 and 3 get separately scoped grants.
- Secrets live in Secrets Manager, never in config files.
- The runner uses SSM only, with no SSH.
- IAM roles are least-privilege, one per component.
- Data is encrypted with KMS at rest and TLS in transit.
- The report flags likely PII fields for the FDE.

## Consequences

- Standard enterprise reviews pass without exceptions.
- CDC tiers 2 and 3 need explicit DBA sign-off, which the report captures.
