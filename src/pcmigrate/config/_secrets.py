"""Guards that keep secret values out of configuration (ADR-014)."""

from __future__ import annotations

import re

SECRET_ARN_PREFIX = "arn:aws:secretsmanager:"

_SECRET_VALUE_PATTERNS = (
    re.compile(r"password\s*=", re.IGNORECASE),
    re.compile(r"://[^/\s:@]+:[^/\s@]+@"),
    re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    re.compile(r"\bpcsk_[A-Za-z0-9_]+"),
)


class SecretInConfigError(ValueError):
    """A configuration value looks like a secret instead of a secret ARN."""


def require_secret_arn(value: str, field: str) -> str:
    """Return *value* if it is a Secrets Manager ARN, else raise."""
    if not value.startswith(SECRET_ARN_PREFIX):
        raise SecretInConfigError(
            f"{field} must be a Secrets Manager ARN starting with {SECRET_ARN_PREFIX!r}"
        )
    return value


def reject_secret_values(value: str, field: str) -> str:
    """Return *value* unless it matches a known secret pattern."""
    for pattern in _SECRET_VALUE_PATTERNS:
        if pattern.search(value):
            raise SecretInConfigError(f"{field} appears to contain a secret value")
    return value
