"""Base stack: S3 bucket, KMS key, Secrets Manager entries, runner and storage-integration roles."""

from infra.base.config import PROJECT_NAME, BaseStackConfig
from infra.base.program import build, program_for

__all__ = ["PROJECT_NAME", "BaseStackConfig", "build", "program_for"]
