"""OpenSearch add-on stack: OSIS pipeline, Glue job, pipeline role, domain access mapping."""

from infra.opensearch.config import OpenSearchStackConfig
from infra.opensearch.program import (
    build,
    domain_access_statement,
    glue_default_arguments,
    glue_policy,
    osis_trust_policy,
    pipeline_policy,
    program_for,
)

__all__ = [
    "OpenSearchStackConfig",
    "build",
    "domain_access_statement",
    "glue_default_arguments",
    "glue_policy",
    "osis_trust_policy",
    "pipeline_policy",
    "program_for",
]
