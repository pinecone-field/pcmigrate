"""OSIS pipeline configuration exporting approved indices as NDJSON to the raw/ zone (ADR-009).

A pipeline scans as soon as it exists, so provision creates it armed with a far-future
``scheduling.start_time``; the export stage records T0 and then moves the start time to T0,
which is what makes "T0 before the scan" hold. One sub-pipeline per index, so each index
lands under its own fixed ``raw/<index>/`` prefix.
The ``add_entries`` processor copies the document id and index name out of event metadata
into the body, which is how the Glue transform and the runner recover ``_id``. The Data
Prepper OpenSearch source has no query option, so the optional ``updated_at`` rescan runs as a
ranged search from the runner (``change_feed.RescanFeed``) rather than a second pipeline.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

import yaml

PIPELINE_ROLE_PLACEHOLDER = "{pipeline_role_arn}"
ARMED_START_TIME = "2099-01-01T00:00:00Z"
MAX_PIPELINE_NAME = 28


def pipeline_name(migration_id: str) -> str:
    """``pcm-<migration_id>`` trimmed to OSIS's 28-character limit."""
    name = f"pcm-{migration_id}"[:MAX_PIPELINE_NAME].rstrip("-")
    return name if re.fullmatch(r"[a-z][a-z0-9-]{2,27}", name) else "pcm-migration"


def sub_pipeline_name(index: str, position: int) -> str:
    """A YAML-safe name for one index's sub-pipeline."""
    slug = re.sub(r"[^a-z0-9]+", "-", index.lower()).strip("-")[:20]
    return f"idx{position}-{slug}" if slug else f"idx{position}"


@dataclass(frozen=True, slots=True)
class OsisPipelineSpec:
    """Everything the pipeline body needs. The role ARN may be a placeholder filled by Pulumi."""

    migration_id: str
    endpoint: str
    indices: tuple[str, ...]
    region: str
    bucket: str
    raw_prefix: str
    pipeline_role_arn: str = PIPELINE_ROLE_PLACEHOLDER
    serverless: bool = False
    credentials_secret_arn: str | None = None
    insecure_tls: bool = False
    batch_size: int = 1000
    search_context_type: str = "point_in_time"
    event_count: int = 10_000
    maximum_size: str = "50mb"
    event_collect_timeout: str = "60s"
    start_time: str | None = ARMED_START_TIME

    @property
    def name(self) -> str:
        """The OSIS pipeline name."""
        return pipeline_name(self.migration_id)

    @property
    def self_managed(self) -> bool:
        """True when the cluster is reached with basic auth from a secret rather than IAM."""
        return self.credentials_secret_arn is not None


def raw_prefix_for(raw_prefix: str, index: str) -> str:
    """``<migration_id>/raw/<index>/``."""
    return f"{raw_prefix.rstrip('/')}/{index}/"


def pipeline_document(spec: OsisPipelineSpec) -> dict[str, Any]:
    """The configuration as a dict, before YAML rendering."""
    body: dict[str, Any] = {"version": "2"}
    secret_name = "source-credentials"
    if spec.self_managed:
        body["extension"] = {
            "aws": {
                "secrets": {
                    secret_name: {
                        "secret_id": spec.credentials_secret_arn,
                        "region": spec.region,
                        "sts_role_arn": spec.pipeline_role_arn,
                    }
                }
            }
        }
    for position, index in enumerate(spec.indices):
        source: dict[str, Any] = {
            "hosts": [spec.endpoint],
            "indices": {"include": [{"index_name_regex": f"^{re.escape(index)}$"}]},
            "search_options": {
                "batch_size": spec.batch_size,
                "search_context_type": spec.search_context_type,
            },
            "acknowledgments": True,
        }
        if spec.start_time:
            source["scheduling"] = {"index_read_count": 1, "start_time": spec.start_time}
        if spec.self_managed:
            source["username"] = f"${{{{aws_secrets:{secret_name}:username}}}}"
            source["password"] = f"${{{{aws_secrets:{secret_name}:password}}}}"
            if spec.insecure_tls:
                source["connection"] = {"insecure": True}
        else:
            source["aws"] = {
                "region": spec.region,
                "sts_role_arn": spec.pipeline_role_arn,
                "serverless": spec.serverless,
            }
        body[sub_pipeline_name(index, position)] = {
            "source": {"opensearch": source},
            "processor": [
                {
                    "add_entries": {
                        "entries": [
                            {
                                "key": "_id",
                                "value_expression": 'getMetadata("opensearch-document_id")',
                                "overwrite_if_key_exists": True,
                            },
                            {
                                "key": "_index",
                                "value_expression": 'getMetadata("opensearch-index")',
                                "overwrite_if_key_exists": True,
                            },
                        ]
                    }
                }
            ],
            "sink": [
                {
                    "s3": {
                        "aws": {"region": spec.region, "sts_role_arn": spec.pipeline_role_arn},
                        "bucket": spec.bucket,
                        "object_key": {"path_prefix": raw_prefix_for(spec.raw_prefix, index)},
                        "threshold": {
                            "event_count": spec.event_count,
                            "maximum_size": spec.maximum_size,
                            "event_collect_timeout": spec.event_collect_timeout,
                        },
                        "codec": {"ndjson": {}},
                        "compression": "none",
                        "buffer_type": "in_memory",
                    }
                }
            ],
        }
    return body


def pipeline_body(spec: OsisPipelineSpec) -> str:
    """The YAML OSIS expects in ``pipeline_configuration_body``."""
    return yaml.safe_dump(pipeline_document(spec), sort_keys=False, width=200)


def with_role(body: str, pipeline_role_arn: str) -> str:
    """Fill the role placeholder once Pulumi knows the pipeline role ARN."""
    return body.replace(PIPELINE_ROLE_PLACEHOLDER, pipeline_role_arn)


def with_start_time(body: str, start_time: str) -> str:
    """Move every sub-pipeline's ``scheduling.start_time`` to *start_time* (T0)."""
    document = yaml.safe_load(body)
    for pipeline in document.values():
        if not isinstance(pipeline, dict) or "source" not in pipeline:
            continue
        source = pipeline["source"].get("opensearch")
        if isinstance(source, dict):
            source.setdefault("scheduling", {})["start_time"] = start_time
            source["scheduling"].setdefault("index_read_count", 1)
    rendered: str = yaml.safe_dump(document, sort_keys=False, width=200)
    return rendered
