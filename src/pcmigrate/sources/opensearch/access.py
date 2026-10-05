"""Domain access mapping for the OSIS pipeline role on fine-grained-access-control domains.

A managed domain with fine-grained access control ignores the resource policy for data
reads; the pipeline role's ARN must also be a backend role of a security role. This is the
only write the OpenSearch connector ever makes, and it goes through a writable client the
provision step opens on purpose.
"""

from __future__ import annotations

from typing import Any

from pcmigrate.sources.opensearch.client import OpenSearchClient

ROLES_MAPPING = "/_plugins/_security/api/rolesmapping"


def map_backend_role(
    client: OpenSearchClient, role_arn: str, *, security_role: str = "all_access"
) -> dict[str, Any]:
    """Add *role_arn* to the backend roles of *security_role*; idempotent."""
    if client.read_only:
        raise RuntimeError("the role mapping needs a writable client")
    try:
        current = client.get(f"{ROLES_MAPPING}/{security_role}")
    except Exception:
        current = {}
    mapping = dict(current.get(security_role) or {})
    backend = [str(r) for r in mapping.get("backend_roles") or []]
    if role_arn in backend:
        return {"security_role": security_role, "backend_roles": backend, "changed": False}
    body = {
        "backend_roles": [*backend, role_arn],
        "hosts": list(mapping.get("hosts") or []),
        "users": list(mapping.get("users") or []),
    }
    client.request("PUT", f"{ROLES_MAPPING}/{security_role}", body=body)
    return {"security_role": security_role, "backend_roles": body["backend_roles"], "changed": True}
