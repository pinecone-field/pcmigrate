"""An in-memory OpenSearch transport: enough of the REST API for the connector's unit tests."""

from __future__ import annotations

import copy
import math
import re
from dataclasses import dataclass, field
from typing import Any

from opensearchpy.exceptions import NotFoundError, RequestError

from tests.fixtures.golden.opensearch.documents import golden_documents, golden_mapping


def _dig(doc: Any, path: str) -> Any:
    for key in path.split("."):
        if isinstance(doc, list):
            doc = [d.get(key) for d in doc if isinstance(d, dict)]
            if not doc:
                return None
            continue
        if not isinstance(doc, dict) or key not in doc:
            return None
        doc = doc[key]
    return doc


@dataclass
class FakeIndex:
    mapping: dict[str, Any]
    settings: dict[str, Any] = field(default_factory=dict)
    docs: dict[str, dict[str, Any]] = field(default_factory=dict)
    aliases: list[str] = field(default_factory=list)

    def visible_source(self, doc_id: str) -> dict[str, Any]:
        source = copy.deepcopy(self.docs[doc_id])
        meta = self.mapping.get("_source") or {}
        if meta.get("enabled") is False:
            return {}
        for pattern in meta.get("excludes") or []:
            regex = "^" + re.escape(str(pattern)).replace("\\*", ".*") + "$"
            for key in list(source):
                if re.match(regex, key):
                    del source[key]
        return source


@dataclass
class FakeTransport:
    """Routes perform_request by method and path. Counters advance so workload deltas exist."""

    indices: dict[str, FakeIndex]
    version: str = "2.19.1"
    plugins: dict[str, str] = field(default_factory=lambda: {"opensearch-knn": "2.19.1.0"})
    security_roles: dict[str, Any] | None = None
    role_mappings: dict[str, Any] = field(default_factory=dict)
    stats_calls: int = 0
    writes_per_stats_call: int = 0
    pits: dict[str, str] = field(default_factory=dict)
    requests: list[tuple[str, str]] = field(default_factory=list)
    closed: bool = False
    fail_knn_score: bool = False

    @classmethod
    def golden(cls, **overrides: Any) -> FakeTransport:
        index = FakeIndex(
            mapping=golden_mapping()["mappings"],
            settings={"index": {"knn": "true", "number_of_shards": "1"}},
        )
        for doc in golden_documents():
            index.docs[doc["doc_id"]] = {k: v for k, v in doc.items()}
        transport = cls(indices={"golden-documents": index}, **overrides)
        return transport

    def close(self) -> None:
        self.closed = True

    def perform_request(self, method: str, path: str, params: Any = None, body: Any = None) -> Any:
        self.requests.append((method, path))
        params = params or {}
        if method == "GET" and path == "/":
            return {"version": {"number": self.version, "distribution": "opensearch"}}
        if method == "GET" and path == "/_cat/plugins":
            return [{"component": k, "version": v} for k, v in self.plugins.items()]
        if method == "GET" and path.startswith("/_cat/indices"):
            names = (
                [path.removeprefix("/_cat/indices/")]
                if path != "/_cat/indices"
                else list(self.indices)
            )
            return [
                {
                    "index": n,
                    "status": "open",
                    "docs.count": str(len(self.indices[n].docs)),
                    "store.size": str(4096 * len(self.indices[n].docs)),
                    "pri": "1",
                }
                for n in names
                if n in self.indices
            ]
        if method == "GET" and path == "/_alias":
            return {n: {"aliases": {a: {} for a in i.aliases}} for n, i in self.indices.items()}
        if method == "GET" and path.endswith("/_mapping"):
            name = path.strip("/").split("/")[0]
            return {name: {"mappings": copy.deepcopy(self._index(name).mapping)}}
        if method == "GET" and path.endswith("/_settings"):
            name = path.strip("/").split("/")[0]
            return {name: {"settings": copy.deepcopy(self._index(name).settings)}}
        if method == "GET" and "/_stats/" in path:
            self.stats_calls += 1
            names = path.strip("/").split("/")[0].split(",")
            bump = self.stats_calls * self.writes_per_stats_call
            return {
                "indices": {
                    n: {
                        "total": {
                            "indexing": {"index_total": 1000 + bump, "delete_total": bump // 10},
                            "search": {"query_total": 500 + 2 * bump},
                        }
                    }
                    for n in names
                }
            }
        if method == "GET" and path.startswith("/_nodes/stats"):
            bump = self.stats_calls * self.writes_per_stats_call
            return {
                "nodes": {
                    "n1": {
                        "indices": {
                            "indexing": {"index_total": 1000 + bump, "delete_total": 0},
                            "search": {"query_total": 500 + 2 * bump},
                        }
                    }
                }
            }
        if method == "GET" and path == "/_plugins/_security/api/roles":
            if self.security_roles is None:
                raise NotFoundError(404, "not found", {})
            return copy.deepcopy(self.security_roles)
        if method == "GET" and path.startswith("/_plugins/_security/api/rolesmapping/"):
            role = path.rsplit("/", 1)[1]
            if role not in self.role_mappings:
                raise NotFoundError(404, "not found", {})
            return {role: copy.deepcopy(self.role_mappings[role])}
        if method == "PUT" and path.startswith("/_plugins/_security/api/rolesmapping/"):
            role = path.rsplit("/", 1)[1]
            self.role_mappings[role] = copy.deepcopy(body)
            return {"status": "OK"}
        if method == "POST" and path.endswith("/_count"):
            name = path.strip("/").split("/")[0]
            return {"count": len(self._index(name).docs)}
        if method == "POST" and path.endswith("/_search/point_in_time"):
            name = path.strip("/").split("/")[0]
            pit_id = f"pit-{len(self.pits) + 1}"
            self.pits[pit_id] = name
            return {"pit_id": pit_id}
        if method == "DELETE" and path == "/_search/point_in_time":
            for pit_id in (body or {}).get("pit_id", []):
                self.pits.pop(pit_id, None)
            return {"pits": []}
        if method == "POST" and path.endswith("/_mget"):
            name = path.strip("/").split("/")[0]
            index = self._index(name)
            return {
                "docs": [
                    {
                        "_index": name,
                        "_id": i,
                        "found": i in index.docs,
                        "_source": index.visible_source(i) if i in index.docs else None,
                    }
                    for i in body["ids"]
                ]
            }
        if method == "POST" and path.endswith("/_search"):
            if path == "/_search":
                pit_id = body["pit"]["id"]
                name = self.pits[pit_id]
            else:
                name = path.strip("/").split("/")[0]
            return self._search(name, body or {})
        if method == "HEAD":
            name = path.strip("/")
            if name in self.indices:
                return {}
            raise NotFoundError(404, "not found", {})
        raise RequestError(400, f"unsupported fake route {method} {path}", {})

    def _index(self, name: str) -> FakeIndex:
        if name not in self.indices:
            raise NotFoundError(404, f"no such index {name}", {})
        return self.indices[name]

    def _matches(self, index: FakeIndex, doc_id: str, query: dict[str, Any]) -> bool:
        source = index.docs[doc_id]
        if not query or "match_all" in query:
            return True
        if "function_score" in query:
            return self._matches(index, doc_id, query["function_score"].get("query") or {})
        if "script_score" in query:
            return self._matches(index, doc_id, query["script_score"].get("query") or {})
        if "bool" in query:
            clauses = query["bool"].get("filter") or []
            if isinstance(clauses, dict):
                clauses = [clauses]
            return all(self._matches(index, doc_id, c) for c in clauses)
        if "term" in query:
            ((fld, value),) = query["term"].items()
            value = value.get("value", value) if isinstance(value, dict) else value
            actual = _dig(source, fld)
            return actual == value or (isinstance(actual, list) and value in actual)
        if "terms" in query:
            ((fld, values),) = query["terms"].items()
            actual = _dig(source, fld)
            return actual in values
        if "range" in query:
            ((fld, spec),) = query["range"].items()
            actual = _dig(source, fld)
            if actual is None:
                return False
            if "gt" in spec:
                return str(actual) > str(spec["gt"])
            if "gte" in spec:
                return str(actual) >= str(spec["gte"])
            return True
        raise RequestError(400, f"unsupported fake query {list(query)}", {})

    def _score(self, index: FakeIndex, doc_id: str, query: dict[str, Any]) -> float:
        script = query["script_score"]["script"]
        if script.get("lang") == "knn" and self.fail_knn_score:
            raise RequestError(400, "knn_score unavailable", {})
        params = script["params"]
        vec = _dig(index.docs[doc_id], params["field"])
        if not isinstance(vec, list):
            return -math.inf
        q = params["query_value"]
        space = params.get("space_type", "cosinesimil")
        if script.get("lang") == "painless":
            space = "cosinesimil" if "cosineSimilarity" in script["source"] else "l2"
        dot = sum(a * b for a, b in zip(q, vec, strict=True))
        if space == "cosinesimil":
            na = math.sqrt(sum(a * a for a in q)) or 1.0
            nb = math.sqrt(sum(b * b for b in vec)) or 1.0
            return 1 + dot / (na * nb)
        if space == "innerproduct":
            return dot + 1 if dot >= 0 else 1 / (1 - dot)
        d2 = sum((a - b) ** 2 for a, b in zip(q, vec, strict=True))
        return 1 / (1 + d2)

    def _search(self, name: str, body: dict[str, Any]) -> dict[str, Any]:
        index = self._index(name)
        query = body.get("query") or {"match_all": {}}
        ids = [i for i in index.docs if self._matches(index, i, query)]
        if "slice" in body:
            sl = body["slice"]
            ids = [i for n, i in enumerate(ids) if n % sl["max"] == sl["id"]]
        sort = body.get("sort")
        if "script_score" in query:
            scored = sorted(
                ((self._score(index, i, query), i) for i in ids), key=lambda s: (-s[0], s[1])
            )
            ids = [i for _, i in scored]
        elif "function_score" in query:
            seed = query["function_score"].get("random_score", {}).get("seed", 0)
            ids = sorted(ids, key=lambda i: hash((seed, i)))
        elif sort and isinstance(sort[0], dict) and "_doc" not in sort[0]:
            fld = next(iter(sort[0]))
            ids = sorted(ids, key=lambda i: (str(_dig(index.docs[i], fld)), i))
        if body.get("search_after") is not None:
            after = body["search_after"]
            if sort and "_doc" in sort[0]:
                ids = [i for n, i in enumerate(ids) if n > int(after[0])]
            elif sort:
                fld = next(iter(sort[0]))
                ids = [
                    i
                    for i in ids
                    if (str(_dig(index.docs[i], fld)), i) > (str(after[0]), str(after[1]))
                ]
        size = int(body.get("size", 10))
        hits = []
        for position, doc_id in enumerate(ids[:size]):
            hit: dict[str, Any] = {"_index": name, "_id": doc_id}
            src = body.get("_source", True)
            if src is True:
                hit["_source"] = index.visible_source(doc_id)
            elif isinstance(src, list):
                visible = index.visible_source(doc_id)
                hit["_source"] = {k: visible[k] for k in src if k in visible}
            if sort and "_doc" in sort[0]:
                base = (
                    int(body["search_after"][0]) + 1 if body.get("search_after") is not None else 0
                )
                hit["sort"] = [base + position]
            elif sort:
                fld = next(iter(sort[0]))
                hit["sort"] = [_dig(index.docs[doc_id], fld), doc_id]
            hits.append(hit)
        result: dict[str, Any] = {"hits": {"total": {"value": len(ids)}, "hits": hits}}
        if "aggs" in body:
            agg = body["aggs"]["top"]["terms"]
            counts: dict[Any, int] = {}
            for i in ids:
                value = _dig(index.docs[i], agg["field"])
                for v in value if isinstance(value, list) else [value]:
                    if v is not None:
                        counts[v] = counts.get(v, 0) + 1
            buckets = sorted(
                (
                    {"key": k, "doc_count": c}
                    for k, c in counts.items()
                    if c >= agg.get("min_doc_count", 1)
                ),
                key=lambda b: (-b["doc_count"], str(b["key"])),
            )[: agg.get("size", 10)]
            result["aggregations"] = {"top": {"buckets": buckets}}
        return result


def make_client(transport: FakeTransport, *, read_only: bool = True) -> Any:
    from pcmigrate.sources.opensearch.client import ConnectionOptions, OpenSearchClient

    return OpenSearchClient(
        ConnectionOptions("http://fake:9200", auth="none"), read_only=read_only, transport=transport
    )
