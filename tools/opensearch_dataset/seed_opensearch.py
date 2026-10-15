# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "boto3>=1.34",
#   "opensearch-py>=2.6",
#   "pyarrow>=16",
#   "numpy>=1.26",
#   "orjson>=3.9",
#   "pyyaml>=6",
#   "pinecone>=10",
# ]
# ///
"""Stand up an Amazon OpenSearch Service domain and load a parquet vector dataset into it.

Independent of pcmigrate: run it with ``uv run seed_opensearch.py <command> --config cfg.yaml``
and uv installs the dependencies listed above into an ephemeral environment. Each parquet
file under ``data.dir`` becomes one index, so a later pcmigrate run lands each index in its
own Pinecone namespace.

Commands:
  plan            rows, bytes, and index names the config would produce
  create-domain   create the domain (or reuse it) and wait for the endpoint
  create-indices  one index per file with the knn_vector mapping and load-time settings
  load            parallel, resumable bulk load; restores replicas and refresh afterwards
  verify          document counts per index against the parquet row counts
  recall          recall@k of OpenSearch (knn query) or Pinecone against the ground truth
  status          domain state, endpoint, and per-index counts and sizes
  delete-domain   delete the domain
"""

from __future__ import annotations

import argparse
import concurrent.futures
import glob
import gzip
import json
import logging
import os
import re
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import numpy as np
import orjson
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

log = logging.getLogger("seed_opensearch")

DEFAULT_METADATA_TYPES: dict[str, str] = {
    "asin": "keyword",
    "parent_asin": "keyword",
    "user_id": "keyword",
    "main_category": "keyword",
    "image": "keyword",
    "title": "text",
    "review": "text",
    "text": "text",
    "helpful_vote": "integer",
    "rating": "float",
    "verified_purchase": "boolean",
    "timestamp": "date",
    "timestamp_epoch": "long",
}


# ----------------------------------------------------------------------------- config


@dataclass
class Config:
    raw: dict[str, Any]

    @classmethod
    def load(cls, path: str) -> Config:
        with open(path) as handle:
            data = yaml.safe_load(handle) or {}
        return cls(data)

    def get(self, dotted: str, default: Any = None) -> Any:
        node: Any = self.raw
        for key in dotted.split("."):
            if not isinstance(node, dict) or key not in node:
                return default
            node = node[key]
        return node

    @property
    def region(self) -> str:
        return str(self.get("region", os.environ.get("AWS_REGION", "us-east-1")))

    @property
    def domain_name(self) -> str:
        return str(self.get("domain.name", "pcm-dataset"))

    @property
    def vector_field(self) -> str:
        return str(self.get("index.vector_field", "embedding"))

    @property
    def dimension(self) -> int:
        return int(self.get("index.dimension", 1024))

    @property
    def id_field(self) -> str:
        return str(self.get("index.id_field", "doc_id"))

    @property
    def metadata_types(self) -> dict[str, str]:
        configured = self.get("index.metadata")
        return dict(configured) if isinstance(configured, dict) else dict(DEFAULT_METADATA_TYPES)


# ----------------------------------------------------------------------------- data files


@dataclass(frozen=True)
class DataFile:
    path: str
    index: str
    rows: int
    row_groups: int
    queries: str | None = None


def _filesystem(path: str) -> tuple[Any, str]:
    if path.startswith("s3://"):
        from pyarrow import fs

        parsed = urlparse(path)
        return fs.S3FileSystem(region=os.environ.get("AWS_REGION")), f"{parsed.netloc}{parsed.path}"
    return None, path


def _list_parquet(cfg: Config) -> list[str]:
    root = str(cfg.get("data.dir"))
    pattern = str(cfg.get("data.pattern", "*.parquet"))
    if root.startswith("s3://"):
        from pyarrow import fs

        parsed = urlparse(root)
        s3 = fs.S3FileSystem(region=cfg.region)
        selector = fs.FileSelector(f"{parsed.netloc}{parsed.path}".rstrip("/"), recursive=False)
        names = [
            f"s3://{info.path}"
            for info in s3.get_file_info(selector)
            if info.is_file and Path(info.path).match(pattern)
        ]
        return sorted(names)
    return sorted(glob.glob(os.path.join(root, pattern)))


def index_name_for(cfg: Config, path: str, position: int) -> str:
    template = str(cfg.get("index.name_template", "passages-{n:06d}"))
    stem = Path(path).stem
    match = re.search(r"(\d+)$", stem)
    n = int(match.group(1)) if match else position
    name = template.format(n=n, stem=stem, position=position)
    name = re.sub(r"[^a-z0-9._-]+", "-", name.lower()).strip("-_.")
    if not name or name.startswith(("_", "-", ".")):
        raise ValueError(f"index name {name!r} derived from {path} is not valid")
    return name


def _data_file(cfg: Config, path: str, index: str, queries: str | None) -> DataFile:
    filesystem, local = _filesystem(path)
    meta = pq.ParquetFile(local, filesystem=filesystem).metadata
    rows = meta.num_rows
    limit = cfg.get("data.max_rows_per_file")
    if limit:
        rows = min(rows, int(limit))
    return DataFile(
        path=path, index=index, rows=rows, row_groups=meta.num_row_groups, queries=queries
    )


def data_files(cfg: Config) -> list[DataFile]:
    """One DataFile per index: from ``data.sets`` (explicit) or ``data.dir`` plus a pattern."""
    sets = cfg.get("data.sets")
    if sets:
        out = [
            _data_file(cfg, str(item["data"]), str(item["index"]), item.get("queries"))
            for item in sets
        ]
    else:
        out = []
        only = cfg.get("data.only_files")
        queries = cfg.get("data.queries")
        for position, path in enumerate(_list_parquet(cfg)):
            if only and Path(path).name not in set(only) and Path(path).stem not in set(only):
                continue
            out.append(
                _data_file(
                    cfg,
                    path,
                    index_name_for(cfg, path, position),
                    str(queries) if queries else None,
                )
            )
    if not out:
        raise SystemExit("no data files: set data.sets or data.dir in the config")
    names = [f.index for f in out]
    if len(set(names)) != len(names):
        raise SystemExit(f"duplicate index names: {names}")
    return out


# ----------------------------------------------------------------------------- clients


def opensearch_client(cfg: Config, endpoint: str | None = None) -> Any:
    from opensearchpy import OpenSearch, RequestsHttpConnection

    url = endpoint or domain_endpoint(cfg)
    parsed = urlparse(url)
    kwargs: dict[str, Any] = {
        "hosts": [url],
        "use_ssl": parsed.scheme == "https",
        "verify_certs": bool(cfg.get("domain.verify_certs", True)),
        "ssl_show_warn": False,
        "connection_class": RequestsHttpConnection,
        "http_compress": False,
        "timeout": int(cfg.get("load.request_timeout_s", 300)),
        "max_retries": 5,
        "retry_on_timeout": True,
        "retry_on_status": (429, 502, 503, 504),
    }
    auth = str(cfg.get("domain.auth", "sigv4"))
    if auth == "sigv4":
        import boto3
        from opensearchpy import AWSV4SignerAuth

        credentials = boto3.Session().get_credentials()
        if credentials is None:
            raise SystemExit("no AWS credentials in the environment")
        kwargs["http_auth"] = AWSV4SignerAuth(credentials, cfg.region, "es")
    elif auth == "basic":
        user, _, password = os.environ.get("OPENSEARCH_BASIC_AUTH", "").partition(":")
        kwargs["http_auth"] = (user, password)
    return OpenSearch(**kwargs)


def domain_endpoint(cfg: Config) -> str:
    override = cfg.get("domain.endpoint")
    if override:
        return str(override)
    import boto3

    status = boto3.client("opensearch", region_name=cfg.region).describe_domain(
        DomainName=cfg.domain_name
    )["DomainStatus"]
    endpoint = status.get("Endpoint") or (status.get("Endpoints") or {}).get("vpc")
    if not endpoint:
        raise SystemExit(f"domain {cfg.domain_name} has no endpoint yet (still processing?)")
    return f"https://{endpoint}"


# ----------------------------------------------------------------------------- domain


def access_policy(cfg: Config, account_id: str) -> str:
    principals = cfg.get("domain.access_principals") or [f"arn:aws:iam::{account_id}:root"]
    return json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": list(principals)},
                    "Action": "es:*",
                    "Resource": f"arn:aws:es:{cfg.region}:{account_id}:domain/{cfg.domain_name}/*",
                }
            ],
        }
    )


def cmd_create_domain(cfg: Config, args: argparse.Namespace) -> None:
    import boto3

    client = boto3.client("opensearch", region_name=cfg.region)
    account_id = boto3.client("sts", region_name=cfg.region).get_caller_identity()["Account"]
    existing = {d["DomainName"] for d in client.list_domain_names()["DomainNames"]}
    if cfg.domain_name in existing:
        log.info("domain %s already exists; waiting for its endpoint", cfg.domain_name)
    else:
        cluster: dict[str, Any] = {
            "InstanceType": str(cfg.get("domain.instance_type", "r6g.4xlarge.search")),
            "InstanceCount": int(cfg.get("domain.instance_count", 3)),
            "ZoneAwarenessEnabled": bool(cfg.get("domain.zone_awareness", False)),
        }
        if cluster["ZoneAwarenessEnabled"]:
            cluster["ZoneAwarenessConfig"] = {
                "AvailabilityZoneCount": int(cfg.get("domain.availability_zones", 2))
            }
        if cfg.get("domain.dedicated_master_type"):
            cluster.update(
                DedicatedMasterEnabled=True,
                DedicatedMasterType=str(cfg.get("domain.dedicated_master_type")),
                DedicatedMasterCount=int(cfg.get("domain.dedicated_master_count", 3)),
            )
        ebs: dict[str, Any] = {
            "EBSEnabled": True,
            "VolumeType": str(cfg.get("domain.ebs_volume_type", "gp3")),
            "VolumeSize": int(cfg.get("domain.ebs_volume_gb", 500)),
        }
        if ebs["VolumeType"] == "gp3":
            ebs["Iops"] = int(cfg.get("domain.ebs_iops", 3000))
            ebs["Throughput"] = int(cfg.get("domain.ebs_throughput_mbps", 250))
        request: dict[str, Any] = {
            "DomainName": cfg.domain_name,
            "EngineVersion": str(cfg.get("domain.engine_version", "OpenSearch_2.19")),
            "ClusterConfig": cluster,
            "EBSOptions": ebs,
            "AccessPolicies": access_policy(cfg, account_id),
            "NodeToNodeEncryptionOptions": {"Enabled": True},
            "EncryptionAtRestOptions": {"Enabled": True},
            "DomainEndpointOptions": {"EnforceHTTPS": True},
            "AdvancedOptions": {"rest.action.multi.allow_explicit_index": "true"},
            "TagList": [{"Key": "managed_by", "Value": "pcmigrate-dataset-seed"}],
        }
        log.info("creating domain %s: %s", cfg.domain_name, json.dumps(cluster))
        client.create_domain(**request)
    endpoint = wait_for_endpoint(client, cfg.domain_name, timeout_s=float(args.timeout))
    log.info("domain ready: https://%s", endpoint)
    print(f"https://{endpoint}")


def wait_for_endpoint(client: Any, name: str, *, timeout_s: float) -> str:
    deadline = time.monotonic() + timeout_s
    while True:
        status = client.describe_domain(DomainName=name)["DomainStatus"]
        endpoint = status.get("Endpoint") or (status.get("Endpoints") or {}).get("vpc")
        if endpoint and not status.get("Processing"):
            return str(endpoint)
        if time.monotonic() > deadline:
            raise SystemExit(f"domain {name} not ready after {timeout_s:.0f}s")
        log.info("waiting for domain %s (processing=%s)", name, status.get("Processing"))
        time.sleep(30)


def cmd_delete_domain(cfg: Config, args: argparse.Namespace) -> None:
    import boto3

    if not args.yes:
        raise SystemExit("pass --yes to delete the domain")
    client = boto3.client("opensearch", region_name=cfg.region)
    client.delete_domain(DomainName=cfg.domain_name)
    log.info("delete requested for domain %s", cfg.domain_name)


def cmd_status(cfg: Config, args: argparse.Namespace) -> None:
    ready = True
    if cfg.get("domain.endpoint"):
        print(f"endpoint override: {cfg.get('domain.endpoint')}")
    else:
        import boto3

        client = boto3.client("opensearch", region_name=cfg.region)
        try:
            status = client.describe_domain(DomainName=cfg.domain_name)["DomainStatus"]
        except client.exceptions.ResourceNotFoundException:
            print(f"domain {cfg.domain_name}: not found")
            return
        print(
            f"domain {cfg.domain_name}: processing={status.get('Processing')} "
            f"endpoint={status.get('Endpoint')} version={status.get('EngineVersion')} "
            f"nodes={status['ClusterConfig'].get('InstanceCount')}x"
            f"{status['ClusterConfig'].get('InstanceType')}"
        )
        ready = bool(status.get("Endpoint")) and not status.get("Processing")
    if ready:
        os_client = opensearch_client(cfg)
        pattern = index_pattern(cfg)
        for row in os_client.cat.indices(index=pattern, format="json", bytes="b", s="index"):
            print(
                f"  {row['index']:<24} docs={int(row['docs.count']):>10,} "
                f"size={int(row['store.size']) / 1e9:8.2f} GB health={row['health']}"
            )


def index_pattern(cfg: Config) -> str:
    """Every index this config owns, as one comma-separated target or a wildcard."""
    sets = cfg.get("data.sets")
    if sets:
        return ",".join(str(item["index"]) for item in sets)
    template = str(cfg.get("index.name_template", "passages-{n:06d}"))
    return template.split("{", 1)[0] + "*"


# ----------------------------------------------------------------------------- indices


def index_body(cfg: Config) -> dict[str, Any]:
    knn = cfg.get("index.knn") or {}
    properties: dict[str, Any] = {
        cfg.id_field: {"type": "keyword"},
        cfg.vector_field: {
            "type": "knn_vector",
            "dimension": cfg.dimension,
            "method": {
                "name": "hnsw",
                "space_type": str(knn.get("space_type", "cosinesimil")),
                "engine": str(knn.get("engine", "lucene")),
                "parameters": {
                    "ef_construction": int(knn.get("ef_construction", 128)),
                    "m": int(knn.get("m", 16)),
                },
            },
        },
    }
    for name, kind in cfg.metadata_types.items():
        if kind == "text":
            properties[name] = {
                "type": "text",
                "fields": {"raw": {"type": "keyword", "ignore_above": 256}},
            }
        elif kind == "date":
            properties[name] = {"type": "date", "format": "strict_date_optional_time||epoch_millis"}
        else:
            properties[name] = {"type": kind}
    return {
        "settings": {
            "index": {
                "number_of_shards": int(cfg.get("index.shards", 2)),
                "number_of_replicas": 0,
                "refresh_interval": "-1",
                "knn": True,
                "knn.algo_param.ef_search": int(knn.get("ef_search", 100)),
                "translog.flush_threshold_size": "1gb",
            }
        },
        "mappings": {"dynamic": "false", "properties": properties},
    }


def cmd_create_indices(cfg: Config, args: argparse.Namespace) -> None:
    client = opensearch_client(cfg)
    body = index_body(cfg)
    for item in data_files(cfg):
        if client.indices.exists(index=item.index):
            if args.recreate:
                log.info("deleting existing index %s", item.index)
                client.indices.delete(index=item.index)
            else:
                log.info("index %s exists; keeping it (use --recreate to drop)", item.index)
                continue
        client.indices.create(index=item.index, body=body)
        log.info(
            "created index %s for %s (%s rows)", item.index, Path(item.path).name, f"{item.rows:,}"
        )


# ----------------------------------------------------------------------------- load


@dataclass(frozen=True)
class Task:
    file: DataFile
    row_group: int
    row_offset: int
    row_limit: int

    @property
    def key(self) -> str:
        return f"{self.file.index}:{self.row_group}"


def plan_tasks(cfg: Config) -> list[Task]:
    tasks: list[Task] = []
    for item in data_files(cfg):
        filesystem, local = _filesystem(item.path)
        meta = pq.ParquetFile(local, filesystem=filesystem).metadata
        offset = 0
        for rg in range(meta.num_row_groups):
            rows = meta.row_group(rg).num_rows
            if offset >= item.rows:
                break
            tasks.append(Task(item, rg, offset, min(rows, item.rows - offset)))
            offset += rows
    return tasks


class State:
    def __init__(self, path: str) -> None:
        self.path = path
        self.lock = threading.Lock()
        self.done: dict[str, int] = {}
        if os.path.exists(path):
            with open(path) as handle:
                self.done = json.load(handle).get("done", {})

    def mark(self, key: str, docs: int) -> None:
        with self.lock:
            self.done[key] = docs
            tmp = self.path + ".tmp"
            with open(tmp, "w") as handle:
                json.dump({"done": self.done}, handle)
            os.replace(tmp, self.path)


def _doc_lines(cfg_raw: dict[str, Any], task: Task, table: pa.Table) -> list[bytes]:
    cfg = Config(cfg_raw)
    vector_field = cfg.vector_field
    id_field = cfg.id_field
    precision = cfg.get("load.float_decimals", 7)
    keep = set(cfg.metadata_types)
    ids = table.column("id").to_pylist()
    metadata = table.column("metadata").to_pylist()
    values = table.column("values").combine_chunks()
    flat = values.values.to_numpy(zero_copy_only=False)
    matrix = flat.reshape(len(ids), -1)
    if precision is not None:
        matrix = np.round(matrix, int(precision))
    lines: list[bytes] = []
    for i, doc_id in enumerate(ids):
        meta = orjson.loads(metadata[i]) if metadata[i] else {}
        doc = {k: v for k, v in meta.items() if k in keep}
        doc[id_field] = doc_id
        doc[vector_field] = matrix[i]
        lines.append(orjson.dumps({"index": {"_index": task.file.index, "_id": doc_id}}))
        lines.append(orjson.dumps(doc, option=orjson.OPT_SERIALIZE_NUMPY))
    return lines


def _bulk_request(client: Any, body: bytes, *, compress: bool) -> dict[str, Any]:
    """POST _bulk with level-1 gzip: opensearch-py's own http_compress uses level 9, which
    costs about 3 ms per 1024-dim document for a few percent better ratio."""
    headers = {"Content-Type": "application/x-ndjson"}
    if compress:
        body = gzip.compress(body, compresslevel=1)
        headers["Content-Encoding"] = "gzip"
    response: dict[str, Any] = client.transport.perform_request(
        "POST", "/_bulk", body=body, headers=headers, timeout=300
    )
    return response


def _send_bulk(
    client: Any, lines: list[bytes], *, retries: int = 5, compress: bool = True
) -> tuple[int, list[str]]:
    body = b"\n".join(lines) + b"\n"
    attempt = 0
    while True:
        attempt += 1
        try:
            response = _bulk_request(client, body, compress=compress)
        except Exception as err:
            if attempt >= retries:
                raise
            log.warning("bulk failed (%s); retry %s/%s", err, attempt, retries)
            time.sleep(min(60, 2**attempt))
            continue
        if not response.get("errors"):
            return len(lines) // 2, []
        failed: list[tuple[bytes, bytes]] = []
        reasons: list[str] = []
        items = response["items"]
        for n, item in enumerate(items):
            result = item.get("index") or item.get("create") or {}
            if "error" in result:
                failed.append((lines[2 * n], lines[2 * n + 1]))
                reasons.append(str(result["error"].get("reason", result["error"]))[:200])
        retryable = all(("429" in r or "rejected" in r or "timeout" in r.lower()) for r in reasons)
        if retryable and attempt < retries:
            log.warning(
                "%s items rejected (%s); retry %s/%s", len(failed), reasons[0], attempt, retries
            )
            time.sleep(min(60, 2**attempt))
            lines = [line for pair in failed for line in pair]
            body = b"\n".join(lines) + b"\n"
            continue
        return (len(items) - len(failed)), reasons


def run_task(cfg_raw: dict[str, Any], task_dict: dict[str, Any], endpoint: str) -> dict[str, Any]:
    """Executed in a worker process: read one row group, serialize, bulk-send in batches."""
    cfg = Config(cfg_raw)
    file = DataFile(**task_dict["file"])
    task = Task(file, task_dict["row_group"], task_dict["row_offset"], task_dict["row_limit"])
    client = opensearch_client(cfg, endpoint)
    filesystem, local = _filesystem(file.path)
    table = pq.ParquetFile(local, filesystem=filesystem).read_row_group(
        task.row_group, columns=["id", "metadata", "values"]
    )
    if task.row_limit < table.num_rows:
        table = table.slice(0, task.row_limit)
    lines = _doc_lines(cfg_raw, task, table)
    batch_docs = int(cfg.get("load.batch_docs", 500))
    compress = str(cfg.get("load.compression", "gzip")) == "gzip"
    sent = 0
    failures: list[str] = []
    for start in range(0, len(lines), 2 * batch_docs):
        ok, reasons = _send_bulk(client, lines[start : start + 2 * batch_docs], compress=compress)
        sent += ok
        failures.extend(reasons[:5])
    return {"key": task.key, "docs": sent, "rows": table.num_rows, "failures": failures}


def cmd_load(cfg: Config, args: argparse.Namespace) -> None:
    endpoint = domain_endpoint(cfg)
    tasks = plan_tasks(cfg)
    state = State(str(cfg.get("load.state_file", "seed-state.json")))
    pending = [t for t in tasks if t.key not in state.done]
    total_docs = sum(t.row_limit for t in tasks)
    done_docs = sum(state.done.values())
    log.info(
        "%s tasks (%s pending), %s documents (%s already loaded), endpoint %s",
        len(tasks),
        len(pending),
        f"{total_docs:,}",
        f"{done_docs:,}",
        endpoint,
    )
    if not pending:
        log.info("nothing to load")
    workers = int(args.workers or cfg.get("load.workers", max(2, (os.cpu_count() or 4) - 1)))
    started = time.monotonic()
    loaded = 0
    failures: list[str] = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(
                run_task,
                cfg.raw,
                {
                    "file": t.file.__dict__,
                    "row_group": t.row_group,
                    "row_offset": t.row_offset,
                    "row_limit": t.row_limit,
                },
                endpoint,
            ): t
            for t in pending
        }
        last_report = started
        for future in concurrent.futures.as_completed(futures):
            task = futures[future]
            result = future.result()
            if result["failures"]:
                failures.extend(result["failures"])
                log.error(
                    "%s: %s documents failed, e.g. %s",
                    task.key,
                    result["rows"] - result["docs"],
                    result["failures"][0],
                )
            state.mark(task.key, int(result["docs"]))
            loaded += int(result["docs"])
            now = time.monotonic()
            if now - last_report >= 30 or loaded == sum(t.row_limit for t in pending):
                rate = loaded / max(1e-9, now - started)
                remaining = sum(t.row_limit for t in pending) - loaded
                log.info(
                    "loaded %s/%s this run (%.0f docs/s, ~%.0f min left)",
                    f"{loaded:,}",
                    f"{sum(t.row_limit for t in pending):,}",
                    rate,
                    remaining / max(rate, 1) / 60,
                )
                last_report = now
    log.info("load finished in %.0fs; finalizing indices", time.monotonic() - started)
    finalize_indices(cfg, args)
    if failures:
        log.error("%s batches reported document failures; first: %s", len(failures), failures[0])
        raise SystemExit(1)


def finalize_indices(cfg: Config, args: argparse.Namespace) -> None:
    client = opensearch_client(cfg)
    replicas = int(cfg.get("index.replicas", 1))
    refresh = str(cfg.get("index.refresh_interval", "30s"))
    for item in data_files(cfg):
        client.indices.refresh(index=item.index)
        client.indices.put_settings(
            index=item.index,
            body={"index": {"number_of_replicas": replicas, "refresh_interval": refresh}},
        )
        if bool(cfg.get("load.force_merge", False)):
            log.info("force-merging %s (this can take a while)", item.index)
            client.indices.forcemerge(index=item.index, max_num_segments=1, request_timeout=3600)
    log.info("indices refreshed; replicas=%s refresh_interval=%s", replicas, refresh)


# ----------------------------------------------------------------------------- verify / recall


def cmd_verify(cfg: Config, args: argparse.Namespace) -> None:
    client = opensearch_client(cfg)
    ok = True
    for item in data_files(cfg):
        client.indices.refresh(index=item.index)
        count = int(client.count(index=item.index)["count"])
        mark = "ok" if count == item.rows else "MISMATCH"
        if count != item.rows:
            ok = False
        print(f"{item.index:<24} expected={item.rows:>10,} actual={count:>10,} {mark}")
    sample = client.search(index=index_pattern(cfg), body={"size": 1, "query": {"match_all": {}}})[
        "hits"
    ]["hits"]
    if sample:
        doc = sample[0]["_source"]
        vector = doc.get(cfg.vector_field) or []
        print(
            f"sample {sample[0]['_index']}/{sample[0]['_id']}: {len(vector)} dims, fields {sorted(k for k in doc if k != cfg.vector_field)}"
        )
    if not ok:
        raise SystemExit(1)


def load_queries(path: str, limit: int) -> list[dict[str, Any]]:
    filesystem, local = _filesystem(path)
    table = pq.read_table(local, filesystem=filesystem)
    rows = table.slice(0, limit).to_pylist()
    out = []
    for row in rows:
        neighbours = (
            row["blob"]["neighbors"] if isinstance(row.get("blob"), dict) else row.get("neighbors")
        )
        out.append(
            {"id": row.get("id"), "vector": row["values"], "neighbors": list(neighbours or [])}
        )
    return out


def _recall(truth: list[str], got: list[str], k: int) -> float:
    return len(set(truth[:k]) & set(got[:k])) / k


def _present_ids(client: Any, pattern: str, ids: list[str]) -> set[str]:
    """Which ground-truth ids exist in the loaded indices (partial loads have few)."""
    hits = client.search(
        index=pattern,
        body={"size": len(ids), "_source": False, "query": {"ids": {"values": ids}}},
    )["hits"]["hits"]
    return {str(h["_id"]) for h in hits}


class PineconeTarget:
    """Dense top-k against a vector index (query API) or a document index (Documents API).

    A document index, which a hybrid migration creates, is searched with one ``dense_vector``
    ``score_by`` clause on ``pinecone.dense_field`` (default ``dense``, pcmigrate's default);
    a vector index reports its dense field as ``_values`` and has no such field.
    """

    def __init__(self, cfg: Config) -> None:
        from pinecone import Pinecone

        api_key = os.environ.get(str(cfg.get("pinecone.api_key_env", "PINECONE_API_KEY")))
        if not api_key:
            raise SystemExit("set the Pinecone API key environment variable")
        name = str(cfg.get("pinecone.index"))
        pc = Pinecone(api_key=api_key)
        self.index: Any = pc.Index(name)
        self.dense_field = str(cfg.get("pinecone.dense_field", "dense"))
        schema = getattr(pc.describe_index(name), "schema", None)
        fields = getattr(schema, "fields", None) or {}
        self.documents = self.dense_field in fields

    def kind(self) -> str:
        return "document index (Documents API)" if self.documents else "vector index (query API)"

    def top(self, namespaces: list[str], vector: list[float], top: int) -> list[str]:
        merged: list[tuple[float, str]] = []
        for namespace in namespaces:
            if self.documents:
                result = self.index.documents.search(
                    namespace=namespace,
                    score_by=[
                        {"type": "dense_vector", "field": self.dense_field, "values": vector}
                    ],
                    top_k=top,
                )
            else:
                result = self.index.query(
                    namespace=namespace, vector=vector, top_k=top, include_values=False
                )
            for match in result.matches:
                score = float(match.score) if match.score is not None else 0.0
                merged.append((score, str(match.id).rsplit("#", 1)[-1]))
        merged.sort(key=lambda m: -m[0])
        return [i for _, i in merged[:top]]


def _recall_scope(cfg: Config) -> list[tuple[str, str, list[str]]]:
    """(label, queries file, indices the ground truth covers), one scope per queries file."""
    files = data_files(cfg)
    by_queries: dict[str, list[str]] = {}
    for item in files:
        if item.queries:
            by_queries.setdefault(item.queries, []).append(item.index)
    if not by_queries:
        raise SystemExit("no queries file: set data.queries or data.sets[].queries")
    return [
        (", ".join(indices) if len(indices) > 1 else indices[0], path, indices)
        for path, indices in by_queries.items()
    ]


def cmd_recall(cfg: Config, args: argparse.Namespace) -> None:
    ks = [int(k) for k in args.k.split(",")]
    top = max(ks)
    os_client = opensearch_client(cfg)
    pinecone_target = PineconeTarget(cfg) if args.target == "pinecone" else None
    if pinecone_target is not None:
        print(f"pinecone target: {cfg.get('pinecone.index')} is a {pinecone_target.kind()}")
    for label, queries_path, indices in _recall_scope(cfg):
        queries = load_queries(queries_path, args.queries)
        pattern = ",".join(indices)
        namespaces = list(cfg.get("pinecone.namespaces") or indices)
        totals = dict.fromkeys(ks, 0.0)
        covered = dict.fromkeys(ks, 0.0)
        covered_n = dict.fromkeys(ks, 0)
        coverage = 0.0
        latencies: list[float] = []
        for q in queries:
            started = time.monotonic()
            if pinecone_target is not None:
                got = pinecone_target.top(namespaces, q["vector"], top)
            else:
                hits = os_client.search(
                    index=pattern,
                    body={
                        "size": top,
                        "_source": False,
                        "query": {"knn": {cfg.vector_field: {"vector": q["vector"], "k": top}}},
                    },
                )["hits"]["hits"]
                got = [str(h["_id"]) for h in hits]
            latencies.append(time.monotonic() - started)
            present = _present_ids(os_client, pattern, q["neighbors"])
            coverage += len(present) / max(1, len(q["neighbors"]))
            loaded_truth = [n for n in q["neighbors"] if n in present]
            for k in ks:
                totals[k] += _recall(q["neighbors"], got, k)
                if len(loaded_truth) >= k:
                    covered[k] += _recall(loaded_truth, got, k)
                    covered_n[k] += 1
        n = len(queries)
        latencies.sort()
        print(
            f"[{label}] target={args.target} queries={n} "
            f"ground-truth coverage={coverage / n:.1%} of neighbours are loaded"
        )
        for k in ks:
            adjusted = (
                f"{covered[k] / covered_n[k]:.4f} over {covered_n[k]} queries"
                if covered_n[k]
                else "n/a"
            )
            print(
                f"  recall@{k}: {totals[k] / n:.4f} against the full ground truth; "
                f"{adjusted} among loaded neighbours"
            )
        p50 = latencies[n // 2] * 1000
        p95 = latencies[max(0, int(n * 0.95) - 1)] * 1000
        print(f"  latency p50={p50:.0f}ms p95={p95:.0f}ms per query")


def cmd_plan(cfg: Config, args: argparse.Namespace) -> None:
    files = data_files(cfg)
    total = sum(f.rows for f in files)
    per_doc = cfg.dimension * 11 + 1500
    print(f"{len(files)} files -> {len(files)} indices, {total:,} documents, {cfg.dimension} dims")
    for f in files:
        queries = f" queries={Path(f.queries).name}" if f.queries else ""
        print(f"  {f.index:<24} {f.rows:>10,} rows  {Path(f.path).name}{queries}")
    print(
        f"estimated bulk JSON: {total * per_doc / 1e9:.1f} GB uncompressed (~{total * per_doc * 0.45 / 1e9:.1f} GB gzip on the wire)"
    )
    print(f"estimated index size: {total * (cfg.dimension * 4 * 1.2 + 2000) / 1e9:.1f} GB per copy")
    print(
        f"domain: {cfg.get('domain.instance_count', 3)}x {cfg.get('domain.instance_type', 'r6g.4xlarge.search')}, {cfg.get('domain.ebs_volume_gb', 500)} GB EBS each"
    )


# ----------------------------------------------------------------------------- main


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--config", required=True, help="YAML config (see config.example.yaml)")
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("plan")
    create = sub.add_parser("create-domain")
    create.add_argument(
        "--timeout", default=3600, type=float, help="seconds to wait for the endpoint"
    )
    indices = sub.add_parser("create-indices")
    indices.add_argument(
        "--recreate", action="store_true", help="drop and recreate existing indices"
    )
    load = sub.add_parser("load")
    load.add_argument("--workers", type=int, default=None)
    sub.add_parser("finalize")
    sub.add_parser("verify")
    recall = sub.add_parser("recall")
    recall.add_argument("--target", choices=["opensearch", "pinecone"], default="opensearch")
    recall.add_argument("--queries", type=int, default=200)
    recall.add_argument("--k", default="10,100", help="comma-separated k values")
    sub.add_parser("status")
    delete = sub.add_parser("delete-domain")
    delete.add_argument("--yes", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )
    if not args.verbose:
        logging.getLogger("opensearch").setLevel(logging.WARNING)
        logging.getLogger("botocore").setLevel(logging.WARNING)
    cfg = Config.load(args.config)
    commands = {
        "plan": cmd_plan,
        "create-domain": cmd_create_domain,
        "create-indices": cmd_create_indices,
        "load": cmd_load,
        "finalize": finalize_indices,
        "verify": cmd_verify,
        "recall": cmd_recall,
        "status": cmd_status,
        "delete-domain": cmd_delete_domain,
    }
    commands[args.command](cfg, args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
