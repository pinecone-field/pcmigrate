"""The Glue script's partition function and argument parsing, without pyspark."""

import importlib.util
import json
import sys
from pathlib import Path

from mypy_boto3_s3 import S3Client

from pcmigrate.config.mapping import Mapping
from pcmigrate.core.canonical import read_canonical
from pcmigrate.report.build import build_report
from pcmigrate.report.mapping_gen import MappingOptions
from pcmigrate.sources.opensearch.introspect import IntrospectOptions, OpenSearchIntrospector
from tests.fixtures.golden.golden_paths import REPO_ROOT
from tests.fixtures.golden.opensearch.documents import golden_documents
from tests.unit.conftest import BUCKET, REGION
from tests.unit.sources.opensearch.fakes import FakeTransport, make_client


def _load_script():  # type: ignore[no-untyped-def]
    path = REPO_ROOT / "glue" / "opensearch_transform.py"
    spec = importlib.util.spec_from_file_location("opensearch_transform", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _mapping() -> Mapping:
    introspector = OpenSearchIntrospector(
        make_client(FakeTransport.golden()), IntrospectOptions(workload_window_s=0, sample_docs=100)
    )
    return build_report(
        introspector.inventory(), MappingOptions(migration_id="m", index="i", region=REGION)
    ).mapping


class FakeRdd:
    def __init__(self, partitions: list[list[str]]) -> None:
        self.partitions = partitions

    def map(self, fn):  # type: ignore[no-untyped-def]
        return FakeRdd([[fn((line,)) for line in p] for p in self.partitions])

    def getNumPartitions(self) -> int:  # noqa: N802
        return len(self.partitions)

    def repartition(self, n: int) -> "FakeRdd":
        flat = [line for p in self.partitions for line in p]
        return FakeRdd([flat[i::n] for i in range(n)])

    def mapPartitionsWithIndex(self, fn):  # type: ignore[no-untyped-def]  # noqa: N802
        return FakeRdd([list(fn(i, iter(p))) for i, p in enumerate(self.partitions)])

    def collect(self):  # type: ignore[no-untyped-def]
        return [item for p in self.partitions for item in p]


class FakeSpark:
    def __init__(self, partitions: list[list[str]]) -> None:
        self._rdd = FakeRdd(partitions)
        self.read = self

    def text(self, path: str) -> "FakeSpark":
        self.path = path
        return self

    @property
    def rdd(self) -> FakeRdd:
        return self._rdd


def test_parse_args_without_awsglue(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setitem(sys.modules, "awsglue", None)
    monkeypatch.setitem(sys.modules, "awsglue.utils", None)
    script = _load_script()
    args = script.parse_args(
        [
            "job.py",
            "--mapping_uri",
            "s3://b/m.yaml",
            "--index",
            "i",
            "--raw_uri",
            "s3://b/raw/",
            "--bucket",
            "b",
            "--canonical_prefix",
            "m/canonical/i",
            "--rejects_prefix",
            "m/rejects/i/glue",
            "--result_uri",
            "s3://b/r.json",
            "--part_bytes",
            "1024",
            "--region",
            REGION,
        ]
    )
    assert args["index"] == "i" and args["JOB_NAME"] == "local" and "min_partitions" not in args


def test_run_transforms_raw_lines_into_canonical_parts(s3: S3Client) -> None:
    script = _load_script()
    mapping = _mapping()
    s3.put_object(
        Bucket=BUCKET, Key="m/manifest/glue/mapping.yaml", Body=mapping.to_yaml().encode()
    )
    docs = golden_documents()[:30]
    lines = [json.dumps({**d, "_id": d["doc_id"], "_index": "golden-documents"}) for d in docs]
    lines[5] = json.dumps({"_id": "bad", "title": "no vector"})
    spark = FakeSpark([lines[:10], lines[10:20], lines[20:]])
    args = {
        "mapping_uri": f"s3://{BUCKET}/m/manifest/glue/mapping.yaml",
        "index": "golden-documents",
        "raw_uri": f"s3://{BUCKET}/m/raw/golden-documents/",
        "bucket": BUCKET,
        "canonical_prefix": "m/canonical/golden-documents",
        "rejects_prefix": "m/rejects/golden-documents/glue",
        "result_uri": f"s3://{BUCKET}/m/manifest/glue/golden-documents/result.json",
        "part_bytes": str(8 * 1024 * 1024),
        "region": REGION,
        "min_partitions": "2",
    }
    summary = script.run(spark, args)
    assert summary["rows"] == 29 and summary["rejected"] == 1 and summary["partitions"] == 3
    stored = json.loads(
        s3.get_object(Bucket=BUCKET, Key="m/manifest/glue/golden-documents/result.json")[
            "Body"
        ].read()
    )
    assert stored == summary
    ns = stored["namespaces"]["golden-documents"]
    assert ns["parts"] == [
        "m/canonical/golden-documents/p00000/part-00000.parquet",
        "m/canonical/golden-documents/p00001/part-00000.parquet",
        "m/canonical/golden-documents/p00002/part-00000.parquet",
    ]
    assert ns["reject_parts"] == ["m/rejects/golden-documents/glue/p00000/part-00000.jsonl"]
    ids = {r.id for r in read_canonical(s3, BUCKET, ns["parts"])}
    assert len(ids) == 29 and "golden-documents#doc-00006" not in ids
    assert Path(script.__file__).name == "opensearch_transform.py"
