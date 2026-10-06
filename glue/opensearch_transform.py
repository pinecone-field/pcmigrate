"""Glue PySpark job: raw OSIS NDJSON to canonical Parquet via pcmigrate.core.canonical (ADR-009).

Each Spark partition of raw lines runs ``transform_lines`` from the pcmigrate wheel, which
streams canonical Parquet parts and reject lines straight to S3 through the same writers
the runner uses. The driver folds the per-partition results into one JSON document at
``--result_uri`` for the export stage to read. The script also runs outside Glue with plain
argparse and a local SparkSession, which is how its test exercises it.
"""

from __future__ import annotations

import argparse
import functools
import json
import logging
import sys
from collections.abc import Iterable, Iterator
from typing import Any
from urllib.parse import urlparse

ARGUMENTS = (
    "mapping_uri",
    "index",
    "raw_uri",
    "bucket",
    "canonical_prefix",
    "rejects_prefix",
    "result_uri",
    "part_bytes",
    "region",
    "min_partitions",
)
log = logging.getLogger("pcmigrate.glue")


def parse_args(argv: list[str]) -> dict[str, str]:
    """Glue's getResolvedOptions when available, else argparse for local runs."""
    try:
        from awsglue.utils import getResolvedOptions

        return dict(getResolvedOptions(argv, ["JOB_NAME", *ARGUMENTS]))
    except ImportError:
        parser = argparse.ArgumentParser()
        parser.add_argument("--JOB_NAME", default="local")
        for name in ARGUMENTS:
            parser.add_argument(f"--{name}", required=name not in ("min_partitions",))
        parsed = parser.parse_args(argv[1:])
        return {k: v for k, v in vars(parsed).items() if v is not None}


def read_s3_text(uri: str, region: str) -> str:
    """Read one small S3 object."""
    import boto3

    parsed = urlparse(uri)
    body = boto3.client("s3", region_name=region).get_object(
        Bucket=parsed.netloc, Key=parsed.path.lstrip("/")
    )["Body"]
    return str(body.read().decode("utf-8"))


def write_s3_text(uri: str, region: str, text: str) -> None:
    """Write one small S3 object."""
    import boto3

    parsed = urlparse(uri)
    boto3.client("s3", region_name=region).put_object(
        Bucket=parsed.netloc, Key=parsed.path.lstrip("/"), Body=text.encode("utf-8")
    )


def transform_partition(
    pid: int,
    lines: Iterable[str],
    *,
    mapping_yaml: str,
    index: str,
    bucket: str,
    canonical_prefix: str,
    rejects_prefix: str,
    part_bytes: int,
    region: str,
) -> Iterator[dict[str, Any]]:
    """Run on an executor: one Spark partition of raw lines to canonical parts and rejects."""
    import boto3

    from pcmigrate.config.mapping import Mapping
    from pcmigrate.sources.opensearch.glue_mapping import DocumentTranslator, transform_lines

    translator = DocumentTranslator.for_index(Mapping.from_yaml(mapping_yaml), index)
    result = transform_lines(
        lines,
        translator=translator,
        s3=boto3.client("s3", region_name=region),
        bucket=bucket,
        canonical_prefix=f"{canonical_prefix.rstrip('/')}/p{pid:05d}",
        rejects_prefix=f"{rejects_prefix.rstrip('/')}/p{pid:05d}",
        target_part_bytes=part_bytes,
    )
    yield result.to_dict()


def run(spark: Any, args: dict[str, str]) -> dict[str, Any]:
    """Transform every raw object under ``raw_uri`` and return the folded result."""
    from pcmigrate.sources.opensearch.glue_mapping import PartitionResult, merge_results

    region = args["region"]
    mapping_yaml = read_s3_text(args["mapping_uri"], region)
    lines = spark.read.text(args["raw_uri"]).rdd.map(lambda row: row[0])
    minimum = int(args.get("min_partitions") or 0)
    if minimum and lines.getNumPartitions() < minimum:
        lines = lines.repartition(minimum)
    worker = functools.partial(
        transform_partition,
        mapping_yaml=mapping_yaml,
        index=args["index"],
        bucket=args["bucket"],
        canonical_prefix=args["canonical_prefix"],
        rejects_prefix=args["rejects_prefix"],
        part_bytes=int(args["part_bytes"]),
        region=region,
    )
    results = [PartitionResult.from_dict(r) for r in lines.mapPartitionsWithIndex(worker).collect()]
    merged = merge_results(results)
    summary = {
        "index": args["index"],
        "partitions": len(results),
        "namespaces": {ns: r.to_dict() for ns, r in merged.items()},
        "rows": sum(r.rows for r in merged.values()),
        "rejected": sum(r.rejected for r in merged.values()),
    }
    write_s3_text(args["result_uri"], region, json.dumps(summary, indent=2) + "\n")
    log.info("transformed %s rows, rejected %s", summary["rows"], summary["rejected"])
    return summary


def main(argv: list[str] | None = None) -> int:
    """Entrypoint for Glue and for local runs.

    Glue records a run as FAILED when the script raises ``SystemExit``, even with code 0,
    so ``__main__`` calls this directly instead of ``sys.exit(main())``.
    """
    logging.basicConfig(level=logging.INFO)
    args = parse_args(list(sys.argv if argv is None else argv))
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName(f"pcmigrate-{args['index']}").getOrCreate()
    try:
        from awsglue.context import GlueContext
        from awsglue.job import Job

        job = Job(GlueContext(spark.sparkContext))
        job.init(args["JOB_NAME"], args)
    except ImportError:
        job = None
    run(spark, args)
    if job is not None:
        job.commit()
    return 0


if __name__ == "__main__":
    main()
