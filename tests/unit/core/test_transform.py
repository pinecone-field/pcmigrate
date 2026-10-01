import json

from mypy_boto3_s3 import S3Client

from pcmigrate.config.mapping import DenseSpec, MetadataField, MetadataType, Metric, ObjectMapping
from pcmigrate.core.canonical import CanonicalRecord, CanonicalWriter, read_canonical
from pcmigrate.core.transform import RecordValidator, read_duplicates, transform_namespace
from tests.unit.conftest import BUCKET

OBJ = ObjectMapping(
    source="public.documents",
    namespace="documents",
    id_template="documents#{id}",
    dense=DenseSpec(column="embedding", dimension=4, metric=Metric.COSINE),
    metadata=[
        MetadataField(source="title", type=MetadataType.STRING),
        MetadataField(source="views", type=MetadataType.INTEGER),
        MetadataField(source="tags", type=MetadataType.STRING_LIST),
    ],
)


def rec(i: int, dense: list[float] | None = None, **metadata: object) -> CanonicalRecord:
    return CanonicalRecord(
        id=f"documents#{i}",
        namespace="documents",
        source_pk=str(i),
        dense=dense if dense is not None else [float(i), 1.0, 0.0, 0.0],
        metadata=metadata or {"title": f"t{i}", "views": i},
    )


def test_validator_rules() -> None:
    v = RecordValidator(OBJ)
    assert v.violations(rec(1)) == []
    assert "dimension 3 != 4" in v.violations(rec(1, dense=[1.0, 2.0, 3.0]))[0]
    assert any("NaN" in p for p in v.violations(rec(1, dense=[float("nan"), 0, 0, 0])))
    assert any("reserved" in p for p in v.violations(rec(1, _hidden="x")))
    assert any("non-scalar" in p for p in v.violations(rec(1, nested={"a": 1})))
    assert any("non-string list" in p for p in v.violations(rec(1, tags=[1, 2])))
    assert any("mapping says integer" in p for p in v.violations(rec(1, views="3")))
    assert any("boolean, mapping says integer" in p for p in v.violations(rec(1, views=True)))
    assert any("over the 40960-byte" in p for p in v.violations(rec(1, title="x" * 50_000)))
    assert any(
        "512 characters" in p
        for p in v.violations(
            CanonicalRecord(
                id="x" * 600, namespace="documents", source_pk="1", dense=[1.0, 0, 0, 0]
            )
        )
    )


def _seed(s3: S3Client, records: list[CanonicalRecord]) -> list[str]:
    writer = CanonicalWriter(
        s3, BUCKET, "mig/canonical/public.documents/p0000", target_part_bytes=10_000, batch_rows=16
    )
    for r in records:
        writer.write(r)
    return [p.key for p in writer.close()]


def test_transform_splits_clean_rejects_duplicates(s3: S3Client) -> None:
    records = [rec(i) for i in range(50)]
    records.append(rec(100, dense=[700.0, 1.0, 0.0, 0.0]))
    records.append(rec(101, dense=[700.0, 1.0, 0.0, 0.0]))
    records.append(rec(102, dense=[700.0, 1.0, 0.0, 0.0]))
    records.append(rec(200, dense=[1.0, 2.0, 3.0]))
    records.append(rec(201, title="ok", views="bad"))
    keys = _seed(s3, records)
    result = transform_namespace(
        s3,
        BUCKET,
        namespace="documents",
        obj=OBJ,
        canonical_keys=keys,
        clean_prefix="mig/canonical/_clean/documents",
        rejects_prefix="mig/rejects/documents",
        duplicates_prefix="mig/manifest/duplicates/documents",
        drop_duplicates=False,
        target_part_bytes=10_000,
    )
    assert result.clean_rows == 51 and result.duplicate_rows == 2 and result.rejected_rows == 2
    clean_ids = [r.id for r in read_canonical(s3, BUCKET, result.clean_parts)]
    assert "documents#100" in clean_ids and "documents#101" not in clean_ids
    dups = list(read_duplicates(s3, BUCKET, result.duplicate_parts))
    assert sorted(d.id for d in dups) == ["documents#101", "documents#102"]
    assert dups[0].dense == [700.0, 1.0, 0.0, 0.0] and dups[0].metadata["title"] == "t101"
    rejects = [
        json.loads(line)
        for key in result.reject_parts
        for line in s3.get_object(Bucket=BUCKET, Key=key)["Body"].read().decode().splitlines()
    ]
    reasons = {r["id"]: r["reason"] for r in rejects}
    assert "dimension 3 != 4" in reasons["documents#200"]
    assert "mapping says integer" in reasons["documents#201"]
    assert result.reject_reasons


def test_transform_drop_mode_rejects_duplicates(s3: S3Client) -> None:
    keys = _seed(s3, [rec(1, dense=[1.0, 1.0, 0.0, 0.0]), rec(2, dense=[1.0, 1.0, 0.0, 0.0])])
    result = transform_namespace(
        s3,
        BUCKET,
        namespace="documents",
        obj=OBJ,
        canonical_keys=keys,
        clean_prefix="mig/c",
        rejects_prefix="mig/r",
        duplicates_prefix="mig/d",
        drop_duplicates=True,
        target_part_bytes=10_000,
    )
    assert result.clean_rows == 1 and result.rejected_rows == 1 and result.duplicate_rows == 0
    assert result.reject_reasons == {"duplicate vector": 1}
