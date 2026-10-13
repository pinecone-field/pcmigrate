"""FTS wave (ADR-018): mapping models, mapping generation, text carried end to end, the
document schema with full-text fields, and the per-class overlap check with fakes."""

from __future__ import annotations

import gzip
import io
import json
from dataclasses import replace
from pathlib import Path

import pytest
from mypy_boto3_s3 import S3Client

from pcmigrate.config import Mapping, MigrationConfig
from pcmigrate.config.mapping import (
    DenseSpec,
    FullTextCheck,
    FullTextSearchSpec,
    MetadataField,
    MetadataType,
    Metric,
    NgramSpec,
    ObjectMapping,
    SparseKind,
    SparseModelSpec,
    SparseSpec,
    TargetSpec,
    TextFieldSpec,
)
from pcmigrate.core.buffer import ChangeEnvelope
from pcmigrate.core.canonical import CanonicalRecord, CanonicalWriter, read_canonical
from pcmigrate.core.events import EventBus, JsonLinesSink
from pcmigrate.core.stages import Pipeline, RunContext
from pcmigrate.core.stages.emit import EmitStage
from pcmigrate.core.stages.export import ExportStage
from pcmigrate.core.stages.import_ import ImportStage
from pcmigrate.core.stages.prepare_index import PrepareIndexStage
from pcmigrate.core.stages.transform import TransformStage
from pcmigrate.core.stages.validate import ValidateStage
from pcmigrate.core.state import Manifest, ManifestStore, S3Lock, Stage
from pcmigrate.core.transform import RecordValidator, SparseEnricher
from pcmigrate.report.build import build_report
from pcmigrate.report.mapping_gen import MappingOptions, generate_mapping
from pcmigrate.sources.base import TextQuery
from pcmigrate.sources.inventory import IndexInfo, TextSearchField
from pcmigrate.sources.pgvector.records import RecordBuilder
from pcmigrate.sources.synthetic import SyntheticSource
from pcmigrate.targets.applier import Applier, vector_record
from pcmigrate.targets.emitters import DocumentFields, DocumentIndexEmitter, ImportLimits
from pcmigrate.targets.emitters.document_jsonl import document_line
from pcmigrate.targets.emitters.vector_parquet import ImportBatch, NamespaceInput
from pcmigrate.targets.index_manager import IndexManager, IndexSpecMismatchError
from pcmigrate.validate.bm25 import Analyzer, Bm25Index
from pcmigrate.validate.fts import build_queries, check_full_text, pinecone_request
from pcmigrate.writepath.shim import BufferedIndex
from tests.unit.conftest import BUCKET, REGION, approve_ctx
from tests.unit.fakes import FakeGateway, FakeSparseEncoder
from tests.unit.report.fixtures import column, documents_table, inventory

ARN = "arn:aws:secretsmanager:us-east-1:111122223333:secret:pinecone"
OPTIONS = MappingOptions(migration_id="acme", index="acme-docs", region="us-east-1")


def test_full_text_search_spec_rules() -> None:
    assert FullTextSearchSpec().to_schema() == {
        "language": "en",
        "stemming": False,
        "stop_words": False,
    }
    english = FullTextSearchSpec(language="en", stemming=True, stop_words=True)
    assert english.to_schema()["stop_words"] is True
    ngram = FullTextSearchSpec(ngram=NgramSpec(min_gram=2, max_gram=3, prefix_only=True))
    assert ngram.to_schema() == {
        "language": "en",
        "ngram": {"min_gram": 2, "max_gram": 3, "prefix_only": True},
    }
    with pytest.raises(ValueError, match="requires stemming"):
        FullTextSearchSpec(stop_words=True)
    with pytest.raises(ValueError, match="cannot be combined"):
        FullTextSearchSpec(stemming=True, ngram=NgramSpec(min_gram=2, max_gram=3))
    with pytest.raises(ValueError, match="language"):
        FullTextSearchSpec(language="xx")
    with pytest.raises(ValueError, match="max_gram"):
        NgramSpec(min_gram=3, max_gram=2)


def _object(**overrides: object) -> ObjectMapping:
    base = {
        "source": "public.documents",
        "namespace": "documents",
        "id_template": "documents#{id}",
        "dense": DenseSpec(column="embedding", dimension=4, metric=Metric.COSINE),
        "text": [
            TextFieldSpec(
                source="body",
                full_text_search=FullTextSearchSpec(language="en", stemming=True, stop_words=True),
                derived_from="body_tsv",
                analyzer="english",
            ),
            TextFieldSpec(source="summary", target="abstract"),
        ],
        "metadata": [MetadataField(source="title", type=MetadataType.STRING)],
    }
    base.update(overrides)
    return ObjectMapping.model_validate(base)


def test_mapping_rules_for_text_fields() -> None:
    obj = _object()
    assert obj.text_sources == {"body": "body", "summary": "abstract"}
    assert obj.text_columns == ["body", "summary"]
    with pytest.raises(ValueError, match="searchable or filterable"):
        _object(metadata=[MetadataField(source="body", type=MetadataType.STRING)])
    with pytest.raises(ValueError, match="several text fields"):
        _object(text=[TextFieldSpec(source="a", target="x"), TextFieldSpec(source="b", target="x")])
    with pytest.raises(ValueError, match=r"target\.type: document"):
        Mapping(
            migration_id="m",
            source={"kind": "pgvector"},  # type: ignore[arg-type]
            target=TargetSpec(index="i", region="us-east-1"),
            objects=[obj],
        )
    mapping = Mapping(
        migration_id="m",
        source={"kind": "pgvector"},  # type: ignore[arg-type]
        target=TargetSpec(index="i", region="us-east-1", type="document"),
        objects=[obj],
        validation={"full_text": {"queries": 20}},  # type: ignore[arg-type]
    )
    assert mapping.full_text and not mapping.hybrid
    assert mapping.text_fields["abstract"] == FullTextSearchSpec()
    assert Mapping.from_yaml(mapping.to_yaml()) == mapping
    assert mapping.validation.full_text is not None
    assert mapping.validation.full_text.thresholds.for_class("phrase") == 0.8
    other = obj.model_copy(
        update={
            "source": "public.other",
            "namespace": "other",
            "text": [TextFieldSpec(source="body", full_text_search=FullTextSearchSpec())],
        }
    )
    with pytest.raises(ValueError, match="different full_text_search settings"):
        Mapping(
            migration_id="m",
            source={"kind": "pgvector"},  # type: ignore[arg-type]
            target=TargetSpec(index="i", region="us-east-1", type="document"),
            objects=[obj, other],
        )
    with pytest.raises(ValueError, match="collides with a vector field"):
        Mapping(
            migration_id="m",
            source={"kind": "pgvector"},  # type: ignore[arg-type]
            target=TargetSpec(index="i", region="us-east-1", type="document"),
            objects=[
                obj.model_copy(update={"text": [TextFieldSpec(source="body", target="dense")]})
            ],
        )
    # generated sparse plus FTS on the same column: text columns are listed once
    both = obj.model_copy(
        update={
            "sparse": SparseSpec(
                kind=SparseKind.GENERATED,
                text_fields=["title", "body"],
                model=SparseModelSpec(),
            )
        }
    )
    assert both.text_columns == ["body", "summary", "title"]


def _fts_table() -> object:
    table = documents_table(
        columns=[*documents_table().columns, column("summary", "text", avg_width=60)],
        text_search=[
            TextSearchField(
                column="body_tsv",
                gin_index="documents_body_tsv_idx",
                expression="to_tsvector('english'::regconfig, COALESCE(body, ''::text))",
                config="english",
                source_columns=["body"],
                dictionaries=["english_stem:snowball", "simple:simple"],
            ),
            TextSearchField(
                column="documents_summary_fts_idx",
                kind="tsvector_index",
                gin_index="documents_summary_fts_idx",
                expression="to_tsvector('simple'::regconfig, summary)",
                config="simple",
                source_columns=["summary"],
            ),
        ],
        indexes=[
            *documents_table().indexes,
            IndexInfo(
                name="documents_summary_fts_idx",
                method="gin",
                columns=[],
                opclasses=["tsvector_ops"],
                definition="CREATE INDEX documents_summary_fts_idx ON public.documents USING gin (to_tsvector('simple'::regconfig, summary))",
            ),
        ],
    )
    return table


def test_generated_mapping_and_report_for_the_fts_roadmap() -> None:
    table = _fts_table()
    dense = generate_mapping(inventory(table), OPTIONS)  # type: ignore[arg-type]
    assert dense.objects[0].text == [] and dense.target.type == "vector"
    assert any(f.source == "body" and f.action == "map" for f in dense.objects[0].metadata)

    pg = inventory(table).model_copy(  # type: ignore[arg-type]
        update={"cluster": {"default_text_search_config": "pg_catalog.english"}}
    )
    fts = generate_mapping(pg, replace(OPTIONS, roadmap="fts"))
    obj = fts.objects[0]
    assert fts.target.type == "document" and fts.full_text and not fts.hybrid
    assert [(t.source, t.field_name) for t in obj.text] == [
        ("body", "body"),
        ("summary", "summary"),
    ]
    assert obj.text[0].full_text_search == FullTextSearchSpec(
        language="en", stemming=True, stop_words=True
    )
    assert obj.text[0].derived_from == "body_tsv" and obj.text[0].analyzer == "english"
    assert obj.text[1].full_text_search == FullTextSearchSpec()
    assert obj.text[1].derived_from == "documents_summary_fts_idx"
    assert not any(f.source in ("body", "summary") and f.action == "map" for f in obj.metadata)
    dropped = next(f for f in obj.metadata if f.source == "body_tsv")
    assert dropped.action == "drop" and "carried as a full-text field" in (dropped.reason or "")
    assert fts.validation.full_text == FullTextCheck()

    bundle = build_report(pg, replace(OPTIONS, roadmap="fts"))
    checks = {f.check for f in bundle.findings}
    assert {"fts_fields", "fts_validation"} <= checks
    assert "Full-text field" in bundle.report_md and "ts_rank_cd" in bundle.report_md
    assert "Analyzer gaps" in bundle.report_md
    assert "full_text_search" in bundle.report_md

    # --fts-from carries a plain text column with the server default configuration
    extra = generate_mapping(pg, replace(OPTIONS, fts_from=("title",)))
    assert [t.source for t in extra.objects[0].text] == ["body", "summary", "title"]
    assert extra.objects[0].text[2].full_text_search.stemming is True
    assert not any(f.source == "title" and f.action == "map" for f in extra.objects[0].metadata)


def test_generated_mapping_for_opensearch_text_fields_and_multi_fields() -> None:
    index = documents_table(
        schema_name="",
        name="docs",
        relkind="i",
        columns=[
            column("doc_id", "keyword"),
            column("title", "text", analyzer="english_stem"),
            column("body", "text"),
            column("category", "keyword"),
        ],
        text_search=[
            TextSearchField(
                column="title",
                kind="text",
                analyzer="english_stem",
                multi_fields={"raw": "keyword"},
            ),
            TextSearchField(column="body", kind="text"),
        ],
        settings={
            "analysis": {
                "filter": {"english_stop": {"type": "stop", "stopwords": "_english_"}},
                "analyzer": {
                    "english_stem": {
                        "type": "custom",
                        "tokenizer": "standard",
                        "filter": ["lowercase", "porter_stem", "english_stop"],
                    }
                },
            }
        },
    )
    os_inventory = inventory(index).model_copy(update={"kind": "opensearch"})  # type: ignore[arg-type]
    mapping = generate_mapping(os_inventory, replace(OPTIONS, roadmap="fts"))
    obj = mapping.objects[0]
    assert {t.source: t.full_text_search for t in obj.text} == {
        "title": FullTextSearchSpec(language="en", stemming=True, stop_words=True),
        "body": FullTextSearchSpec(),
    }
    raw = next(f for f in obj.metadata if f.source == "title")
    assert raw.target == "title_raw" and raw.type is MetadataType.STRING
    assert not any(f.source == "body" for f in obj.metadata)
    bundle = build_report(os_inventory, replace(OPTIONS, roadmap="fts"))
    notes = [f for f in bundle.findings if f.check == "fts_translation"]
    assert any("title_raw" in f.message for f in notes)
    assert "the index's BM25" in bundle.report_md


def test_record_builder_carries_text_and_transform_keeps_it() -> None:
    obj = _object(
        sparse=SparseSpec(
            kind=SparseKind.GENERATED, text_fields=["title", "body"], model=SparseModelSpec()
        )
    )
    builder = RecordBuilder(obj, ("id",))
    assert builder.source_columns == ["id", "title", "embedding", "body", "summary"]
    record = builder.build(
        {
            "id": 1,
            "title": "Hello",
            "embedding": [0.1, 0.2, 0.3, 0.4],
            "body": "running rivers",
            "summary": None,
        }
    )
    assert record.text_fields == {"title": "Hello", "body": "running rivers"}
    assert record.metadata == {"title": "Hello"}
    enricher = SparseEnricher(obj.sparse, FakeSparseEncoder(), keep=frozenset(obj.text_sources))  # type: ignore[arg-type]
    assert enricher.enrich([record]) == {}
    assert record.sparse is not None and record.text_fields == {"body": "running rivers"}
    validator = RecordValidator(obj)
    assert validator.violations(record) == []
    clash = CanonicalRecord(
        id="x",
        namespace="documents",
        source_pk="x",
        dense=[0.1, 0.2, 0.3, 0.4],
        sparse=record.sparse,
        text_fields={"summary": "s"},
        metadata={"abstract": "also"},
    )
    assert any("searchable or filterable" in p for p in validator.violations(clash))
    capped = RecordBuilder(
        obj.model_copy(
            update={"text": [TextFieldSpec(source="body", max_chars=5)], "sparse": None}
        ),
        ("id",),
    ).build({"id": 2, "embedding": [0.1, 0.2, 0.3, 0.4], "body": "abcdefghij"})
    assert capped.text_fields == {"body": "abcde"}
    dotted = RecordBuilder(
        obj.model_copy(
            update={
                "text": [TextFieldSpec(source="attrs.abstract", target="abstract")],
                "sparse": None,
            }
        ),
        ("id",),
    )
    assert dotted.source_columns == ["id", "title", "embedding", "attrs"]
    deep = dotted.build(
        {"id": 3, "embedding": [0.1, 0.2, 0.3, 0.4], "attrs": '{"abstract": "from json"}'}
    )
    assert deep.text_fields == {"attrs.abstract": "from json"}


def test_document_line_emitter_and_data_plane_records(s3: S3Client) -> None:
    record = CanonicalRecord(
        id="doc-1",
        namespace="docs",
        source_pk="1",
        dense=[0.5, 0.25],
        text_fields={"body": "some text", "summary": "short", "ignored": "x"},
        metadata={"title": "T"},
    )
    fields = DocumentFields(text={"body": "body", "summary": "abstract"})
    assert document_line(record, fields) == {
        "_id": "doc-1",
        "dense": [0.5, 0.25],
        "body": "some text",
        "abstract": "short",
        "title": "T",
    }
    writer = CanonicalWriter(s3, BUCKET, "m/canonical/docs", target_part_bytes=8 * 1024 * 1024)
    writer.write(record)
    parts = writer.close()
    emitter = DocumentIndexEmitter(
        s3,
        BUCKET,
        "m/import",
        fields_by_namespace={"docs": fields},
        limits=ImportLimits(),
    )
    result = emitter.emit(
        ImportBatch(id="batch-00000", namespaces=("docs",), estimated_bytes=1, estimated_files=1),
        [NamespaceInput(namespace="docs", keys=tuple(p.key for p in parts), bytes=1)],
    )
    body = s3.get_object(Bucket=BUCKET, Key=result.parts[0].key)["Body"].read()
    lines = [json.loads(line) for line in gzip.decompress(body).decode().splitlines()]
    assert lines[0]["abstract"] == "short" and "ignored" not in lines[0]

    vector = vector_record(record, {"body": "body", "summary": "abstract"})
    assert vector.text_fields == {"body": "some text", "abstract": "short"}
    gw = FakeGateway()
    target = TargetSpec(index="acme-docs", region="us-east-1", type="document")
    dense = DenseSpec(column="embedding", dimension=2, metric=Metric.COSINE)
    text = {
        "body": FullTextSearchSpec(language="en", stemming=True, stop_words=True),
        "abstract": FullTextSearchSpec(),
    }
    info = IndexManager(gw).prepare(target, dense, ["docs"], text=text)
    assert info.schema is not None and info.schema.text_fields == ("body", "abstract")
    assert gw.create_calls[0]["fields"]["body"] == {
        "type": "string",
        "full_text_search": {"language": "en", "stemming": True, "stop_words": True},
    }
    host = info.host
    gw.upsert(host, "docs", [vector])
    assert gw.fetch(host, "docs", ["doc-1"])["doc-1"].text_fields["abstract"] == "short"
    assert [m.id for m in gw.query_text(host, "docs", "text", fields=["body"], top_k=5)] == [
        "doc-1"
    ]
    assert gw.query_text(host, "docs", "short", fields=["body"], top_k=5) == []
    assert (
        gw.query_text(
            host,
            "docs",
            "some",
            fields=["body"],
            top_k=5,
            filter={"body": {"$match_phrase": "text some"}},
        )
        == []
    )
    # adopt mode refuses a missing field or a different configuration
    adopt = target.model_copy(update={"mode": "adopt"})
    with pytest.raises(IndexSpecMismatchError, match="full-text field 'extra' is missing"):
        IndexManager(gw).prepare(
            adopt, dense, ["other"], text={**text, "extra": FullTextSearchSpec()}
        )
    with pytest.raises(IndexSpecMismatchError, match="full-text field 'body' differs"):
        IndexManager(gw).prepare(adopt, dense, ["other"], text={"body": FullTextSearchSpec()})
    IndexManager(gw).prepare(adopt, dense, ["other"], text=text)


def test_applier_and_dual_write_carry_text_fields() -> None:
    from pcmigrate.core.buffer import InMemoryBuffer

    gw = FakeGateway()
    target = TargetSpec(index="acme-docs", region="us-east-1", type="document")
    dense = DenseSpec(column="embedding", dimension=2, metric=Metric.COSINE)
    info = IndexManager(gw).prepare(target, dense, ["docs"], text={"body": FullTextSearchSpec()})
    buffer = InMemoryBuffer()
    BufferedIndex(buffer).upsert(
        vectors=[
            {
                "id": "a",
                "values": [0.1, 0.2],
                "metadata": {"t": 1},
                "text_fields": {"body": "hello world"},
            }
        ],
        namespace="docs",
    )
    applier = Applier(gw, info.host, buffer, text_fields={"docs": {"body": "body"}})
    assert applier.drain_once().upserts == 1
    assert gw.fetch(info.host, "docs", ["a"])["a"].text_fields == {"body": "hello world"}
    envelope = ChangeEnvelope.from_json(
        ChangeEnvelope(
            op="upsert", id="b", namespace="docs", source_change_id="x", text_fields={"body": "b"}
        ).to_json()
    )
    assert envelope.text_fields == {"body": "b"}


def test_bm25_ranker_and_query_builder() -> None:
    index = Bm25Index(Analyzer(stemming=False, stop_words=True))
    index.add("a", "the quick brown fox jumps over the lazy dog")
    index.add("b", "quick quick fox")
    index.add("c", "a slow turtle")
    assert [i for i, _ in index.rank("quick", 10)] == ["b", "a"]
    assert [i for i, _ in index.rank("quick fox", 10, mode="phrase")] == ["b"]
    assert [i for i, _ in index.rank("lazy quick", 10, mode="all")] == ["a"]
    assert index.rank("missing", 10) == []
    index.remove("b")
    assert [i for i, _ in index.rank("quick", 10)] == ["a"]
    grams = Analyzer(ngram=(2, 3, True)).tokens("abcd")
    assert grams == ["ab", "abc"]

    records = [
        CanonicalRecord(
            id=f"d{n}",
            namespace="docs",
            source_pk=str(n),
            dense=[1.0],
            text_fields={"body": text},
            metadata={"category": "news" if n % 2 else "blog"},
        )
        for n, text in enumerate(
            [
                "harbor anchor rudder harbor voyage zephyrwren",
                "harbor anchor mast lighthouse",
                "compass tide keel voyage emberlark",
            ]
        )
    ]
    queries = build_queries(records, "body", "body", filters=[{"category": {"$eq": "news"}}])
    assert [q.text for q in queries["single_term"]] == ["rudder", "lighthouse", "compass"]
    assert set(queries["multi_term"][0].text.split()) == {"rudder", "zephyrwren", "harbor"}
    assert queries["phrase"][0] == TextQuery("body", "harbor anchor", mode="phrase", source="body")
    assert queries["filtered"][0].filter == {"category": {"$eq": "news"}}
    assert pinecone_request(queries["phrase"][0]) == (
        "harbor anchor",
        {"body": {"$match_phrase": "harbor anchor"}},
    )
    assert pinecone_request(queries["single_term"][0]) == ("rudder", None)


def _fts_mapping() -> Mapping:
    return Mapping.model_validate(
        {
            "migration_id": "fts",
            "source": {"kind": "synthetic"},
            "target": {"index": "fts", "region": REGION, "type": "document"},
            "objects": [
                {
                    "source": "synthetic.documents",
                    "namespace": "documents",
                    "id_template": "documents#{id}",
                    "dense": {"column": "embedding", "dimension": 16, "metric": "cosine"},
                    "text": [
                        {"source": "title", "full_text_search": {"language": "en"}},
                        {"source": "body", "target": "content"},
                    ],
                }
            ],
            "validation": {
                "full_text": {"k": 10, "queries": 20},
                "fidelity_sample": 50,
                "recall_at_k": {"k": 10, "queries": 20},
            },
        }
    )


def test_fts_synthetic_pipeline_end_to_end_with_fakes(s3: S3Client, tmp_path: Path) -> None:
    mapping = _fts_mapping()
    source = SyntheticSource(rows=300, dimension=16, partitions=2, text_words=24)
    gw = FakeGateway(import_records=300)
    cfg = MigrationConfig(
        migration_id="fts",
        bucket=BUCKET,
        aws_region=REGION,
        pinecone_api_key_secret=ARN,
        canonical_part_bytes=8 * 1024 * 1024,
    )
    store = ManifestStore(s3, BUCKET, "fts")
    ctx = RunContext(
        config=cfg,
        mapping=mapping,
        manifest=Manifest(migration_id="fts"),
        store=store,
        events=EventBus("fts", [JsonLinesSink(io.StringIO())]),
        s3=s3,
        source=source,
        target=gw,
    )
    approve_ctx(ctx, tmp_path)
    Pipeline(
        [
            PrepareIndexStage(),
            ExportStage(use_processes=False),
            TransformStage(),
            EmitStage(ImportLimits()),
            ImportStage(poll_interval_s=0),
        ]
    ).run(ctx, S3Lock(s3, BUCKET, "fts"))
    prepared = ctx.manifest.stage(Stage.PREPARE_INDEX).outputs
    assert prepared["type"] == "document" and prepared["text_fields"] == ["title", "content"]
    assert gw.create_calls[0]["fields"]["content"] == {
        "type": "string",
        "full_text_search": {"language": "en", "stemming": False, "stop_words": False},
    }
    records = list(
        read_canonical(
            s3,
            BUCKET,
            ctx.manifest.stage(Stage.TRANSFORM).outputs["parts_by_namespace"]["documents"],
        )
    )
    assert all(set(r.text_fields) == {"title", "body"} for r in records)
    keys = [o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET, Prefix="fts/import/")["Contents"]]
    body = s3.get_object(Bucket=BUCKET, Key=keys[0])["Body"].read()
    first = json.loads(gzip.decompress(body).decode().splitlines()[0])
    assert "content" in first and "title" in first and "body" not in first
    host = "fts.fake.pinecone.io"
    gw.upsert(
        host,
        "documents",
        [vector_record(r, mapping.objects[0].text_sources) for r in records],
    )
    ctx2 = RunContext(
        config=cfg,
        mapping=mapping,
        manifest=store.load() or ctx.manifest,
        store=store,
        events=EventBus("fts", [JsonLinesSink(io.StringIO())]),
        s3=s3,
        source=source,
        target=gw,
    )
    approve_ctx(ctx2, tmp_path)
    Pipeline([ValidateStage(settle_window_s=5, poll_interval_s=0, filter_queries=3)]).run(
        ctx2, S3Lock(s3, BUCKET, "fts")
    )
    checks = ctx2.manifest.stage(Stage.VALIDATE).outputs["checks"]
    assert checks["full_text_overlap:documents"], checks
    assert all(checks.values()), checks
    validation = json.loads(
        s3.get_object(Bucket=BUCKET, Key="fts/manifest/validation.json")["Body"].read()
    )
    outcome = next(c for c in validation["checks"] if c["check"] == "full_text_overlap")
    classes = outcome["details"]["classes"]
    assert set(classes) == {"single_term", "multi_term", "phrase", "filtered"}
    assert all(c["queries"] >= 15 and c["mean"] == 1.0 for c in classes.values()), classes
    assert outcome["details"]["fields"] == ["title", "content"]
    assert any(q["filter"] and "$match_phrase" in json.dumps(q["filter"]) for q in gw.text_queries)
    report = s3.get_object(Bucket=BUCKET, Key="fts/manifest/validation.md")["Body"].read().decode()
    assert "full_text_overlap | documents | pass" in report and "single_term" in report

    # a direct check with a tightened threshold still passes against the in-process truth
    result = check_full_text(
        gw,
        host,
        source,
        "documents",
        mapping.objects[0],
        k=10,
        queries=8,
        thresholds=mapping.validation.full_text.thresholds.model_copy(  # type: ignore[union-attr]
            update={"single_term": 1.0}
        ),
    )
    assert result.passed and result.classes["single_term"].mean == 1.0
