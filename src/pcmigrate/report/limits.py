"""Pinecone limits the findings and mapping generator enforce.

Values confirmed against docs.pinecone.io on 2026-10-01 for API 2026-07. The design lists
"size limits" as an open question; update here when Pinecone publishes new numbers.
"""

METADATA_BYTES_PER_RECORD = 40 * 1024
RECORD_ID_MAX_CHARS = 512
DENSE_DIMENSION_MAX = 20_000
SPARSE_INDEX_MAX = 2**32 - 1
FIELD_NAME_MAX_BYTES = 64
RESERVED_FIELD_PREFIXES = ("_", "$")
JSON_FLATTEN_MAX_DEPTH = 2
EXPORT_WARN_SECONDS = 2 * 60 * 60
ON_DEMAND_IMPORT_MAX_BYTES = 1024**4
SPARSE_MAX_NONZERO = 2048
SPARSE_MODEL_MAX_TOKENS = 2048
SPARSE_MODEL_DEFAULT_TOKENS = 512
SPARSE_MODEL_USD_PER_MILLION_TOKENS = 0.08
SPARSE_MODEL_TOKENS_PER_MINUTE = {"starter": 250_000, "standard": 3_000_000}
CHARS_PER_TOKEN = 4
