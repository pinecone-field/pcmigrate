"""Pinecone access layer. The only package that imports the Pinecone SDK, pinned to API 2026-07."""

from pcmigrate.targets.pinecone_client import (
    DocumentSchema,
    ImportInfo,
    IndexInfo,
    Match,
    PineconeGateway,
    SdkGateway,
    VectorRecord,
    api_key_from_secret,
)
from pcmigrate.targets.sparse_encoder import (
    EncoderLimits,
    EncoderUsage,
    PineconeSparseEncoder,
    SparseEncoder,
    SparseEncodingError,
)

__all__ = [
    "DocumentSchema",
    "EncoderLimits",
    "EncoderUsage",
    "ImportInfo",
    "IndexInfo",
    "Match",
    "PineconeGateway",
    "PineconeSparseEncoder",
    "SdkGateway",
    "SparseEncoder",
    "SparseEncodingError",
    "VectorRecord",
    "api_key_from_secret",
]
