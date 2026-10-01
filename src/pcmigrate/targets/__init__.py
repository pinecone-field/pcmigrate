"""Pinecone access layer. The only package that imports the Pinecone SDK, pinned to API 2026-07."""

from pcmigrate.targets.pinecone_client import (
    ImportInfo,
    IndexInfo,
    Match,
    PineconeGateway,
    SdkGateway,
    VectorRecord,
    api_key_from_secret,
)

__all__ = [
    "ImportInfo",
    "IndexInfo",
    "Match",
    "PineconeGateway",
    "SdkGateway",
    "VectorRecord",
    "api_key_from_secret",
]
