"""Source connectors. All source access goes through sources/<kind>/; introspection is read-only."""

from pcmigrate.sources.base import Change, Partition, RecordSink, SourceConnector, SourceSchema

__all__ = ["Change", "Partition", "RecordSink", "SourceConnector", "SourceSchema"]
