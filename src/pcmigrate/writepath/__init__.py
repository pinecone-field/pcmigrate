"""Customer dual-write path: SDK shim and HTTP endpoint writing into the change buffer (ADR-007)."""

from pcmigrate.writepath.shim import BufferedIndex

__all__ = ["BufferedIndex"]
