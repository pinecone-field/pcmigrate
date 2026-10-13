"""Shared shapes for translating source text analysis into ``full_text_search`` (ADR-018).

Both connectors produce an ``FtsTranslation`` per text field: the Pinecone configuration
that comes closest, plus every source feature that has no equivalent. The findings engine
turns the gaps into warnings; the mapping generator writes the configuration.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from pcmigrate.config.mapping import FTS_LANGUAGES, FullTextSearchSpec, NgramSpec

LANGUAGE_CODES: dict[str, str] = {
    "arabic": "ar",
    "danish": "da",
    "german": "de",
    "greek": "el",
    "english": "en",
    "spanish": "es",
    "finnish": "fi",
    "french": "fr",
    "hungarian": "hu",
    "italian": "it",
    "dutch": "nl",
    "norwegian": "no",
    "portuguese": "pt",
    "romanian": "ro",
    "russian": "ru",
    "swedish": "sv",
    "tamil": "ta",
    "turkish": "tr",
}
DEFAULT_LANGUAGE = "en"


def language_code(name: str | None) -> str | None:
    """``english`` or ``en`` to ``en``; None when Pinecone has no analysis for the language."""
    if not name:
        return None
    lowered = name.lower().strip()
    if lowered in FTS_LANGUAGES:
        return lowered
    return LANGUAGE_CODES.get(lowered)


@dataclass
class FtsTranslation:
    """What one source text field becomes, and what it loses."""

    source: str
    language: str = DEFAULT_LANGUAGE
    stemming: bool = False
    stop_words: bool = False
    ngram: NgramSpec | None = None
    unsupported: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    analyzer: str | None = None
    derived_from: str | None = None
    expression: str | None = None
    searchable: bool = True

    def spec(self) -> FullTextSearchSpec:
        """The validated Pinecone configuration (stop words without stemming are dropped)."""
        stemming = self.stemming
        stop_words = self.stop_words
        ngram = self.ngram
        if ngram is not None:
            stemming = stop_words = False
        elif stop_words and not stemming:
            stop_words = False
        return FullTextSearchSpec(
            language=self.language, stemming=stemming, stop_words=stop_words, ngram=ngram
        )

    def finalize(self) -> None:
        """Record the one loss ``spec()`` applies silently: stop words without stemming."""
        if self.ngram is None and self.stop_words and not self.stemming:
            self.lose(
                "stop-word removal without stemming: Pinecone's stop_words requires stemming, "
                "so stop words are kept (low IDF under BM25); set stemming and stop_words in "
                "mapping.yaml to drop them"
            )

    def lose(self, feature: str) -> None:
        """Record a source feature with no Pinecone equivalent, once."""
        if feature not in self.unsupported:
            self.unsupported.append(feature)

    def note(self, text: str) -> None:
        """Record an informational translation detail, once."""
        if text not in self.notes:
            self.notes.append(text)
