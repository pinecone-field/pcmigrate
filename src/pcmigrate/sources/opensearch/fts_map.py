"""Translate OpenSearch analyzers into Pinecone ``full_text_search`` settings (ADR-018).

A text field's analyzer (built-in, a language analyzer, or a custom chain of tokenizer,
token filters, and char filters from the index's ``analysis`` settings) maps onto the four
knobs Pinecone exposes: language, stemming, stop words, and character n-grams. Every
component with no equivalent (synonyms, char filters, accent folding, shingles, per-field
boosts, a different search analyzer, ...) is recorded as unsupported so the report can
warn per field.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from pcmigrate.config.mapping import NgramSpec
from pcmigrate.sources.fts import DEFAULT_LANGUAGE, FtsTranslation, language_code
from pcmigrate.sources.inventory import ObjectInfo, TextSearchField

LANGUAGE_ANALYZERS = frozenset(
    {
        "arabic",
        "armenian",
        "basque",
        "bengali",
        "brazilian",
        "bulgarian",
        "catalan",
        "cjk",
        "czech",
        "danish",
        "dutch",
        "english",
        "estonian",
        "finnish",
        "french",
        "galician",
        "german",
        "greek",
        "hindi",
        "hungarian",
        "indonesian",
        "irish",
        "italian",
        "latvian",
        "lithuanian",
        "norwegian",
        "persian",
        "portuguese",
        "romanian",
        "russian",
        "sorani",
        "spanish",
        "swedish",
        "turkish",
        "thai",
    }
)
ANALYZER_ALIASES = {"brazilian": "portuguese"}
WORD_TOKENIZERS = frozenset({"standard", "classic", "letter", "lowercase", "uax_url_email"})
NGRAM_TOKENIZERS = frozenset({"ngram", "nGram", "edge_ngram", "edgeNGram"})
NGRAM_FILTERS = frozenset({"ngram", "nGram", "edge_ngram", "edgeNGram"})
STEMMER_FILTERS = frozenset({"stemmer", "porter_stem", "kstem", "snowball", "hunspell"})
HARMLESS_FILTERS = frozenset(
    {
        "lowercase",
        "trim",
        "unique",
        "length",
        "limit",
        "truncate",
        "decimal_digit",
        "apostrophe",
        "classic",
        "remove_duplicates",
        "flatten_graph",
    }
)
UNSUPPORTED_FILTERS = {
    "synonym": "synonym filter",
    "synonym_graph": "synonym_graph filter",
    "asciifolding": "asciifolding (accent folding)",
    "shingle": "shingle filter (word n-grams)",
    "word_delimiter": "word_delimiter filter",
    "word_delimiter_graph": "word_delimiter_graph filter",
    "stemmer_override": "stemmer_override rules",
    "keyword_marker": "keyword_marker (protected words)",
    "keyword_repeat": "keyword_repeat filter",
    "phonetic": "phonetic filter",
    "elision": "elision filter",
    "common_grams": "common_grams filter",
    "reverse": "reverse filter",
    "uppercase": "uppercase filter",
    "cjk_bigram": "cjk_bigram filter",
    "cjk_width": "cjk_width filter",
    "pattern_replace": "pattern_replace filter",
    "pattern_capture": "pattern_capture filter",
    "delimited_payload": "delimited_payload filter",
    "fingerprint": "fingerprint filter",
    "multiplexer": "multiplexer filter",
    "condition": "conditional filter",
    "predicate_token_filter": "predicate_token_filter",
    "dictionary_decompounder": "dictionary_decompounder filter",
    "hyphenation_decompounder": "hyphenation_decompounder filter",
    "min_hash": "min_hash filter",
    "icu_folding": "icu_folding filter",
    "icu_normalizer": "icu_normalizer filter",
    "icu_transform": "icu_transform filter",
    "icu_collation": "icu_collation filter",
    "kuromoji_stemmer": "kuromoji_stemmer filter",
    "nori_part_of_speech": "nori_part_of_speech filter",
}
STEMMER_LANGUAGES = {
    "english": "en",
    "light_english": "en",
    "minimal_english": "en",
    "porter": "en",
    "porter2": "en",
    "lovins": "en",
    "possessive_english": "en",
    "arabic": "ar",
    "danish": "da",
    "german": "de",
    "german2": "de",
    "light_german": "de",
    "minimal_german": "de",
    "greek": "el",
    "spanish": "es",
    "light_spanish": "es",
    "finnish": "fi",
    "light_finnish": "fi",
    "french": "fr",
    "light_french": "fr",
    "minimal_french": "fr",
    "hungarian": "hu",
    "light_hungarian": "hu",
    "italian": "it",
    "light_italian": "it",
    "dutch": "nl",
    "dutch_kp": "nl",
    "norwegian": "no",
    "light_norwegian": "no",
    "minimal_norwegian": "no",
    "light_nynorsk": "no",
    "minimal_nynorsk": "no",
    "portuguese": "pt",
    "light_portuguese": "pt",
    "minimal_portuguese": "pt",
    "portuguese_rslp": "pt",
    "brazilian": "pt",
    "romanian": "ro",
    "russian": "ru",
    "light_russian": "ru",
    "swedish": "sv",
    "light_swedish": "sv",
    "turkish": "tr",
    "tamil": "ta",
}


@dataclass
class AnalysisSettings:
    """The index's ``analysis`` block, split by component."""

    analyzers: dict[str, dict[str, Any]] = field(default_factory=dict)
    tokenizers: dict[str, dict[str, Any]] = field(default_factory=dict)
    filters: dict[str, dict[str, Any]] = field(default_factory=dict)
    char_filters: dict[str, dict[str, Any]] = field(default_factory=dict)

    @classmethod
    def from_settings(cls, analysis: Mapping[str, Any] | None) -> AnalysisSettings:
        """From the raw ``index.analysis`` dict (as ``_settings`` returns it)."""
        analysis = analysis or {}
        return cls(
            analyzers=dict(analysis.get("analyzer") or {}),
            tokenizers=dict(analysis.get("tokenizer") or {}),
            filters=dict(analysis.get("filter") or {}),
            char_filters=dict(analysis.get("char_filter") or {}),
        )


def _stop_language(translation: FtsTranslation, stopwords: Any, origin: str) -> None:
    if isinstance(stopwords, str):
        name = stopwords.strip("_").lower()
        if name == "none":
            return
        code = language_code(name)
        if code is None:
            translation.lose(f"{origin}: stop words for {name!r}, which Pinecone does not analyze")
            return
        if translation.language != code and translation.stemming:
            translation.lose(
                f"{origin}: stop words are {code} but stemming is {translation.language}"
            )
        elif not translation.stemming:
            translation.language = code
        translation.stop_words = True
        return
    if isinstance(stopwords, list):
        translation.stop_words = True
        translation.lose(
            f"{origin}: a custom stop-word list ({len(stopwords)} words); Pinecone uses its "
            "own list"
        )
        return
    translation.stop_words = True


def _stem_language(translation: FtsTranslation, name: str | None, origin: str) -> None:
    key = (name or "english").lower()
    code = STEMMER_LANGUAGES.get(key) or language_code(key)
    if code is None:
        translation.lose(f"{origin}: stemmer {key!r} has no Pinecone equivalent")
        return
    if (
        translation.stop_words
        and translation.language != code
        and translation.language != DEFAULT_LANGUAGE
    ):
        translation.lose(f"{origin}: stemmer is {code} but stop words are {translation.language}")
    translation.language = code
    translation.stemming = True
    if key not in (
        "english",
        "porter",
        "porter2",
        "french",
        "german",
        "spanish",
        "italian",
        "dutch",
        "portuguese",
        "russian",
        "swedish",
        "danish",
        "finnish",
        "hungarian",
        "norwegian",
        "romanian",
        "turkish",
        "arabic",
        "greek",
        "tamil",
    ):
        translation.note(f"{origin}: {key!r} stemmer approximated by Pinecone's {code} stemming")


def _ngram(
    settings: Mapping[str, Any], *, prefix: bool, origin: str, translation: FtsTranslation
) -> None:
    min_gram = int(settings.get("min_gram", 1))
    max_gram = int(settings.get("max_gram", 2))
    if translation.ngram is not None:
        translation.lose(f"{origin}: a second n-gram component; the first one is kept")
        return
    translation.ngram = NgramSpec(
        min_gram=min_gram, max_gram=max(min_gram, max_gram), prefix_only=prefix
    )
    if settings.get("token_chars"):
        translation.note(
            f"{origin}: token_chars {settings['token_chars']} are not configurable in Pinecone"
        )


def _apply_filter(translation: FtsTranslation, name: str, analysis: AnalysisSettings) -> None:
    definition = analysis.filters.get(name)
    kind = str(definition.get("type", name)) if definition else name
    params: Mapping[str, Any] = definition or {}
    origin = f"filter {name!r}"
    if kind in HARMLESS_FILTERS:
        return
    if kind == "stop":
        _stop_language(translation, params.get("stopwords", "_english_"), origin)
        return
    if kind in STEMMER_FILTERS:
        if kind == "hunspell":
            translation.lose(
                f"{origin}: hunspell dictionary {params.get('locale')!r}; Pinecone stems with "
                "its own rules"
            )
            translation.stemming = True
            return
        language = params.get("language") or params.get("name")
        if kind in ("porter_stem", "kstem"):
            language = "english"
        _stem_language(translation, str(language) if language else None, origin)
        return
    if kind in NGRAM_FILTERS:
        _ngram(
            params, prefix=kind.lower().startswith("edge"), origin=origin, translation=translation
        )
        return
    if kind in UNSUPPORTED_FILTERS:
        translation.lose(f"{origin}: {UNSUPPORTED_FILTERS[kind]}")
        return
    translation.lose(f"{origin}: {kind} filter has no Pinecone equivalent")


def _apply_tokenizer(translation: FtsTranslation, name: str, analysis: AnalysisSettings) -> None:
    definition = analysis.tokenizers.get(name)
    kind = str(definition.get("type", name)) if definition else name
    params: Mapping[str, Any] = definition or {}
    origin = f"tokenizer {name!r}"
    if kind in WORD_TOKENIZERS:
        if kind == "letter":
            translation.note(f"{origin}: letter tokenizer drops digits; Pinecone keeps them")
        return
    if kind in NGRAM_TOKENIZERS:
        _ngram(
            params, prefix=kind.lower().startswith("edge"), origin=origin, translation=translation
        )
        return
    if kind == "whitespace":
        translation.lose(f"{origin}: whitespace tokenization keeps punctuation attached to words")
        return
    if kind == "keyword":
        translation.searchable = False
        translation.lose(
            f"{origin}: the whole value is one token; keep the field as filterable metadata"
        )
        return
    translation.lose(f"{origin}: {kind} tokenizer has no Pinecone equivalent")


def _apply_builtin_analyzer(translation: FtsTranslation, name: str) -> bool:
    """True when *name* is a built-in analyzer that was applied."""
    if name == "standard":
        return True
    if name == "simple":
        translation.note("simple analyzer drops digits; Pinecone keeps them")
        return True
    if name == "whitespace":
        translation.lose("whitespace analyzer: no lowercasing and punctuation attached to words")
        return True
    if name == "stop":
        _stop_language(translation, "_english_", "stop analyzer")
        return True
    if name == "keyword":
        translation.searchable = False
        translation.lose(
            "keyword analyzer: the whole value is one token; keep the field as filterable metadata"
        )
        return True
    if name == "pattern":
        translation.lose("pattern analyzer: regex tokenization")
        return True
    if name == "fingerprint":
        translation.lose("fingerprint analyzer: deduplicated, sorted tokens")
        return True
    if name in LANGUAGE_ANALYZERS:
        code = language_code(ANALYZER_ALIASES.get(name, name))
        if code is None:
            translation.language = DEFAULT_LANGUAGE
            translation.lose(
                f"{name} analyzer: Pinecone has no {name} analysis; tokenized as en without "
                "stemming"
            )
            return True
        translation.language = code
        translation.stemming = True
        translation.stop_words = True
        return True
    return False


def translate_analyzer(
    translation: FtsTranslation, name: str, analysis: AnalysisSettings, *, origin: str = "analyzer"
) -> None:
    """Apply analyzer *name* (built-in, language, or custom) to the translation."""
    translation.analyzer = name
    definition = analysis.analyzers.get(name)
    if definition is None:
        if _apply_builtin_analyzer(translation, name):
            return
        translation.lose(
            f"{origin} {name!r} is not defined in the index settings and is not built in; "
            "standard assumed"
        )
        return
    kind = str(definition.get("type", "custom"))
    if kind != "custom":
        if kind == "standard":
            if definition.get("stopwords") not in (None, "_none_"):
                _stop_language(translation, definition.get("stopwords"), f"{origin} {name!r}")
            return
        if _apply_builtin_analyzer(translation, kind):
            if definition.get("stopwords") not in (None, "_none_"):
                _stop_language(translation, definition.get("stopwords"), f"{origin} {name!r}")
            if definition.get("stem_exclusion"):
                translation.lose(f"{origin} {name!r}: stem_exclusion words")
            return
        translation.lose(f"{origin} {name!r}: {kind} analyzer has no Pinecone equivalent")
        return
    for char_filter in definition.get("char_filter") or []:
        char_def = analysis.char_filters.get(str(char_filter))
        char_kind = str(char_def.get("type", char_filter)) if char_def else str(char_filter)
        translation.lose(f"char filter {char_filter!r} ({char_kind})")
    _apply_tokenizer(translation, str(definition.get("tokenizer", "standard")), analysis)
    for token_filter in definition.get("filter") or []:
        _apply_filter(translation, str(token_filter), analysis)


def translate_field(
    text: TextSearchField, analysis: AnalysisSettings, *, default_analyzer: str | None = None
) -> FtsTranslation:
    """The translation of one OpenSearch text field."""
    translation = FtsTranslation(
        source=text.column, derived_from=text.analyzer or default_analyzer or "standard"
    )
    mapping = text.mapping
    analyzer = text.analyzer or default_analyzer or "standard"
    if text.kind == "search_as_you_type":
        translation.ngram = NgramSpec(min_gram=2, max_gram=10, prefix_only=True)
        translation.lose("search_as_you_type: shingle sub-fields are replaced by prefix n-grams")
        translation.analyzer = analyzer
    else:
        translate_analyzer(translation, analyzer, analysis)
    if text.kind == "match_only_text":
        translation.note(
            "match_only_text scores every match equally in the source; Pinecone ranks by BM25"
        )
    if mapping.get("index") is False:
        translation.searchable = False
        translation.lose("index: false; the field is not searchable in the source")
    search_analyzer = text.search_analyzer
    if search_analyzer and search_analyzer != analyzer:
        translation.lose(
            f"search_analyzer {search_analyzer!r} differs from analyzer {analyzer!r}; Pinecone "
            "analyzes queries with the field's own configuration"
        )
    if mapping.get("search_quote_analyzer"):
        translation.lose(f"search_quote_analyzer {mapping['search_quote_analyzer']!r}")
    boost = mapping.get("boost")
    if boost is not None and float(boost) != 1.0:
        translation.lose(f"per-field boost {boost}: BM25 in Pinecone weights every field equally")
    similarity = mapping.get("similarity")
    if similarity and str(similarity).upper() != "BM25":
        translation.lose(f"similarity {similarity!r}: Pinecone scores with BM25")
    if mapping.get("norms") is False:
        translation.lose(
            "norms: false (no length normalization); BM25 in Pinecone normalizes by length"
        )
    index_options = mapping.get("index_options")
    if index_options in ("docs", "freqs"):
        translation.note(
            f"index_options {index_options!r}: the source cannot run phrase queries on this field"
        )
    if mapping.get("copy_to"):
        translation.lose(f"copy_to {mapping['copy_to']}: copied text is not reproduced")
    if mapping.get("index_prefixes") is not None:
        translation.note("index_prefixes: prefix queries need an ngram field in Pinecone")
    translation.finalize()
    for sub, sub_kind in text.multi_fields.items():
        if sub_kind == "keyword":
            translation.note(
                f"multi-field {text.column}.{sub} (keyword) becomes filterable metadata "
                f"{text.column}_{sub}"
            )
        else:
            translation.lose(
                f"multi-field {text.column}.{sub} ({sub_kind}) is not carried; one analyzer "
                "per field"
            )
    return translation


def translate_table(table: ObjectInfo, *, extra: Any = ()) -> list[FtsTranslation]:
    """Every text field of an index translated; ``extra`` names fields to carry regardless."""
    analysis = AnalysisSettings.from_settings(table.settings.get("analysis"))
    default_analyzer = "default" if "default" in analysis.analyzers else None
    out: list[FtsTranslation] = []
    seen: set[str] = set()
    for text in table.text_search:
        out.append(translate_field(text, analysis, default_analyzer=default_analyzer))
        seen.add(text.column)
    for name in extra:
        if name in seen or table.column(name) is None:
            continue
        translation = FtsTranslation(source=name, derived_from=None)
        translation.note(
            "carried on request (--fts-from); not an analyzed text field in the source"
        )
        out.append(translation)
    return out
