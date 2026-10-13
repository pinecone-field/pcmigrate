"""A deterministic English corpus for the golden fixtures of the FTS wave (ADR-018).

Both golden sources carry the same text: the pgvector seed is generated into
``pgvector/003_fts_corpus.sql`` by ``python -m tests.fixtures.golden.corpus``, and the
OpenSearch seeder imports this module. Only the standard library is used because the
seeder runs in a bare ``python:3.12-slim`` container.

Every document gets a topic. Its body draws mostly from the topic's vocabulary with a
Zipf-like skew (so term frequencies vary and rankings are well defined), some words from
a shared pool, and one or two rare words that appear in few documents (so single-term
queries are selective). The summary is a short sentence from the same topic.
"""

from __future__ import annotations

import random
from pathlib import Path

TOPICS: dict[str, list[str]] = {
    "astronomy": [
        "telescope",
        "galaxy",
        "nebula",
        "orbit",
        "comet",
        "eclipse",
        "planet",
        "meteor",
        "satellite",
        "gravity",
        "quasar",
        "asteroid",
        "constellation",
        "observatory",
        "supernova",
        "spectrum",
        "horizon",
        "lunar",
        "solar",
        "cosmic",
        "radiation",
        "pulsar",
        "aurora",
        "zenith",
        "parallax",
    ],
    "cooking": [
        "recipe",
        "simmer",
        "garlic",
        "butter",
        "onion",
        "roast",
        "saffron",
        "skillet",
        "dough",
        "yeast",
        "vinegar",
        "broth",
        "marinade",
        "season",
        "whisk",
        "braise",
        "cinnamon",
        "pepper",
        "lentil",
        "almond",
        "sauce",
        "oven",
        "crust",
        "glaze",
        "stew",
    ],
    "sailing": [
        "harbor",
        "anchor",
        "rudder",
        "mast",
        "compass",
        "tide",
        "keel",
        "voyage",
        "lighthouse",
        "schooner",
        "rigging",
        "starboard",
        "galley",
        "mooring",
        "helm",
        "breeze",
        "current",
        "lagoon",
        "buoy",
        "sextant",
        "cargo",
        "hull",
        "ballast",
        "regatta",
        "ferry",
    ],
    "gardening": [
        "compost",
        "seedling",
        "trellis",
        "orchard",
        "pruning",
        "mulch",
        "blossom",
        "greenhouse",
        "harvest",
        "pollen",
        "lavender",
        "soil",
        "irrigation",
        "perennial",
        "rosemary",
        "hedge",
        "sapling",
        "fertilizer",
        "meadow",
        "thistle",
        "tulip",
        "weeding",
        "nursery",
        "canopy",
        "vine",
    ],
    "music": [
        "violin",
        "chorus",
        "melody",
        "rhythm",
        "tempo",
        "harmony",
        "sonata",
        "trumpet",
        "orchestra",
        "cadence",
        "octave",
        "ballad",
        "quartet",
        "conductor",
        "overture",
        "piano",
        "soprano",
        "refrain",
        "lyric",
        "symphony",
        "clarinet",
        "fugue",
        "chord",
        "baton",
        "encore",
    ],
    "railways": [
        "locomotive",
        "platform",
        "signal",
        "junction",
        "timetable",
        "carriage",
        "viaduct",
        "depot",
        "freight",
        "tunnel",
        "gauge",
        "siding",
        "conductor",
        "ticket",
        "terminus",
        "boiler",
        "tramway",
        "turntable",
        "sleeper",
        "cutting",
        "embankment",
        "whistle",
        "semaphore",
        "coupling",
        "gradient",
    ],
    "medicine": [
        "clinic",
        "diagnosis",
        "vaccine",
        "surgeon",
        "symptom",
        "antibiotic",
        "dosage",
        "patient",
        "therapy",
        "fracture",
        "pulse",
        "anatomy",
        "immune",
        "pharmacy",
        "prescription",
        "recovery",
        "infection",
        "cardiac",
        "ligament",
        "nurse",
        "stethoscope",
        "biopsy",
        "remedy",
        "ward",
        "sutures",
    ],
    "architecture": [
        "facade",
        "arch",
        "column",
        "basilica",
        "cornice",
        "buttress",
        "blueprint",
        "masonry",
        "vault",
        "pavilion",
        "courtyard",
        "terrace",
        "scaffold",
        "lintel",
        "cathedral",
        "spire",
        "mortar",
        "balcony",
        "foundation",
        "gable",
        "atrium",
        "pediment",
        "cupola",
        "portico",
        "beam",
    ],
}
COMMON = [
    "morning",
    "evening",
    "village",
    "market",
    "river",
    "mountain",
    "winter",
    "summer",
    "journey",
    "letter",
    "window",
    "bridge",
    "garden",
    "island",
    "station",
    "museum",
    "silver",
    "copper",
    "timber",
    "glass",
    "paper",
    "stone",
    "cloud",
    "forest",
    "valley",
    "lantern",
    "ladder",
    "basket",
    "wagon",
    "saddle",
    "needle",
    "hammer",
    "candle",
    "mirror",
    "ribbon",
    "pocket",
    "corner",
    "shadow",
    "whisper",
    "thunder",
]
RARE = [
    f"{a}{b}"
    for a in (
        "zephyr",
        "quill",
        "ember",
        "fjord",
        "obsidian",
        "saffire",
        "verdant",
        "marrow",
        "gilded",
        "hollow",
        "tundra",
        "cobalt",
        "amber",
        "cinder",
        "glacial",
        "willow",
    )
    for b in ("wren", "moth", "finch", "lark", "stag", "hare", "vole", "newt", "crane", "pike")
]
TOPIC_NAMES = list(TOPICS)
DOC_COUNT = 1000


def _zipf_choice(rng: random.Random, words: list[str]) -> str:
    weights = [1.0 / (rank + 1) for rank in range(len(words))]
    return rng.choices(words, weights=weights, k=1)[0]


def topic_of(i: int) -> str:
    """The topic of document *i* (1-based)."""
    return TOPIC_NAMES[(i * 7 + i // 13) % len(TOPIC_NAMES)]


def body_text(i: int) -> str:
    """The body of document *i*: 30 to 79 words, topic-heavy with rare markers."""
    rng = random.Random(1000 + i)
    topic = TOPICS[topic_of(i)]
    rotated = topic[i % len(topic) :] + topic[: i % len(topic)]
    count = 30 + (i * 11) % 50
    words: list[str] = []
    for position in range(count):
        if position % 10 == 3:
            words.append(rng.choice(COMMON))
        else:
            words.append(_zipf_choice(rng, rotated))
    rare = rng.sample(RARE, 1 + i % 2)
    for marker in rare:
        words.insert(rng.randrange(len(words) + 1), marker)
    sentences: list[str] = []
    cursor = 0
    while cursor < len(words):
        size = rng.randint(6, 12)
        chunk = words[cursor : cursor + size]
        sentences.append(" ".join(chunk).capitalize() + ".")
        cursor += size
    return " ".join(sentences)


def summary_text(i: int) -> str:
    """A one-sentence summary of document *i* from its topic."""
    rng = random.Random(5000 + i)
    topic = TOPICS[topic_of(i)]
    picked = rng.sample(topic, 4)
    return f"Notes on the {picked[0]} and the {picked[1]} near the {picked[2]} {picked[3]}."


def sql_literal(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def write_pgvector_seed(path: Path) -> None:
    """``003_fts_corpus.sql``: the body and summary of every golden document row."""
    lines = [
        "-- Generated by tests/fixtures/golden/corpus.py; do not edit by hand.",
        "-- Full-text corpus for the FTS wave: topic-heavy bodies with rare markers, and a",
        "-- one-sentence summary. Rows that are NULL in 002_seed.sql (id % 6 = 0) stay NULL.",
        "UPDATE documents SET summary = NULL, body = NULL WHERE id > 1000;",
    ]
    for i in range(1, DOC_COUNT + 1):
        body = "NULL" if i % 6 == 0 else sql_literal(body_text(i))
        summary = "NULL" if i % 9 == 0 else sql_literal(summary_text(i))
        lines.append(f"UPDATE documents SET body = {body}, summary = {summary} WHERE id = {i};")
    lines.append("ANALYZE documents;")
    path.write_text("\n".join(lines) + "\n")


if __name__ == "__main__":
    write_pgvector_seed(Path(__file__).parent / "pgvector" / "003_fts_corpus.sql")
