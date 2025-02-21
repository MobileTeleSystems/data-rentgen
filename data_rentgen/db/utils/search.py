# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from collections.abc import Sequence
from enum import IntFlag
from string import punctuation

from sqlalchemy import ColumnElement, func
from sqlalchemy.orm import InstrumentedAttribute

# left some punctuation to match file paths, URLs and host names
TSQUERY_UNSUPPORTED_CHARS = "".join(sorted(set(punctuation) - {"/", ".", "_", "-"}))
TSQUERY_UNSUPPORTED_CHARS_REPLACEMENT = str.maketrans(TSQUERY_UNSUPPORTED_CHARS, " " * len(TSQUERY_UNSUPPORTED_CHARS))
TSQUERY_ALL_PUNCTUATION_REPLACEMENT = str.maketrans(punctuation, " " * len(punctuation))


class SearchRankNormalization(IntFlag):
    """See https://www.postgresql.org/docs/current/textsearch-controls.html#TEXTSEARCH-RANKING"""

    IGNORE_LENGTH = 0
    DOCUMENT_LENGTH_LOGARITHM = 1
    DOCUMENT_LENGTH = 2
    HARMONIC_DISTANCE = 4
    UNIQUE_WORDS = 8
    UNIQUE_WORDS_LOGARITHM = 16
    RANK_PLUS_ONE = 32


def ts_rank(search_vector: InstrumentedAttribute, ts_query: ColumnElement) -> ColumnElement:
    """Get ts_rank for search query ranking.

    Places results with smaller number of total words (like table name) to the top,
    and long results (as file paths) to the bottom.

    Also places on top results with lexemes order matching the tsvector order.
    """
    return func.ts_rank_cd(search_vector, ts_query, SearchRankNormalization.UNIQUE_WORDS)


def make_tsquery(user_input: str) -> ColumnElement:
    """Convert user input to tsquery.

    Work almost like `plainto_tsquery`, but wraps words with `:*` to allow matching by prefix,
    and also allows to match file paths or domain names by parts separated with dot.
    """
    # 'english' dictionary performs stemming, converting `application_123_123` to tsvector `'applic':1 '123':2`,
    # which does not match a tsquery like 'application:* & 123:*'.
    # Instead prefer 'simple' dictionary as it does not use stemming.
    return func.to_tsquery("simple", build_tsquery(user_input))


def ts_match(search_vector: InstrumentedAttribute, ts_query: ColumnElement) -> ColumnElement:
    """Build an expression to get only search_vector matching ts_query."""
    return search_vector.op("@@")(ts_query)


def build_tsquery(user_input: str) -> str:
    original_words = words_with_supported_punctuation(user_input)
    only_words = words_without_any_punctuation(user_input)

    return combine_queries(
        combine_words(*original_words, by_prefix=False),
        combine_words(*original_words),
        combine_words(*only_words) if only_words != original_words else [],
    )


def combine_words(*words: Sequence[str], by_prefix: bool = True) -> str:
    # Convert this ['some', 'query']
    # to this `'some' & 'query'` or `'some':* & 'query':*`
    modifier = ":*" if by_prefix else ""
    return " & ".join(f"'{word}'{modifier}" for word in words if word)


def combine_queries(*queries: Sequence[str]) -> str:
    # Convert this ['/some/file/path:* & abc:*', 'some:* & file:* & path:*']
    # to this '(/some/file/path:* & abc:*) | (some:* & file:* & path:* & abc:*)'
    return " | ".join(f"({query})" for query in queries if query)


def words_with_supported_punctuation(query: str) -> list[str]:
    # convert '@/some/path.or.domain!' -> '/some/path.or.domain'
    converted = query.translate(TSQUERY_UNSUPPORTED_CHARS_REPLACEMENT)
    result = []
    for part in converted.split():
        # delete parts containing only punctuation chars, like ./
        if not all(char in punctuation for char in part):
            result.append(part)  # noqa: PERF401
    return result


def words_without_any_punctuation(query: str) -> list[str]:
    # convert '@/some/path.or.domain!' -> 'some path or domain'
    return query.translate(TSQUERY_ALL_PUNCTUATION_REPLACEMENT).split()
