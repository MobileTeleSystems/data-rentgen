import pytest

from data_rentgen.db.utils.search import build_tsquery


@pytest.mark.parametrize(
    ["user_input", "expected"],
    [
        pytest.param(
            "some word 12345",
            "('some' & 'word' & '12345') | ('some':* & 'word':* & '12345':*)",
            id="alphanumeric",
        ),
        pytest.param(
            "with_underscore_123",
            "('with_underscore_123') | ('with_underscore_123':*) | ('with':* & 'underscore':* & '123':*)",
            id="with_underscore",
        ),
        pytest.param(
            "some-domain.name",
            "('some-domain.name') | ('some-domain.name':*) | ('some':* & 'domain':* & 'name':*)",
            id="domain_name",
        ),
        pytest.param(
            "/file/path",
            "('/file/path') | ('/file/path':*) | ('file':* & 'path':*)",
            id="file_path",
        ),
        pytest.param(
            "!@#$%^&*()_+~-=[]{}|;':,./<>?a_lot_of_punctuation",
            "('a_lot_of_punctuation') | ('a_lot_of_punctuation':*) | ('a':* & 'lot':* & 'of':* & 'punctuation':*)",
            id="strip_punctuation",
        ),
    ],
)
def test_build_tsquery(user_input: str, expected: str) -> None:
    assert build_tsquery(user_input) == expected
