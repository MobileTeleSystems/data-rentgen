from datetime import UTC, datetime, timedelta
from uuid import UUID

from data_rentgen.utils.uuid import (
    generate_incremental_uuid,
    generate_new_uuid,
    generate_static_uuid,
    get_max_uuid,
    get_min_uuid,
)


def test_generate_new_uuid_is_uuidv7():
    uuid = generate_new_uuid()
    assert uuid.version == 7


def test_generate_new_uuid_always_different():
    uuid1 = generate_new_uuid()
    uuid2 = generate_new_uuid()
    assert uuid1 != uuid2


def test_generate_new_uuid_sorted_like_timestamp():
    instant = datetime.now(tz=UTC)
    following = instant + timedelta(milliseconds=1)

    uuid1 = generate_new_uuid(instant)
    uuid2 = generate_new_uuid(following)
    assert uuid1 < uuid2


def test_generate_static_uuid_is_uuidv5():
    uuid = generate_static_uuid("test")
    assert uuid.version == 5


def test_generate_static_uuid_same_input_same_result():
    uuid1 = generate_static_uuid("test")
    uuid2 = generate_static_uuid("test")
    assert uuid1 == uuid2


def test_generate_static_uuid_different_input_different_result():
    uuid1 = generate_static_uuid("test1")
    uuid2 = generate_static_uuid("test2")
    assert uuid1 != uuid2


def test_generate_incremental_uuid_is_uuidv7():
    instant = datetime.now(tz=UTC)
    uuid = generate_incremental_uuid(instant, "test")
    assert uuid.version == 7


def test_generate_incremental_uuid_same_input_same_result():
    instant = datetime.now(tz=UTC)
    uuid1 = generate_incremental_uuid(instant, "test")
    uuid2 = generate_incremental_uuid(instant, "test")
    assert uuid1 == uuid2


def test_generate_incremental_uuid_different_input_different_result():
    current = datetime.now(tz=UTC)
    uuid1 = generate_incremental_uuid(current, "test1")
    uuid2 = generate_incremental_uuid(current, "test2")
    assert uuid1 != uuid2


def test_generate_incremental_uuid_sorted_like_timestamp():
    current = datetime.now(tz=UTC)
    following = current + timedelta(milliseconds=1)
    uuid1 = generate_incremental_uuid(current, "test")
    uuid2 = generate_incremental_uuid(following, "test")
    assert uuid1 < uuid2


def test_get_min_uuid():
    timestamp = datetime(2025, 9, 21, 23, 35, 49, 123456, tzinfo=UTC)
    uuid = get_min_uuid(timestamp)
    assert uuid == UUID("01996ea2-3883-0000-0000-000000000000")


def test_get_max_uuid():
    timestamp = datetime(2025, 9, 21, 23, 35, 49, 123456, tzinfo=UTC)
    uuid = get_max_uuid(timestamp)
    assert uuid == UUID("01996ea2-3883-ffff-ffff-ffffffffffff")
