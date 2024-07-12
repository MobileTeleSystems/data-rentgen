from datetime import datetime, timedelta

from data_rentgen.db.utils.uuid import (
    generate_incremental_uuid,
    generate_new_uuid,
    generate_static_uuid,
)


def test_generate_new_uuid_is_uuidv7():
    uuid = generate_new_uuid()
    assert uuid.version == 7


def test_generate_new_uuid_always_different():
    uuid1 = generate_new_uuid()
    uuid2 = generate_new_uuid()
    assert uuid1 != uuid2


def test_generate_new_uuid_sorted_like_timestamp():
    instant = datetime.now()
    following = instant + timedelta(milliseconds=1)

    uuid1 = generate_new_uuid(instant)
    uuid2 = generate_new_uuid(following)
    assert uuid1 < uuid2


def test_generate_static_uuid_is_uuidv5():
    uuid = generate_static_uuid(b"test")
    assert uuid.version == 5


def test_generate_static_uuid_same_input_same_result():
    uuid1 = generate_static_uuid(b"test")
    uuid2 = generate_static_uuid(b"test")
    assert uuid1 == uuid2


def test_generate_static_uuid_different_input_different_result():
    uuid1 = generate_static_uuid(b"test1")
    uuid2 = generate_static_uuid(b"test2")
    assert uuid1 != uuid2


def test_generate_incremental_uuid_is_uuidv7():
    instant = datetime.now()
    uuid = generate_incremental_uuid(instant, b"test")
    assert uuid.version == 7


def test_generate_incremental_uuid_same_input_same_result():
    instant = datetime.now()
    uuid1 = generate_incremental_uuid(instant, b"test")
    uuid2 = generate_incremental_uuid(instant, b"test")
    assert uuid1 == uuid2


def test_generate_incremental_uuid_different_input_different_result():
    current = datetime.now()
    uuid1 = generate_incremental_uuid(current, b"test1")
    uuid2 = generate_incremental_uuid(current, b"test2")
    assert uuid1 != uuid2


def test_generate_incremental_uuid_sorted_like_timestamp():
    current = datetime.now()
    following = current + timedelta(milliseconds=1)
    uuid1 = generate_incremental_uuid(current, b"test")
    uuid2 = generate_incremental_uuid(following, b"test")
    assert uuid1 < uuid2
