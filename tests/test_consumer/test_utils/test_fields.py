from data_rentgen.db.utils.fields import get_fields_digest


def test_get_fields_digest_result_is_uuidv5():
    fields = [{"name": "field"}]
    digest = get_fields_digest(fields)
    assert digest.version == 5


def test_get_fields_digest_same_input_same_result():
    fields = [{"name": "field", "type": "some"}]

    digest1 = get_fields_digest(fields)
    digest2 = get_fields_digest(fields)
    assert digest1 == digest2


def test_get_fields_digest_key_order_does_not_matter():
    fields1 = [{"name": "field", "type": "some"}]
    fields2 = [{"type": "some", "name": "field"}]

    digest1 = get_fields_digest(fields1)
    digest2 = get_fields_digest(fields2)
    assert digest1 == digest2


def test_get_fields_digest_field_order_does_matter():
    fields1 = [{"name": "field"}, {"name": "another"}]
    fields2 = [{"name": "another"}, {"name": "field"}]

    digest1 = get_fields_digest(fields1)
    digest2 = get_fields_digest(fields2)
    assert digest1 != digest2
