from data_rentgen.dto.schema import SchemaDTO


def test_get_schema_digest_result_is_uuidv5():
    schema = SchemaDTO(fields=[{"name": "field"}])

    assert schema.digest.version == 5


def test_get_schema_digest_same_input_same_result():
    schema = SchemaDTO(fields=[{"name": "field", "type": "some"}])

    assert schema.digest == schema.digest


def test_get_schema_digest_key_order_does_not_matter():
    schema1 = SchemaDTO(fields=[{"name": "field", "type": "some"}])
    schema2 = SchemaDTO(fields=[{"type": "some", "name": "field"}])

    assert schema1.digest == schema2.digest


def test_get_schema_digest_field_order_does_matter():
    schema1 = SchemaDTO(fields=[{"name": "field"}, {"name": "another"}])
    schema2 = SchemaDTO(fields=[{"name": "another"}, {"name": "field"}])

    assert schema1.digest != schema2.digest
