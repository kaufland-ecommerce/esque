def test_avro_deserializer(
    avro_serialized_data: bytes, schema_registry_client: SchemaRegistryClient, deserialized_data: Any
):
    deserializer: AvroDeserializer = AvroDeserializer(schema_registry_client)
    actual_message: Any = deserializer.deserialize(avro_serialized_data)
    assert actual_message == deserialized_data
