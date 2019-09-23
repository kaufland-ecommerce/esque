from esque.protocol import ApiVersionsRequestData, BrokerConnection


def test_simple():
    with BrokerConnection(('localhost', 9092), 'esque_integration_test') as connection:
        data = ApiVersionsRequestData()

        request = connection.send(data)
        assert len(request.response_data.api_versions) > 0
