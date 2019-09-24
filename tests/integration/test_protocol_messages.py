from esque.protocol import ApiVersionsRequestData, BrokerConnection


def test_simple(test_config):
    server = test_config.bootstrap_hosts[0]
    port = int(test_config.bootstrap_port)
    with BrokerConnection((server, port), "esque_integration_test") as connection:
        data = ApiVersionsRequestData()

        request = connection.send(data)
        assert len(request.response_data.api_versions) > 0
