import pytest


@pytest.fixture(autouse=True)
def os_environ_variables(monkeypatch):

    environ_variables = {"ESQUE_VERBOSE": "DEBUG", "ESQUE_CONTEXT_ENABLED": "True"}
    for k, v in environ_variables.items():
        monkeypatch.setenv(k, v)

    return environ_variables
