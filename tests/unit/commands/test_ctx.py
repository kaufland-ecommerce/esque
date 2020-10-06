import pytest
from click.testing import CliRunner

from esque.cli.commands import ctx
from esque.config import Config
from tests.conftest import LOAD_INTEGRATION_TEST_CONFIG, config_loader


def reload_config(load_config: config_loader, *, config: int = LOAD_INTEGRATION_TEST_CONFIG):
    conffile, _ = load_config(config)
    Config.set_instance(Config())
    return Config.get_instance()


def parse_context_output(output: str):
    return output.strip("\n").split("\n")


@pytest.mark.integration
def test_ctx_switch(non_interactive_cli_runner: CliRunner, load_config: config_loader):
    esque_config = reload_config(load_config)

    contexts = esque_config.available_contexts

    # Check the current context actually exists
    result = non_interactive_cli_runner.invoke(ctx, catch_exceptions=False)

    found_contexts = parse_context_output(result.output)

    assert result.exit_code == 0
    assert found_contexts == contexts

    # Switch once to all contexts
    for i, context in enumerate(contexts):
        non_interactive_cli_runner.invoke(ctx, contexts[i], catch_exceptions=False)
        result = non_interactive_cli_runner.invoke(ctx, catch_exceptions=False)

        assert esque_config.current_context == context
        assert result.exit_code == 0
