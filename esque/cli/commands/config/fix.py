import click

from esque.cli.options import State, default_options
from esque.config import Config
from esque.errors import ValidationException


@click.command("fix")
@default_options
def config_fix(state: State):
    """Fix simple errors in esque config.

    Fixes simple errors like wrong current_contexts in the esque config when the configs was tampered with manually."""
    try:
        state.config.context_switch(state.config.current_context)
        click.echo("Your config seems fine. 🎉")
    except ValidationException:
        _cfg: Config = Config(disable_validation=True)
        if _cfg.current_context not in _cfg.available_contexts:
            click.echo(f"Found invalid current context. Switching context to state {_cfg.available_contexts[0]}.")
            _cfg.context_switch(_cfg.available_contexts[0])
            Config.set_instance(_cfg)
            state.config.save()
        else:
            click.echo("Can't fix this configuration error try fixing it manually.")
