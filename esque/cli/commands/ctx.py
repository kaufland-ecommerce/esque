import click

from esque.cli.autocomplete import list_contexts
from esque.cli.options import State, default_options
from esque.cli.output import bold


@click.command("ctx")
@click.argument("context", required=False, default=None, shell_complete=list_contexts)
@default_options
def ctx(state: State, context: str):
    """List contexts and switch between them.

    \b
    USAGE:
    esque ctx               : list available contexts
    esque ctx CONTEXT       : switch to context CONTEXT
    """
    if not context:
        for c in state.config.available_contexts:
            if c == state.config.current_context:
                click.echo(bold(c))
            else:
                click.echo(c)
    if context:
        state.config.context_switch(context)
        state.config.save()
        click.echo(f"Switched to context: {context}.")
