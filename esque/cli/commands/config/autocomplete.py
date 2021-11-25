import getpass
import pwd
from pathlib import Path

import click

from esque.cli.options import State, default_options
from esque.cli.output import green_bold
from esque.config import config_dir


@click.command("autocomplete")
@default_options
def config_autocomplete(state: State):
    """Configure esque autocompletion functionality.

    Generate the autocompletion script based on the current shell and
    give instructions to install it into the current environment.
    """
    directory = config_dir()
    config_file_name = "autocomplete.sh"
    config_file: Path = directory / config_file_name
    current_shell = pwd.getpwnam(getpass.getuser()).pw_shell.split("/")[-1]
    source_designator = "source" if current_shell in ["bash", "sh"] else "source_zsh"
    default_environment = ".bashrc" if current_shell in ["bash", "sh"] else ".zshrc"
    with open(config_file.absolute(), "w") as config_fd:
        config_fd.write('eval "$(_ESQUE_COMPLETE=' + source_designator + ' esque)"')
    click.echo("Autocompletion script generated to " + green_bold(str(config_file.absolute())))
    click.echo(
        "To use the autocompletion feature, simply source the contents of the script into your environment, e.g."
    )
    click.echo(
        '\t\techo -e "\\nsource '
        + str(config_file.absolute())
        + '" >> '
        + str(pwd.getpwnam(getpass.getuser()).pw_dir)
        + "/"
        + default_environment
    )
