import urllib.parse

import click


@click.command("urlencode")
@click.argument("value", metavar="<value>")
def urlencode(value: str):
    """Url-encode the given value.

    Can be used to make any query parameters safe to use in the input or output uri.
    See also `esque io --help`.
    """
    click.echo(urllib.parse.quote_plus(value))
