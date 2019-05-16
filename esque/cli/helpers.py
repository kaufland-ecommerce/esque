import click


def ensure_approval(question: str, *, no_verify: bool = False) -> bool:
    if no_verify:
        return True
    return click.confirm(question)
