import os
from typing import List, Optional, Union

# Make sure to only ever import the whole module instead of single variables
# That way it is easier to mock the variables during testing if necessary.
# BAD: from esque.cli.environment import ESQUE_CONF_PATH
# GOOD: from esque.cli import environment


ESQUE_CONF_PATH: str
ESQUE_VERBOSE: str
ESQUE_CONTEXT_ENABLED: bool
ESQUE_BOOTSTRAP_SERVERS: List
ESQUE_SECURITY_PROTOCOL: str
ESQUE_SCHEMA_REGISTRY: str
ESQUE_NUM_PARTITIONS: str
ESQUE_REPLICATION_FACTOR: str
ESQUE_SASL_MECHANISM: str
ESQUE_SASL_USER: str
ESQUE_SASL_PASSWORD: str
ESQUE_SSL_CAFILE: str
ESQUE_SSL_CERTFILE: str
ESQUE_SSL_KEYFILE: str
ESQUE_SSL_PASSWORD: str


def __getattr__(name: str) -> Optional[Union[str, bool, int, List[str]]]:
    env = {k: v for k, v in globals()["__annotations__"].items() if k.startswith("ESQUE_")}

    if name in env.keys():
        val = os.getenv(name)

        if val is None:
            return None

        if env[name] == bool:
            return val == "True"

        if env[name] == List:
            return list(val.split(","))

        return env[name](os.getenv(name))

    raise AttributeError(f"Environment Variable {name} not defined.")
