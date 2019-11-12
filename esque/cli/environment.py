import os

# Make sure to only ever import the whole module instead of single variables
# That way it is easier to mock the variables during testing if necessary.
# BAD: from esque.cli.environment import ESQUE_CONF_PATH
# GOOD: from esque.cli import environment

ESQUE_CONF_PATH = os.environ.get("ESQUE_CONF_PATH")
ESQUE_VERBOSE = os.environ.get("ESQUE_VERBOSE")
