from typing import Any, Dict

PARAMETER_PREFIX_SEPARATOR: str = "__"


def extract_parameters(prefix: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
    full_prefix: str = prefix + PARAMETER_PREFIX_SEPARATOR
    return {key[len(full_prefix) :]: value for key, value in parameters.items() if key.startswith(full_prefix)}


def embed_parameters(prefix: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
    full_prefix: str = prefix + PARAMETER_PREFIX_SEPARATOR
    return {full_prefix + key: value for key, value in parameters.items()}
