from typing import Any, Optional


_execution_environment_cache = {}
_invocation_cache = {}


def initialize_invocation(context):
    # Start with an empty invocation cache
    _clear_invocation_cache()

    # Create an empty list of invocation_ids if it doesn't exist.
    if not get_execution_environment_cache_entry("invocation_ids"):
        set_execution_environment_cache_entry("invocation_ids", [])

    # Add the current invocation ID to the persistent list of invocation IDs.
    _execution_environment_cache["invocation_ids"].append(
        context.aws_request_id.lower()
    )


def _clear_invocation_cache():
    global _invocation_cache
    _invocation_cache = {}


def set_invocation_cache_entry(key: str, value: Any):
    if not isinstance(key, str):
        raise RuntimeError("Cache key must be string")

    _invocation_cache[key] = value


def set_invocation_cache_entries(entries: dict):
    for k, v in entries.items():
        set_invocation_cache_entry(k, v)


def get_invocation_cache_entries() -> dict:
    return _invocation_cache


def get_invocation_cache_entry(key: str, default: Optional[Any] = None) -> Any:
    return _invocation_cache.get(key, default)


def set_execution_environment_cache_entry(key: str, value: Any):
    if not isinstance(key, str):
        raise RuntimeError("Cache key must be string")

    _execution_environment_cache[key] = value


def set_execution_environment_cache_entries(entries: dict):
    for k, v in entries.items():
        set_execution_environment_cache_entry(k, v)


def get_execution_environment_cache_entries() -> dict:
    return _execution_environment_cache


def get_execution_environment_cache_entry(
    key: str, default: Optional[Any] = None
) -> Any:
    return _execution_environment_cache.get(key, default)
