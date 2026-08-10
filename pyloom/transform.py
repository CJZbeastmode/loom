"""Python transform registry for Loom."""

from __future__ import annotations
from typing import Any, Callable

_registry: dict[str, Callable[..., Any]] = {}

def register(name: str) -> Callable:
    def decorator(fn: Callable) -> Callable:
        _registry[name] = fn
        return fn
    return decorator

def get(name: str) -> Callable | None:
    return _registry.get(name)
