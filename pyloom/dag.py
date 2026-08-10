"""
Python DAG builder API for Loom.

Usage:
    from pyloom.dag import DAG

    dag = (
        DAG("my-pipeline")
        .source("api", connection="my_api")
        .transform("my_module:my_func")
        .filter("status", "eq", "done")
        .sink("s3", connection="my_s3")
    )
    dag.to_json("pipelines/my.dag.json")
"""

from __future__ import annotations
from typing import Any

class DAG:
    def __init__(self, name: str, version: str = "1.0") -> None:
        pass

    def source(self, name: str, **kwargs: Any) -> DAG:
        return self

    def transform(self, entrypoint: str, **kwargs: Any) -> DAG:
        return self

    def filter(self, field: str, op: str, value: str, **kwargs: Any) -> DAG:
        return self

    def aggregate(self, group_by: list[str], aggs: list[dict[str, Any]], **kwargs: Any) -> DAG:
        return self

    def join(self, key: str, sources: list[str], **kwargs: Any) -> DAG:
        return self

    def sink(self, connection: str, **kwargs: Any) -> DAG:
        return self

    def to_json(self, path: str) -> None:
        pass

    def to_dict(self) -> dict[str, Any]:
        return {}
