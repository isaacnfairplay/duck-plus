from dataclasses import dataclass
from typing import Protocol, List, Literal, Set, Optional
import os
import duckdb

SUPPORTED_EXTENSIONS = ("parquet", "csv", "xlsx")  # xlsx requires DuckDB 1.2

class TransformAlreadyAppliedError(Exception):
    """Raised when attempting to reapply a transform with the same name."""
    def __init__(self, transform_name: str, message: Optional[str] = None):
        self.transform_name = transform_name
        super().__init__(f"Transform '{transform_name}' has already been applied{message and f': {message}' or ''}")

@dataclass
class ConnectedRelation:
    """Couples a DuckDB relation with its connection and database path."""
    conn_path: str  # Database path, e.g., ":memory:" or file path
    conn: duckdb.DuckDBPyConnection
    relation: duckdb.DuckDBPyRelation

    def __post_init__(self):
        if self.conn_path != ":memory:" and not os.path.exists(os.path.dirname(self.conn_path)):
            raise ValueError(f"Invalid connection path: {self.conn_path}")

class FileTransform(Protocol):
    """Protocol for UDFs that transform a FileEntryRelation."""
    def __call__(self, fer: "FileEntryRelation") -> "FileEntryRelation":
        ...

@dataclass
class FileEntryRelation:
    """Represents a file and its DuckDB relation with UDF transformations."""
    entry: os.DirEntry
    relation_conn: ConnectedRelation
    transform_function_log: List[FileTransform]
    transform_names: Set[str]  # Tracks unique transform names

    def __post_init__(self):
        if self.transform_function_log is None:
            self.transform_function_log = []
        if self.transform_names is None:
            self.transform_names = set()
        try:
            extension = self.entry.name.split(".")[-1].lower()
            if extension not in SUPPORTED_EXTENSIONS:
                raise ValueError(f"Unsupported extension '{extension}' in {self.entry.name}. Supported: {SUPPORTED_EXTENSIONS}")
        except IndexError:
            raise ValueError(f"No file extension found in {self.entry.name}")

    @property
    def source_type(self) -> Literal["parquet", "csv", "xlsx"]:
        """Return the file's extension as a supported type."""
        return self.entry.name.split(".")[-1].lower()

    def apply_transform(self, transform: FileTransform, transform_alias: str, re_apply_udf: Optional[bool]=None) -> "FileEntryRelation":
        """Apply a UDF and log it, preventing re-application of transforms with the same name."""
        if transform_alias in self.transform_names:
            raise TransformAlreadyAppliedError(transform_alias)
        if not re_apply_udf and transform in self.transform_function_log:
            raise TransformAlreadyAppliedError(transform_alias, "If you intended to re-apply this UDF please use 're_apply_udf=True'")
        result = transform(self)
        result.transform_function_log = self.transform_function_log + [transform]
        result.transform_names = self.transform_names | {transform_alias}
        return result