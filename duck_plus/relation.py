from dataclasses import dataclass
from typing import Protocol, List, Literal, Set, Optional
import os
import duckdb

class Relation:
    """A wrapper around duckdb.DuckDBPyRelation with additional metadata."""
    def __init__(self, 
        relation: duckdb.DuckDBPyRelation, 
        source: duckdb.DuckDBPyConnection):
        self.relation: duckdb.DuckDBPyRelation= relation
        self.source_relation: duckdb.DuckDBPyConnection

    def __repr__(self):
        return f"Relation(source={self.source}, columns={self.relation.columns})"
    
    def using_join(self, other: "Relation" | duckdb.DuckDBPyRelation,
     how: Literal['inner','outer','anti','semi','natural','natural left','natural semi', 'natural anti', 'left', 'right', 'full'] = 'inner',
     using_columns: List[str] = [],
     ) -> "Relation":
        """Join with another Relation or DuckDBPyRelation using specified columns.

        Args:
            other: The other Relation or DuckDBPyRelation to join with.
            how: The type of join to perform. Defaults to 'inner'.
            using_columns: List of column names to join on. Must be non-empty unless using 'natural' join.

        Returns:
            A new Relation representing the joined result.

        Raises:
            ValueError: If using_columns is empty and how is not 'natural'.
        """
        using_columns = [col.lower() for col in using_columns]
        if not using_columns and 'natural' not in how.split(' '):
            raise ValueError("using_columns must be non-empty unless using 'natural' join.")
        if not using_columns:
            using_columns = [col.lower() for col in self.relation.columns
                if col.lower() in (c.lower() for c in other.relation.columns)]
        if not using_columns:
            raise ValueError("No common columns found for natural join.")
        if any(col not in (c.lower() for c in other.relation.columns) for col in using_columns):
            raise ValueError("All using_columns must exist in the second relation.")
        if isinstance(other, Relation):
            if any(col not in other.relation.columns for col in using_columns):
                raise ValueError("All using_columns must exist in the second relation.")
        # Ensure using_columns contains unique values, preserving order
        unique_columns = []
        seen = set()
        duplicates = [x for x in using_columns if x in seen or seen.add(x)]
        if duplicates:
            raise ValueError("using_columns must not contain duplicates.")
        using_columns = [x for i, x in enumerate(using_columns) if x not in using_columns[:i]]

        # Check all columns exist in both relations (case-insensitive)
        if isinstance(other, Relation):
            other_cols = set(c.lower() for c in other.relation.columns)
        else:
            other_cols = set(c.lower() for c in other.columns)
        missing_cols = [col for col in using_columns if col.lower() not in other_cols]
        if missing_cols:
            raise ValueError(f"Columns {missing_cols} not found in the second relation.")

