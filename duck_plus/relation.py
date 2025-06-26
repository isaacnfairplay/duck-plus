from dataclasses import dataclass
from typing import List, Literal, Optional, Sequence, Union
import duckdb

class Relation:
    """A wrapper around duckdb.DuckDBPyRelation with additional metadata."""

    def __init__(
        self,
        relation: duckdb.DuckDBPyRelation,
        source: duckdb.DuckDBPyConnection,
    ) -> None:
        self.relation: duckdb.DuckDBPyRelation = relation
        self.source: duckdb.DuckDBPyConnection = source

    def __repr__(self) -> str:
        return f"Relation(source={self.source}, columns={self.relation.columns})"

    def using_join(
        self,
        other: Union["Relation", duckdb.DuckDBPyRelation],
        how: Literal[
            "inner",
            "outer",
            "anti",
            "semi",
            "natural",
            "natural left",
            "natural semi",
            "natural anti",
            "left",
            "right",
            "full",
        ] = "inner",
        using_columns: Optional[Sequence[str]] = None,
    ) -> "Relation":
        """
        Join with another Relation or DuckDBPyRelation using specified columns.

        Args:
            other: The other Relation or DuckDBPyRelation to join with.
            how: The type of join to perform. Defaults to 'inner'.
            using_columns: List of column names to join on. Must be non-empty unless using 'natural' join.

        Returns:
            A new Relation representing the joined result.

        Raises:
            ValueError: If using_columns is empty and how is not 'natural'.
        """
        if using_columns is None:
            using_columns = []

        using_columns = [col.lower() for col in using_columns]

        # Determine columns for natural join if needed
        if not using_columns and "natural" in how:
            if isinstance(other, Relation):
                other_columns = [c.lower() for c in other.relation.columns]
            else:
                other_columns = [c.lower() for c in other.columns]
            using_columns = [
                col.lower()
                for col in self.relation.columns
                if col.lower() in other_columns
            ]
            if not using_columns:
                raise ValueError("No common columns found for natural join.")

        if not using_columns and "natural" not in how:
            raise ValueError("using_columns must be non-empty unless using 'natural' join.")

        # Ensure using_columns contains unique values, preserving order
        seen = set()
        unique_columns = []
        for col in using_columns:
            if col in seen:
                raise ValueError("using_columns must not contain duplicates.")
            seen.add(col)
            unique_columns.append(col)
        using_columns = unique_columns

        # Check all columns exist in both relations (case-insensitive)
        if isinstance(other, Relation):
            other_cols = set(c.lower() for c in other.relation.columns)
        else:
            other_cols = set(c.lower() for c in other.columns)
        missing_cols = [col for col in using_columns if col.lower() not in other_cols]
        if missing_cols:
            raise ValueError(f"Columns {missing_cols} not found in the second relation.")

        # Perform the join
        if isinstance(other, Relation):
            joined = self.relation.join(
                other.relation, using_columns, how=how
            )
            return Relation(joined, self.source)
        else:
            joined = self.relation.join(
                other, using_columns, how=how
            )
            return Relation(joined, self.source)
