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
        native_hows = {"left", "right", "outer", "semi", "inner", "anti"}
        custom_natural = {
            "natural", "natural left", "natural semi", "natural anti"
        }
        if using_columns is None:
            using_columns = []

        # Lowercase for comparison, but preserve original for SQL quoting
        using_columns_lc = [col.lower() for col in using_columns]

        # Determine columns for natural join if needed
        if (not using_columns and any(how.startswith(n) for n in custom_natural)):
            if isinstance(other, Relation):
                other_columns = [c.lower() for c in other.relation.columns]
            else:
                other_columns = [c.lower() for c in other.columns]
            using_columns_lc = [
                col.lower()
                for col in self.relation.columns
                if col.lower() in other_columns
            ]
            using_columns = [
                col for col in self.relation.columns
                if col.lower() in using_columns_lc
            ]
            if not using_columns:
                raise ValueError("No common columns found for natural join.")

        if not using_columns and how not in custom_natural:
            raise ValueError("using_columns must be non-empty unless using 'natural' join.")

        # Ensure using_columns contains unique values, preserving order
        seen = set()
        unique_columns = []
        for col in using_columns:
            col_lc = col.lower()
            if col_lc in seen:
                raise ValueError("using_columns must not contain duplicates.")
            seen.add(col_lc)
            unique_columns.append(col)
        using_columns = unique_columns
        using_columns_lc = [col.lower() for col in using_columns]

        # Check all columns exist in both relations (case-insensitive)
        if isinstance(other, Relation):
            other_cols = set(c.lower() for c in other.relation.columns)
        else:
            other_cols = set(c.lower() for c in other.columns)
        missing_cols = [col for col in using_columns if col.lower() not in other_cols]
        if missing_cols:
            raise ValueError(f"Columns {missing_cols} not found in the second relation.")

        # Native join types
        if how in native_hows:
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

        # Custom logic for natural and natural variants
        # Compose SQL manually
        def quote(col):
            return f'"{col}"'

        left_alias = "l"
        right_alias = "r"

        # Get SQL for both sides
        left_sql = self.relation.query if hasattr(self.relation, "query") else self.relation.to_sql()
        if isinstance(other, Relation):
            right_sql = other.relation.query if hasattr(other.relation, "query") else other.relation.to_sql()
            right_columns = list(other.relation.columns)
        else:
            right_sql = other.query if hasattr(other, "query") else other.to_sql()
            right_columns = list(other.columns)

        # Build join condition
        on_clauses = [
            f"{left_alias}.{quote(col)} = {right_alias}.{quote(col)}"
            for col in using_columns
        ]
        on_clause = " AND ".join(on_clauses)

        # Select columns: all from left, then right columns not in left
        left_columns = list(self.relation.columns)
        left_set = set(c.lower() for c in left_columns)
        select_cols = [f"{left_alias}.{quote(col)} AS {quote(col)}" for col in left_columns]
        select_cols += [
            f"{right_alias}.{quote(col)} AS {quote(col)}"
            for col in right_columns if col.lower() not in left_set
        ]
        select_clause = ", ".join(select_cols)

        # Determine join type
        if how == "natural":
            join_type = "INNER JOIN"
        elif how == "natural left":
            join_type = "LEFT JOIN"
        elif how == "natural semi":
            # Only keep left rows with a match, no right columns
            sql = f"""
            SELECT {', '.join([f'{left_alias}.{quote(col)}' for col in left_columns])}
            FROM ({left_sql}) AS {left_alias}
            WHERE EXISTS (
                SELECT 1 FROM ({right_sql}) AS {right_alias}
                WHERE {on_clause}
            )
            """
            result = self.source.sql(sql)
            return Relation(result, self.source)
        elif how == "natural anti":
            # Only keep left rows with no match, no right columns
            sql = f"""
            SELECT {', '.join([f'{left_alias}.{quote(col)}' for col in left_columns])}
            FROM ({left_sql}) AS {left_alias}
            WHERE NOT EXISTS (
                SELECT 1 FROM ({right_sql}) AS {right_alias}
                WHERE {on_clause}
            )
            """
            result = self.source.sql(sql)
            return Relation(result, self.source)
        else:
            raise ValueError(f"Unsupported join type: {how}")

        # Standard join SQL
        sql = f"""
        SELECT {select_clause}
        FROM ({left_sql}) AS {left_alias}
        {join_type} ({right_sql}) AS {right_alias}
        ON {on_clause}
        """
        result = self.source.sql(sql)
        return Relation(result, self.source)

    def asof_join(
        self,
        other: Union["Relation", duckdb.DuckDBPyRelation],
        on: Union[str, Sequence[str]],
        by: Optional[Sequence[str]] = None,
        tolerance: Optional[str] = None,
        direction: Literal["backward", "forward", "nearest"] = "backward",
    ) -> "Relation":
        """
        Perform an asof join with another Relation or DuckDBPyRelation, mirroring DuckDB SQL ASOF JOIN syntax.

        Args:
            other: The other Relation or DuckDBPyRelation to join with.
            on: The column name or list of column names to perform the asof join on.
            by: Optional list of column names to group by before joining.
            tolerance: Optional string representing the maximum time difference allowed for the join.
            direction: The direction of the asof join. Defaults to 'backward'.

        Returns:
            A new Relation representing the joined result.

        Raises:
            ValueError: If join columns are not found in either relation.
        """
        # Normalize 'on' to a list of column names
        if isinstance(on, str):
            on_columns = [on]
        else:
            on_columns = list(on)

        if by is None:
            by = []

        # Get quoted column names for SQL
        def quote(col):
            return f'"{col}"'

        left_alias = "l"
        right_alias = "r"

        # Validate columns exist
        self_cols = set(c.lower() for c in self.relation.columns)
        if isinstance(other, Relation):
            other_cols = set(c.lower() for c in other.relation.columns)
        else:
            other_cols = set(c.lower() for c in other.columns)

        missing_in_self = [col for col in on_columns if col.lower() not in self_cols]
        missing_in_other = [col for col in on_columns if col.lower() not in other_cols]
        if missing_in_self:
            raise ValueError(f"Columns {missing_in_self} not found in the first relation.")
        if missing_in_other:
            raise ValueError(f"Columns {missing_in_other} not found in the second relation.")

        missing_by_self = [col for col in by if col.lower() not in self_cols]
        missing_by_other = [col for col in by if col.lower() not in other_cols]
        if missing_by_self:
            raise ValueError(f"Columns {missing_by_self} not found in the first relation.")
        if missing_by_other:
            raise ValueError(f"Columns {missing_by_other} not found in the second relation.")

        # Build SQL for ASOF JOIN
        left_sql = self.relation.query if hasattr(self.relation, "query") else self.relation.to_sql()
        if isinstance(other, Relation):
            right_sql = other.relation.query if hasattr(other.relation, "query") else other.relation.to_sql()
        else:
            right_sql = other.query if hasattr(other, "query") else other.to_sql()

        # Compose ON clause
        on_clauses = []
        for col in by:
            on_clauses.append(f"{left_alias}.{quote(col)} = {right_alias}.{quote(col)}")
        # ASOF join key
        asof_col = on_columns[0]
        if direction == "backward":
            asof_op = "<="
        elif direction == "forward":
            asof_op = ">="
        elif direction == "nearest":
            asof_op = "<="
        else:
            raise ValueError(f"Unsupported direction: {direction}")

        on_clauses.append(f"{left_alias}.{quote(asof_col)} {asof_op} {right_alias}.{quote(asof_col)}")

        # Tolerance clause
        tolerance_clause = ""
        if tolerance is not None:
            tolerance_clause = (
                f" AND ABS({left_alias}.{quote(asof_col)} - {right_alias}.{quote(asof_col)}) <= INTERVAL '{tolerance}'"
            )

        # Handle column selection and aliasing for ASOF JOIN result
        # DuckDB will include all columns from both tables, prefixing duplicates with r_. 
        # We want to show left table values for overlapping columns, unless not present in right.
        # So, we explicitly select columns: left columns, then right columns not in left.

        # Get column lists
        left_columns = list(self.relation.columns)
        if isinstance(other, Relation):
            right_columns = [col.lower() for col in other.relation.columns]
        else:
            right_columns = [col.lower() for col in other.columns]

        # Lowercase sets for comparison
        left_set = set(left_columns)
        right_set = set(right_columns)

        # Columns to select: all left, then right columns not in left
        select_cols = [f"{left_alias}.{quote(col)} AS {quote(col)}" for col in left_columns]
        select_col.extend([col for col in left_columns if col not in right_set])
        
        select_clause = ",\n        ".join(select_cols)
        
        join_type = "ASOF"
        if direction == "forward":
            join_type = "ASOF FORWARD"
        elif direction == "nearest":
            join_type = "ASOF NEAREST"

        sql = f"""
        SELECT *
        FROM ({left_sql}) AS {left_alias}
        {join_type} JOIN ({right_sql}) AS {right_alias}
        ON {' AND '.join(on_clauses)}{tolerance_clause}
        """

        # Execute the SQL
        result = self.source.sql(sql)
        return Relation(result, self.source)
