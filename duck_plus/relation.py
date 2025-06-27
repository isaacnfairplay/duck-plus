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
        # Use the DuckDBPyRelation API where possible
        left_rel = self.relation
        if isinstance(other, Relation):
            right_rel = other.relation
        else:
            right_rel = other

        # Handle natural joins by inferring columns
        if how.startswith("natural"):
            if using_columns is not None and len(using_columns) > 0:
                raise ValueError("Do not specify using_columns for natural joins.")
            # Find common columns
            left_cols_set = set(col.lower() for col in left_rel.columns)
            right_cols_set = set(col.lower() for col in right_rel.columns)
            common_cols = [col for col in left_rel.columns if col.lower() in right_cols_set]
            if not common_cols:
                raise ValueError("No common columns for natural join.")
            # Map how to join type
            if how == "natural":
                join_type = "inner"
            elif how == "natural left":
                join_type = "left"
            elif how == "natural right":
                join_type = "right"
            elif how == "natural full" or how == "natural outer":
                join_type = "outer"
            elif how == "natural semi":
                join_type = "semi"
            elif how == "natural anti":
                join_type = "anti"
            else:
                raise ValueError(f"Unsupported natural join type: {how}")
            # Use the API
            result = left_rel.join(right_rel, common_cols, join_type)
            return Relation(result, self.source)
        else:
            if not using_columns or len(using_columns) == 0:
                raise ValueError("using_columns must be specified and non-empty unless using a natural join.")
            # Check for duplicate columns in using_columns
            lowered = [col.lower() for col in using_columns]
            if len(lowered) != len(set(lowered)):
                raise ValueError("Duplicate columns specified in using_columns.")
            # Check that all columns exist in both relations
            left_cols_set = set(col.lower() for col in left_rel.columns)
            right_cols_set = set(col.lower() for col in right_rel.columns)
            missing_in_left = [col for col in using_columns if col.lower() not in left_cols_set]
            missing_in_right = [col for col in using_columns if col.lower() not in right_cols_set]
            if missing_in_left or missing_in_right:
                raise ValueError(
                    f"Columns missing in join: "
                    f"{'left: ' + str(missing_in_left) if missing_in_left else ''}"
                    f"{', ' if missing_in_left and missing_in_right else ''}"
                    f"{'right: ' + str(missing_in_right) if missing_in_right else ''}"
                )
            join_type = how
            result = left_rel.join(right_rel, list(using_columns), join_type)
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
