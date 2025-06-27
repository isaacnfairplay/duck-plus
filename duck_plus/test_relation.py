import pytest
import duckdb
from duck_plus.relation import Relation

@pytest.fixture
def duckdb_conn():
    conn = duckdb.connect(":memory:")
    yield conn
    conn.close()

@pytest.fixture
def sample_relations(duckdb_conn):
    # Create two tables for join tests
    duckdb_conn.execute("CREATE TABLE t1 (id INTEGER, value TEXT, ts TIMESTAMP)")
    duckdb_conn.execute("CREATE TABLE t2 (id INTEGER, value2 TEXT, ts TIMESTAMP)")
    duckdb_conn.execute("INSERT INTO t1 VALUES (1, 'a', '2023-01-01 10:00:00'), (2, 'b', '2023-01-01 11:00:00'), (3, 'c', '2023-01-01 12:00:00')")
    duckdb_conn.execute("INSERT INTO t2 VALUES (1, 'x', '2023-01-01 10:05:00'), (2, 'y', '2023-01-01 11:05:00'), (4, 'z', '2023-01-01 13:00:00')")
    rel1 = Relation(duckdb_conn.table("t1"), duckdb_conn)
    rel2 = Relation(duckdb_conn.table("t2"), duckdb_conn)
    return rel1, rel2

def test_using_join_inner(sample_relations):
    rel1, rel2 = sample_relations
    joined = rel1.using_join(rel2, how="inner", using_columns=["id"])
    result = joined.relation.fetchall()
    assert len(result) == 2
    assert {row[0] for row in result} == {1, 2}

def test_using_join_left(sample_relations):
    rel1, rel2 = sample_relations
    joined = rel1.using_join(rel2, how="left", using_columns=["id"])
    result = joined.relation.fetchall()
    assert len(result) == 3
    ids = [row[0] for row in result]
    assert set(ids) == {1, 2, 3}

def test_using_join_natural(sample_relations):
    rel1, rel2 = sample_relations
    joined = rel1.using_join(rel2, how="natural")
    result = joined.relation.fetchall()
    # Only id and ts are common, so join on those
    assert all(row[0] in (1, 2) for row in result) or result == []

def test_using_join_missing_column(sample_relations):
    rel1, rel2 = sample_relations
    with pytest.raises(ValueError):
        rel1.using_join(rel2, how="inner", using_columns=["nonexistent"])

def test_using_join_duplicate_columns(sample_relations):
    rel1, rel2 = sample_relations
    with pytest.raises(ValueError):
        rel1.using_join(rel2, how="inner", using_columns=["id", "id"])

def test_using_join_empty_columns_not_natural(sample_relations):
    rel1, rel2 = sample_relations
    with pytest.raises(ValueError):
        rel1.using_join(rel2, how="inner", using_columns=[])

def test_using_join_no_common_columns_natural(duckdb_conn):
    duckdb_conn.execute("CREATE TABLE t3 (foo INTEGER)")
    duckdb_conn.execute("CREATE TABLE t4 (bar INTEGER)")
    rel3 = Relation(duckdb_conn.table("t3"), duckdb_conn)
    rel4 = Relation(duckdb_conn.table("t4"), duckdb_conn)
    with pytest.raises(ValueError):
        rel3.using_join(rel4, how="natural")

def test_asof_join_basic(duckdb_conn):
    duckdb_conn.execute("CREATE TABLE t5 (id INTEGER, ts TIMESTAMP)")
    duckdb_conn.execute("CREATE TABLE t6 (id INTEGER, ts TIMESTAMP)")
    duckdb_conn.execute("INSERT INTO t5 VALUES (1, '2023-01-01 10:00:00'), (2, '2023-01-01 11:00:00')")
    duckdb_conn.execute("INSERT INTO t6 VALUES (1, '2023-01-01 09:59:00'), (1, '2023-01-01 10:01:00'), (2, '2023-01-01 10:59:00')")
    rel5 = Relation(duckdb_conn.table("t5"), duckdb_conn)
    rel6 = Relation(duckdb_conn.table("t6"), duckdb_conn)
    joined = rel5.asof_join(rel6, on="ts", by=["id"])
    result = joined.relation.fetchall()
    assert len(result) == 2

def test_asof_join_missing_on_column(sample_relations):
    rel1, rel2 = sample_relations
    with pytest.raises(ValueError):
        rel1.asof_join(rel2, on="nonexistent")

def test_asof_join_missing_by_column(sample_relations):
    rel1, rel2 = sample_relations
    with pytest.raises(ValueError):
        rel1.asof_join(rel2, on="ts", by=["nonexistent"])

def test_asof_join_direction_forward(duckdb_conn):
    duckdb_conn.execute("CREATE TABLE t7 (id INTEGER, ts TIMESTAMP)")
    duckdb_conn.execute("CREATE TABLE t8 (id INTEGER, ts TIMESTAMP)")
    duckdb_conn.execute("INSERT INTO t7 VALUES (1, '2023-01-01 10:00:00')")
    duckdb_conn.execute("INSERT INTO t8 VALUES (1, '2023-01-01 10:01:00')")
    rel7 = Relation(duckdb_conn.table("t7"), duckdb_conn)
    rel8 = Relation(duckdb_conn.table("t8"), duckdb_conn)
    joined = rel7.asof_join(rel8, on="ts", by=["id"], direction="forward")
    result = joined.relation.fetchall()
    assert len(result) == 1

def test_asof_join_invalid_direction(sample_relations):
    rel1, rel2 = sample_relations
    with pytest.raises(ValueError):
        rel1.asof_join(rel2, on="ts", direction="invalid")

def test_repr(sample_relations):
    rel1, _ = sample_relations
    s = repr(rel1)
    assert "Relation(source=" in s
    assert "columns=" in s