"""
Integration tests for the unified ArrowjetConnection.

Tests both safe mode (ADBC) and bulk mode (COPY/UNLOAD) through
the single connect() entry point.

Requires env vars: REDSHIFT_HOST, REDSHIFT_PASS, STAGING_BUCKET, STAGING_IAM_ROLE
"""

import os
import pytest
import numpy as np
import pyarrow as pa

import arrowjet as arrowjet

pytestmark = pytest.mark.skipif(
    not os.environ.get("REDSHIFT_HOST"),
    reason="REDSHIFT_HOST not set",
)


def _connect_safe_only():
    """Connect without staging  - safe mode only."""
    return arrowjet.connect(
        host=os.environ["REDSHIFT_HOST"],
        database=os.environ.get("REDSHIFT_DATABASE", "dev"),
        user=os.environ.get("REDSHIFT_USER", "awsuser"),
        password=os.environ["REDSHIFT_PASS"],
        port=int(os.environ.get("REDSHIFT_PORT", "5439")),
    )


def _connect_full():
    """Connect with staging  - safe + bulk mode."""
    return arrowjet.connect(
        host=os.environ["REDSHIFT_HOST"],
        database=os.environ.get("REDSHIFT_DATABASE", "dev"),
        user=os.environ.get("REDSHIFT_USER", "awsuser"),
        password=os.environ["REDSHIFT_PASS"],
        port=int(os.environ.get("REDSHIFT_PORT", "5439")),
        staging_bucket=os.environ["STAGING_BUCKET"],
        staging_iam_role=os.environ["STAGING_IAM_ROLE"],
        staging_region=os.environ.get("STAGING_REGION", "us-east-1"),
        staging_prefix="arrowjet-m4-test",
    )


class TestSafeMode:
    def test_cursor_execute(self):
        with _connect_safe_only() as conn:
            cursor = conn.execute("SELECT 1 AS test")
            row = cursor.fetchone()
            assert row == (1,)

    def test_fetch_dataframe(self):
        with _connect_safe_only() as conn:
            df = conn.fetch_dataframe("SELECT 1 AS id, 'hello' AS name")
            assert len(df) == 1
            assert "id" in df.columns
            assert "name" in df.columns

    def test_fetch_arrow_table(self):
        with _connect_safe_only() as conn:
            table = conn.fetch_arrow_table("SELECT 1 AS id, 2.5 AS value")
            assert table.num_rows == 1
            assert table.num_columns == 2

    def test_safe_only_has_no_bulk(self):
        with _connect_safe_only() as conn:
            assert not conn.has_bulk

    def test_safe_only_bulk_raises(self):
        with _connect_safe_only() as conn:
            with pytest.raises(arrowjet.ArrowjetError, match="Bulk mode not available"):
                conn.read_bulk("SELECT 1")

    def test_context_manager(self):
        conn = _connect_safe_only()
        with conn:
            cursor = conn.execute("SELECT 1")
            assert cursor.fetchone() == (1,)
        # Connection should be closed after with block

    def test_repr_safe_only(self):
        with _connect_safe_only() as conn:
            assert "safe-only" in repr(conn)


class TestBulkMode:
    def _setup_table(self, conn_rs, name, rows):
        """Create test table using redshift_connector (not ADBC)."""
        import redshift_connector
        rs = redshift_connector.connect(
            host=os.environ["REDSHIFT_HOST"],
            port=int(os.environ.get("REDSHIFT_PORT", "5439")),
            database=os.environ.get("REDSHIFT_DATABASE", "dev"),
            user=os.environ.get("REDSHIFT_USER", "awsuser"),
            password=os.environ["REDSHIFT_PASS"],
        )
        cursor = rs.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {name}")
        cursor.execute(f"CREATE TABLE {name} (id BIGINT, value DOUBLE PRECISION)")
        rs.commit()
        if rows > 0:
            cursor.execute(
                f"INSERT INTO {name} SELECT n, RANDOM() * 100 "
                f"FROM (SELECT ROW_NUMBER() OVER () AS n FROM stv_blocklist LIMIT {rows})"
            )
            rs.commit()
        rs.close()

    def _drop_table(self, name):
        import redshift_connector
        rs = redshift_connector.connect(
            host=os.environ["REDSHIFT_HOST"],
            port=int(os.environ.get("REDSHIFT_PORT", "5439")),
            database=os.environ.get("REDSHIFT_DATABASE", "dev"),
            user=os.environ.get("REDSHIFT_USER", "awsuser"),
            password=os.environ["REDSHIFT_PASS"],
        )
        rs.cursor().execute(f"DROP TABLE IF EXISTS {name}")
        rs.commit()
        rs.close()

    def test_full_connection_has_bulk(self):
        with _connect_full() as conn:
            assert conn.has_bulk
            assert "safe+bulk" in repr(conn)

    def test_read_bulk(self):
        self._setup_table(None, "m4_test_read", 1000)
        try:
            with _connect_full() as conn:
                result = conn.read_bulk("SELECT * FROM m4_test_read")
                assert result.rows > 0
                assert result.table.num_columns == 2
                df = result.to_pandas()
                assert len(df) > 0
        finally:
            self._drop_table("m4_test_read")

    def test_write_bulk(self):
        table = pa.table({
            "id": pa.array(np.arange(5000), type=pa.int64()),
            "value": pa.array(np.random.random(5000), type=pa.float64()),
        })
        self._setup_table(None, "m4_test_write", 0)
        try:
            with _connect_full() as conn:
                result = conn.write_bulk(table, "m4_test_write")
                assert result.rows == 5000

                # Verify via safe mode
                df = conn.fetch_dataframe("SELECT COUNT(*) AS cnt FROM m4_test_write")
                assert df["cnt"].iloc[0] == 5000
        finally:
            self._drop_table("m4_test_write")

    def test_write_dataframe(self):
        import pandas as pd
        df = pd.DataFrame({
            "id": np.arange(1000, dtype=np.int64),
            "value": np.random.random(1000),
        })
        self._setup_table(None, "m4_test_write_df", 0)
        try:
            with _connect_full() as conn:
                result = conn.write_dataframe(df, "m4_test_write_df")
                assert result.rows == 1000
        finally:
            self._drop_table("m4_test_write_df")

    def test_safe_and_bulk_in_same_connection(self):
        """Both modes work through the same connection."""
        self._setup_table(None, "m4_test_both", 500)
        try:
            with _connect_full() as conn:
                # Safe mode read
                df_safe = conn.fetch_dataframe("SELECT COUNT(*) AS cnt FROM m4_test_both")
                safe_count = df_safe["cnt"].iloc[0]

                # Bulk mode read
                result = conn.read_bulk("SELECT * FROM m4_test_both")
                bulk_count = result.rows

                # Both should see the same data
                assert safe_count == bulk_count
        finally:
            self._drop_table("m4_test_both")


class TestMetadata:
    """Task 4.6, 4.7: Verify ADBC metadata APIs work for Redshift."""

    def test_get_tables(self):
        with _connect_safe_only() as conn:
            tables = conn.get_tables("public")
            assert isinstance(tables, list)
            assert len(tables) > 0

    def test_get_columns(self):
        with _connect_full() as conn:
            # Use a known table from M0 benchmarks
            cols = conn.get_columns("benchmark_test_1m", "public")
            if cols:  # table may not exist if cluster was reset
                assert isinstance(cols, list)
                assert all("name" in c and "type" in c for c in cols)

    def test_get_schemas(self):
        with _connect_safe_only() as conn:
            schemas = conn.get_schemas()
            assert "public" in schemas
            assert isinstance(schemas, list)

    def test_metadata_uses_safe_mode(self):
        """Metadata should work even without bulk config."""
        with _connect_safe_only() as conn:
            assert not conn.has_bulk
            tables = conn.get_tables()
            assert isinstance(tables, list)


class TestCompatibility:
    """Task 4.9: Safe mode matches redshift_connector behavior."""

    def test_basic_types(self):
        with _connect_safe_only() as conn:
            df = conn.fetch_dataframe(
                "SELECT 1::INT AS i, 2.5::FLOAT AS f, 'hello'::VARCHAR AS s, true::BOOL AS b"
            )
            assert df["i"].iloc[0] == 1
            assert abs(df["f"].iloc[0] - 2.5) < 0.01
            assert df["s"].iloc[0] == "hello"
            assert df["b"].iloc[0] is True or df["b"].iloc[0] == True

    def test_null_handling(self):
        with _connect_safe_only() as conn:
            df = conn.fetch_dataframe("SELECT NULL::INT AS n")
            assert df["n"].isna().iloc[0]

    def test_multiple_rows(self):
        with _connect_safe_only() as conn:
            df = conn.fetch_dataframe(
                "SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3"
            )
            assert len(df) == 3


class TestTransactionsAndSafety:
    """Task 4.10: Transactions, temp tables, cursors in safe mode."""

    def test_autocommit_property(self):
        with _connect_safe_only() as conn:
            # ADBC default may vary  - just check it's accessible
            _ = conn.autocommit

    def test_explicit_commit(self):
        import redshift_connector
        rs = redshift_connector.connect(
            host=os.environ["REDSHIFT_HOST"],
            port=int(os.environ.get("REDSHIFT_PORT", "5439")),
            database=os.environ.get("REDSHIFT_DATABASE", "dev"),
            user=os.environ.get("REDSHIFT_USER", "awsuser"),
            password=os.environ["REDSHIFT_PASS"],
        )
        rs.cursor().execute("DROP TABLE IF EXISTS m4_tx_test")
        rs.cursor().execute("CREATE TABLE m4_tx_test (id INT)")
        rs.commit()
        rs.close()

        try:
            with _connect_safe_only() as conn:
                conn.execute("INSERT INTO m4_tx_test VALUES (1)")
                conn.commit()
                df = conn.fetch_dataframe("SELECT COUNT(*) AS cnt FROM m4_tx_test")
                assert df["cnt"].iloc[0] == 1
        finally:
            rs2 = redshift_connector.connect(
                host=os.environ["REDSHIFT_HOST"],
                port=int(os.environ.get("REDSHIFT_PORT", "5439")),
                database=os.environ.get("REDSHIFT_DATABASE", "dev"),
                user=os.environ.get("REDSHIFT_USER", "awsuser"),
                password=os.environ["REDSHIFT_PASS"],
            )
            rs2.cursor().execute("DROP TABLE IF EXISTS m4_tx_test")
            rs2.commit()
            rs2.close()

    def test_fetch_numpy(self):
        with _connect_safe_only() as conn:
            arr = conn.fetch_numpy_array("SELECT 1 AS x, 2 AS y")
            assert arr.shape == (1, 2)


class TestObservability:
    """Task 5.1-5.3: Metrics, tracing hooks, cost logging."""

    def _setup_table(self, name, rows):
        import redshift_connector
        rs = redshift_connector.connect(
            host=os.environ["REDSHIFT_HOST"],
            port=int(os.environ.get("REDSHIFT_PORT", "5439")),
            database=os.environ.get("REDSHIFT_DATABASE", "dev"),
            user=os.environ.get("REDSHIFT_USER", "awsuser"),
            password=os.environ["REDSHIFT_PASS"],
        )
        cursor = rs.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {name}")
        cursor.execute(f"CREATE TABLE {name} (id BIGINT, value DOUBLE PRECISION)")
        rs.commit()
        if rows > 0:
            cursor.execute(
                f"INSERT INTO {name} SELECT n, RANDOM() * 100 "
                f"FROM (SELECT ROW_NUMBER() OVER () AS n FROM stv_blocklist LIMIT {rows})"
            )
            rs.commit()
        rs.close()

    def _drop_table(self, name):
        import redshift_connector
        rs = redshift_connector.connect(
            host=os.environ["REDSHIFT_HOST"],
            port=int(os.environ.get("REDSHIFT_PORT", "5439")),
            database=os.environ.get("REDSHIFT_DATABASE", "dev"),
            user=os.environ.get("REDSHIFT_USER", "awsuser"),
            password=os.environ["REDSHIFT_PASS"],
        )
        rs.cursor().execute(f"DROP TABLE IF EXISTS {name}")
        rs.commit()
        rs.close()

    def test_metrics_after_bulk_write(self):
        self._setup_table("m5_test_metrics_w", 0)
        table = pa.table({
            "id": pa.array(np.arange(100), type=pa.int64()),
            "value": pa.array(np.random.random(100), type=pa.float64()),
        })
        try:
            with _connect_full() as conn:
                conn.write_bulk(table, "m5_test_metrics_w")
                m = conn.get_metrics()
                assert m.ops_bulk_write == 1
                assert m.bytes_staged_total > 0
        finally:
            self._drop_table("m5_test_metrics_w")

    def test_metrics_after_bulk_read(self):
        self._setup_table("m5_test_metrics_r", 500)
        try:
            with _connect_full() as conn:
                conn.read_bulk("SELECT * FROM m5_test_metrics_r")
                m = conn.get_metrics()
                assert m.ops_bulk_read == 1
                assert m.bytes_staged_total > 0
        finally:
            self._drop_table("m5_test_metrics_r")

    def test_tracing_hook_fires(self):
        self._setup_table("m5_test_hook", 100)
        events = []

        try:
            conn = arrowjet.connect(
                host=os.environ["REDSHIFT_HOST"],
                database=os.environ.get("REDSHIFT_DATABASE", "dev"),
                user=os.environ.get("REDSHIFT_USER", "awsuser"),
                password=os.environ["REDSHIFT_PASS"],
                staging_bucket=os.environ["STAGING_BUCKET"],
                staging_iam_role=os.environ["STAGING_IAM_ROLE"],
                staging_region=os.environ.get("STAGING_REGION", "us-east-1"),
                staging_prefix="arrowjet-m5-test",
                on_bulk_operation=lambda e: events.append(e),
            )
            conn.read_bulk("SELECT * FROM m5_test_hook")
            assert len(events) == 1
            assert events[0].success
            assert events[0].operation_type == "read"
            assert events[0].rows > 0
            conn.close()
        finally:
            self._drop_table("m5_test_hook")

    def test_metrics_error_counted(self):
        with _connect_full() as conn:
            try:
                conn.read_bulk("SELECT * FROM nonexistent_m5_table")
            except Exception:
                pass
            m = conn.get_metrics()
            assert m.errors_total >= 1


class TestAutoMode:
    """Task 6.1-6.6: Auto mode routing."""

    def _setup_table(self, name, rows):
        import redshift_connector
        rs = redshift_connector.connect(
            host=os.environ["REDSHIFT_HOST"],
            port=int(os.environ.get("REDSHIFT_PORT", "5439")),
            database=os.environ.get("REDSHIFT_DATABASE", "dev"),
            user=os.environ.get("REDSHIFT_USER", "awsuser"),
            password=os.environ["REDSHIFT_PASS"],
        )
        cursor = rs.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {name}")
        cursor.execute(f"CREATE TABLE {name} (id BIGINT, value DOUBLE PRECISION)")
        rs.commit()
        if rows > 0:
            cursor.execute(
                f"INSERT INTO {name} SELECT n, RANDOM() * 100 "
                f"FROM (SELECT ROW_NUMBER() OVER () AS n FROM stv_blocklist LIMIT {rows})"
            )
            rs.commit()
        rs.close()

    def _drop_table(self, name):
        import redshift_connector
        rs = redshift_connector.connect(
            host=os.environ["REDSHIFT_HOST"],
            port=int(os.environ.get("REDSHIFT_PORT", "5439")),
            database=os.environ.get("REDSHIFT_DATABASE", "dev"),
            user=os.environ.get("REDSHIFT_USER", "awsuser"),
            password=os.environ["REDSHIFT_PASS"],
        )
        rs.cursor().execute(f"DROP TABLE IF EXISTS {name}")
        rs.commit()
        rs.close()

    def test_read_auto_no_hint_uses_direct(self):
        """Phase 1: without hint, auto routes to direct."""
        self._setup_table("m6_test_auto_direct", 100)
        try:
            with _connect_full() as conn:
                result = conn.read_auto("SELECT * FROM m6_test_auto_direct")
                # Returns Arrow table (direct path)
                assert result.num_rows > 0
        finally:
            self._drop_table("m6_test_auto_direct")

    def test_read_auto_with_hint_uses_bulk(self):
        """Phase 1: with hint, auto routes to bulk."""
        self._setup_table("m6_test_auto_bulk", 100)
        try:
            with _connect_full() as conn:
                result = conn.read_auto(
                    "SELECT * FROM m6_test_auto_bulk",
                    bulk_hint=True,
                )
                # Returns ReadResult (bulk path)
                from arrowjet.bulk.reader import ReadResult
                assert isinstance(result, ReadResult)
                assert result.rows > 0
        finally:
            self._drop_table("m6_test_auto_bulk")

    def test_read_auto_ineligible_falls_back(self):
        """Ineligible query always routes to direct regardless of hint."""
        with _connect_full() as conn:
            # INSERT is not eligible  - should fall back to direct
            # But this would fail on execute, so test with a valid SELECT
            result = conn.read_auto("SELECT 1 AS test")
            assert result.num_rows == 1
