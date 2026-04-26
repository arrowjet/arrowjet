"""
Unit tests for the bulk read engine  - no AWS calls required.
Tests UNLOAD command generation, eligibility checking, and ReadResult.
"""

import pytest
from arrowjet.staging.config import StagingConfig, EncryptionMode
from arrowjet.bulk.unload_builder import UnloadCommandBuilder
from arrowjet.bulk.eligibility import check_read_eligibility, EligibilityResult
from arrowjet.bulk.reader import ReadResult
import pyarrow as pa


class TestUnloadCommandBuilder:
    def _make_config(self, **overrides):
        defaults = dict(
            bucket="test-bucket",
            iam_role="arn:aws:iam::123:role/test",
            region="us-east-1",
        )
        defaults.update(overrides)
        return StagingConfig(**defaults)

    def test_basic_unload(self):
        cfg = self._make_config()
        builder = UnloadCommandBuilder(cfg)
        sql = builder.build("SELECT * FROM t", "s3://bucket/prefix/")
        assert "UNLOAD" in sql
        assert "SELECT * FROM t" in sql
        assert "s3://bucket/prefix/" in sql
        assert "IAM_ROLE" in sql
        assert "FORMAT PARQUET" in sql
        assert "PARALLEL ON" in sql

    def test_parallel_off(self):
        cfg = self._make_config()
        builder = UnloadCommandBuilder(cfg)
        sql = builder.build("SELECT 1", "s3://b/p/", parallel=False)
        assert "PARALLEL OFF" in sql

    def test_kms_encryption(self):
        cfg = self._make_config(
            encryption=EncryptionMode.SSE_KMS,
            kms_key_id="arn:aws:kms:us-east-1:123:key/abc",
        )
        builder = UnloadCommandBuilder(cfg)
        sql = builder.build("SELECT 1", "s3://b/p/")
        assert "ENCRYPTED" in sql
        assert "KMS_KEY_ID" in sql

    def test_no_encryption_by_default(self):
        cfg = self._make_config()
        builder = UnloadCommandBuilder(cfg)
        sql = builder.build("SELECT 1", "s3://b/p/")
        assert "ENCRYPTED" not in sql

    def test_dollar_quoting(self):
        cfg = self._make_config()
        builder = UnloadCommandBuilder(cfg)
        sql = builder.build("SELECT 'hello' FROM t", "s3://b/p/")
        assert "$$SELECT 'hello' FROM t$$" in sql


class TestEligibility:
    def test_eligible_autocommit_select(self):
        result = check_read_eligibility("SELECT * FROM t", autocommit=True)
        assert result.eligible

    def test_eligible_with_clause(self):
        result = check_read_eligibility(
            "WITH cte AS (SELECT 1) SELECT * FROM cte", autocommit=True
        )
        assert result.eligible

    def test_not_eligible_no_autocommit(self):
        result = check_read_eligibility("SELECT * FROM t", autocommit=False)
        assert not result.eligible
        assert "autocommit" in result.reason.lower()

    def test_explicit_mode_bypasses_autocommit(self):
        result = check_read_eligibility(
            "SELECT * FROM t", autocommit=False, explicit_mode=True
        )
        assert result.eligible

    def test_not_eligible_insert(self):
        result = check_read_eligibility(
            "INSERT INTO t SELECT * FROM s", autocommit=True
        )
        assert not result.eligible
        assert "SELECT" in result.reason

    def test_not_eligible_update(self):
        result = check_read_eligibility("UPDATE t SET x=1", autocommit=True)
        assert not result.eligible

    def test_not_eligible_delete(self):
        result = check_read_eligibility("DELETE FROM t", autocommit=True)
        assert not result.eligible

    def test_not_eligible_ddl(self):
        result = check_read_eligibility("CREATE TABLE t (id INT)", autocommit=True)
        assert not result.eligible

    def test_not_eligible_temp_table_hash(self):
        result = check_read_eligibility("SELECT * FROM #temp_data", autocommit=True)
        assert not result.eligible
        assert "temporary" in result.reason.lower()

    def test_not_eligible_temp_table_prefix(self):
        result = check_read_eligibility(
            "SELECT * FROM temp_staging_table", autocommit=True
        )
        assert not result.eligible

    def test_not_eligible_staging_invalid(self):
        result = check_read_eligibility(
            "SELECT * FROM t", autocommit=True, staging_valid=False
        )
        assert not result.eligible
        assert "staging" in result.reason.lower()

    def test_eligibility_result_bool(self):
        assert bool(EligibilityResult(True))
        assert not bool(EligibilityResult(False, "reason"))

    def test_subquery_select(self):
        result = check_read_eligibility(
            "(SELECT * FROM t UNION ALL SELECT * FROM s)", autocommit=True
        )
        assert result.eligible


class TestReadResult:
    def test_repr(self):
        r = ReadResult(
            table=pa.table({"x": [1, 2]}),
            rows=2, files_count=1, bytes_staged=100,
            unload_time_s=1.0, download_time_s=0.5, total_time_s=1.5,
            s3_path="s3://b/p/",
        )
        assert "2" in repr(r)

    def test_to_pandas(self):
        r = ReadResult(
            table=pa.table({"x": [1, 2, 3]}),
            rows=3, files_count=1, bytes_staged=100,
            unload_time_s=1.0, download_time_s=0.5, total_time_s=1.5,
            s3_path="s3://b/p/",
        )
        df = r.to_pandas()
        assert len(df) == 3
        assert list(df.columns) == ["x"]


class TestEligibilityEdgeCases:
    """Additional eligibility tests for edge cases."""

    def test_cte_with_insert_rejected(self):
        result = check_read_eligibility(
            "WITH cte AS (SELECT 1) INSERT INTO t SELECT * FROM cte",
            autocommit=True,
        )
        assert not result.eligible

    def test_cte_with_update_rejected(self):
        result = check_read_eligibility(
            "WITH cte AS (SELECT 1) UPDATE t SET x = cte.x FROM cte",
            autocommit=True,
        )
        assert not result.eligible

    def test_cte_with_delete_rejected(self):
        result = check_read_eligibility(
            "WITH cte AS (SELECT id FROM t) DELETE FROM s WHERE id IN (SELECT id FROM cte)",
            autocommit=True,
        )
        assert not result.eligible

    def test_cte_with_select_allowed(self):
        result = check_read_eligibility(
            "WITH cte AS (SELECT 1 AS x) SELECT * FROM cte",
            autocommit=True,
        )
        assert result.eligible

    def test_show_rejected(self):
        result = check_read_eligibility("SHOW search_path", autocommit=True)
        assert not result.eligible

    def test_explain_rejected(self):
        result = check_read_eligibility("EXPLAIN SELECT * FROM t", autocommit=True)
        assert not result.eligible

    def test_call_rejected(self):
        result = check_read_eligibility("CALL my_procedure()", autocommit=True)
        assert not result.eligible

    def test_set_rejected(self):
        result = check_read_eligibility("SET search_path TO public", autocommit=True)
        assert not result.eligible

    def test_vacuum_rejected(self):
        result = check_read_eligibility("VACUUM my_table", autocommit=True)
        assert not result.eligible

    def test_create_temp_table_rejected(self):
        result = check_read_eligibility(
            "CREATE TEMP TABLE t AS SELECT 1", autocommit=True
        )
        assert not result.eligible

    def test_drop_table_rejected(self):
        result = check_read_eligibility("DROP TABLE t", autocommit=True)
        assert not result.eligible

    def test_truncate_rejected(self):
        result = check_read_eligibility("TRUNCATE t", autocommit=True)
        assert not result.eligible

    def test_nested_subquery_select_allowed(self):
        result = check_read_eligibility(
            "SELECT * FROM (SELECT id, name FROM users WHERE active = true) sub",
            autocommit=True,
        )
        assert result.eligible

    def test_select_with_join_allowed(self):
        result = check_read_eligibility(
            "SELECT a.id, b.name FROM orders a JOIN users b ON a.user_id = b.id",
            autocommit=True,
        )
        assert result.eligible

    def test_multiline_select_allowed(self):
        result = check_read_eligibility(
            """
            SELECT
                id,
                name,
                value
            FROM my_table
            WHERE id > 100
            ORDER BY name
            """,
            autocommit=True,
        )
        assert result.eligible

    def test_select_into_rejected(self):
        result = check_read_eligibility(
            "SELECT * INTO new_table FROM t", autocommit=True
        )
        assert not result.eligible

    def test_select_into_with_schema_rejected(self):
        result = check_read_eligibility(
            "SELECT id, name INTO archive.backup FROM users", autocommit=True
        )
        assert not result.eligible

    def test_for_update_rejected(self):
        result = check_read_eligibility(
            "SELECT * FROM t FOR UPDATE", autocommit=True
        )
        assert not result.eligible

    def test_for_share_rejected(self):
        result = check_read_eligibility(
            "SELECT * FROM t FOR SHARE", autocommit=True
        )
        assert not result.eligible

    def test_for_no_key_update_rejected(self):
        result = check_read_eligibility(
            "SELECT * FROM t FOR NO KEY UPDATE", autocommit=True
        )
        assert not result.eligible


class TestLimitWrapping:
    """UNLOAD doesn't support LIMIT directly  - builder should auto-wrap."""

    def _build(self, query):
        from arrowjet.bulk.unload_builder import _wrap_if_limit
        return _wrap_if_limit(query)

    def test_limit_gets_wrapped(self):
        result = self._build("SELECT * FROM sales LIMIT 1000")
        assert result == "SELECT * FROM (SELECT * FROM sales LIMIT 1000)"

    def test_no_limit_unchanged(self):
        result = self._build("SELECT * FROM sales WHERE date > '2025-01-01'")
        assert result == "SELECT * FROM sales WHERE date > '2025-01-01'"

    def test_limit_with_semicolon(self):
        result = self._build("SELECT * FROM sales LIMIT 100;")
        assert "SELECT * FROM (SELECT * FROM sales LIMIT 100)" in result

    def test_limit_case_insensitive(self):
        result = self._build("SELECT * FROM sales limit 50")
        assert result.startswith("SELECT * FROM (")

    def test_limit_with_where(self):
        result = self._build("SELECT * FROM sales WHERE region = 'us' LIMIT 500")
        assert result == "SELECT * FROM (SELECT * FROM sales WHERE region = 'us' LIMIT 500)"

    def test_already_subquery_no_double_wrap(self):
        q = "SELECT * FROM (SELECT * FROM sales LIMIT 100)"
        result = self._build(q)
        # Should not double-wrap  - the outer query has no LIMIT
        assert result == q

    def test_limit_in_subquery_no_wrap(self):
        q = "SELECT * FROM (SELECT * FROM sales LIMIT 100) WHERE id > 5"
        result = self._build(q)
        # LIMIT is not at the end of the outer query  - no wrap needed
        assert result == q


    def test_offset_gets_wrapped(self):
        result = self._build("SELECT * FROM sales LIMIT 100 OFFSET 50")
        assert result.startswith("SELECT * FROM (")

    def test_offset_only_gets_wrapped(self):
        result = self._build("SELECT * FROM sales OFFSET 50")
        assert result.startswith("SELECT * FROM (")

    def test_no_offset_unchanged(self):
        result = self._build("SELECT * FROM sales WHERE id > 100")
        assert result == "SELECT * FROM sales WHERE id > 100"
