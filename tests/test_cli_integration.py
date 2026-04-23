"""
Integration tests for CLI commands — requires real Redshift + S3.

Tests the full CLI pipeline against real infrastructure.
Requires env vars: REDSHIFT_HOST, REDSHIFT_PASS, STAGING_BUCKET, STAGING_IAM_ROLE
"""

import os
import pytest
from click.testing import CliRunner

from arrowjet.cli.main import cli

pytestmark = pytest.mark.skipif(
    not os.environ.get("REDSHIFT_HOST"),
    reason="REDSHIFT_HOST not set",
)

STAGING_BUCKET = os.environ.get("STAGING_BUCKET", "")
STAGING_IAM_ROLE = os.environ.get("STAGING_IAM_ROLE", "")
REDSHIFT_HOST = os.environ.get("REDSHIFT_HOST", "")
REDSHIFT_PASS = os.environ.get("REDSHIFT_PASS", "")


def _cli_args(**overrides):
    """Base CLI connection args from env vars."""
    defaults = {
        "host": REDSHIFT_HOST,
        "password": REDSHIFT_PASS,
        "staging_bucket": STAGING_BUCKET,
        "iam_role": STAGING_IAM_ROLE,
    }
    defaults.update(overrides)
    return defaults


class TestExportIntegration:
    def test_export_to_local_parquet(self, tmp_path):
        out = str(tmp_path / "out.parquet")
        runner = CliRunner()
        result = runner.invoke(cli, [
            "export",
            "--query", "SELECT COUNT(*) AS cnt FROM benchmark_test_1m",
            "--to", out,
            "--host", REDSHIFT_HOST,
            "--password", REDSHIFT_PASS,
            "--staging-bucket", STAGING_BUCKET,
            "--iam-role", STAGING_IAM_ROLE,
        ])
        assert result.exit_code == 0, result.output
        assert "Exported" in result.output
        assert os.path.exists(out)

    def test_export_to_s3_direct(self):
        dest = f"s3://{STAGING_BUCKET}/cli-test-export/"
        runner = CliRunner()
        result = runner.invoke(cli, [
            "export",
            "--query", "SELECT * FROM benchmark_test_1m LIMIT 100",
            "--to", dest,
            "--host", REDSHIFT_HOST,
            "--password", REDSHIFT_PASS,
            "--staging-bucket", STAGING_BUCKET,
            "--iam-role", STAGING_IAM_ROLE,
        ])
        assert result.exit_code == 0, result.output
        assert "direct to S3" in result.output
        assert "Exported to" in result.output

    def test_export_shows_connection_context(self, tmp_path):
        out = str(tmp_path / "out.parquet")
        runner = CliRunner()
        result = runner.invoke(cli, [
            "export",
            "--query", "SELECT 1 AS x",
            "--to", out,
            "--host", REDSHIFT_HOST,
            "--password", REDSHIFT_PASS,
        ])
        assert result.exit_code == 0, result.output
        assert "Connected:" in result.output

    def test_export_invalid_query_fails(self, tmp_path):
        out = str(tmp_path / "out.parquet")
        runner = CliRunner()
        result = runner.invoke(cli, [
            "export",
            "--query", "SELECT * FROM nonexistent_table_xyz",
            "--to", out,
            "--host", REDSHIFT_HOST,
            "--password", REDSHIFT_PASS,
        ])
        assert result.exit_code != 0 or "error" in result.output.lower()


class TestImportIntegration:
    def test_import_from_s3(self):
        """Import from S3 path that already has data from export tests."""
        runner = CliRunner()
        # First ensure the target table exists
        import redshift_connector
        conn = redshift_connector.connect(
            host=REDSHIFT_HOST, port=5439, database="dev",
            user="awsuser", password=REDSHIFT_PASS,
        )
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS cli_import_test")
        cursor.execute("""
            CREATE TABLE cli_import_test (
                int_col_0 BIGINT, int_col_1 BIGINT, int_col_2 BIGINT,
                int_col_3 BIGINT, int_col_4 BIGINT,
                float_col_0 DOUBLE PRECISION, float_col_1 DOUBLE PRECISION,
                float_col_2 DOUBLE PRECISION, float_col_3 DOUBLE PRECISION,
                float_col_4 DOUBLE PRECISION,
                str_col_0 VARCHAR, str_col_1 VARCHAR, str_col_2 VARCHAR,
                str_col_3 VARCHAR, str_col_4 VARCHAR
            )
        """)
        conn.close()

        result = runner.invoke(cli, [
            "import",
            "--from", f"s3://{STAGING_BUCKET}/cli-test-export/",
            "--to", "cli_import_test",
            "--host", REDSHIFT_HOST,
            "--password", REDSHIFT_PASS,
            "--iam-role", STAGING_IAM_ROLE,
        ])
        assert result.exit_code == 0, result.output
        assert "Imported" in result.output

    def test_import_nonexistent_table_fails(self):
        runner = CliRunner()
        result = runner.invoke(cli, [
            "import",
            "--from", f"s3://{STAGING_BUCKET}/cli-test-export/",
            "--to", "nonexistent_table_xyz_abc",
            "--host", REDSHIFT_HOST,
            "--password", REDSHIFT_PASS,
            "--iam-role", STAGING_IAM_ROLE,
        ])
        assert result.exit_code != 0 or "Error" in result.output

    def test_import_local_parquet(self, tmp_path):
        import pyarrow as pa
        import pyarrow.parquet as pq
        import redshift_connector

        # Create target table
        conn = redshift_connector.connect(
            host=REDSHIFT_HOST, port=5439, database="dev",
            user="awsuser", password=REDSHIFT_PASS,
        )
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS cli_local_import_test")
        cursor.execute("CREATE TABLE cli_local_import_test (id BIGINT, val DOUBLE PRECISION)")
        conn.close()

        # Create local Parquet file
        table = pa.table({"id": list(range(50)), "val": [float(i) for i in range(50)]})
        parquet_file = tmp_path / "data.parquet"
        pq.write_table(table, str(parquet_file))

        runner = CliRunner()
        result = runner.invoke(cli, [
            "import",
            "--from", str(parquet_file),
            "--to", "cli_local_import_test",
            "--host", REDSHIFT_HOST,
            "--password", REDSHIFT_PASS,
            "--staging-bucket", STAGING_BUCKET,
            "--iam-role", STAGING_IAM_ROLE,
        ])
        assert result.exit_code == 0, result.output
        assert "50" in result.output


class TestValidateIntegration:
    def test_validate_row_count(self):
        runner = CliRunner()
        result = runner.invoke(cli, [
            "validate",
            "--table", "benchmark_test_1m",
            "--row-count",
            "--host", REDSHIFT_HOST,
            "--password", REDSHIFT_PASS,
        ])
        assert result.exit_code == 0, result.output
        assert "1,000,000" in result.output
        assert "Connected:" in result.output

    def test_validate_schema(self):
        runner = CliRunner()
        result = runner.invoke(cli, [
            "validate",
            "--table", "benchmark_test_1m",
            "--schema",
            "--host", REDSHIFT_HOST,
            "--password", REDSHIFT_PASS,
        ])
        assert result.exit_code == 0, result.output
        assert "Schema" in result.output
        assert "columns" in result.output

    def test_validate_sample(self):
        runner = CliRunner()
        result = runner.invoke(cli, [
            "validate",
            "--table", "benchmark_test_1m",
            "--sample",
            "--sample-rows", "3",
            "--host", REDSHIFT_HOST,
            "--password", REDSHIFT_PASS,
        ])
        assert result.exit_code == 0, result.output
        assert "Sample" in result.output

    def test_validate_nonexistent_table_fails(self):
        runner = CliRunner()
        result = runner.invoke(cli, [
            "validate",
            "--table", "nonexistent_table_xyz_abc",
            "--row-count",
            "--host", REDSHIFT_HOST,
            "--password", REDSHIFT_PASS,
        ])
        assert result.exit_code != 0 or "error" in result.output.lower()

    def test_validate_custom_schema(self):
        runner = CliRunner()
        result = runner.invoke(cli, [
            "validate",
            "--table", "benchmark_test_1m",
            "--schema-name", "public",
            "--row-count",
            "--host", REDSHIFT_HOST,
            "--password", REDSHIFT_PASS,
        ])
        assert result.exit_code == 0, result.output
        assert "public.benchmark_test_1m" in result.output

    def test_validate_wrong_schema_fails(self):
        runner = CliRunner()
        result = runner.invoke(cli, [
            "validate",
            "--table", "benchmark_test_1m",
            "--schema-name", "nonexistent_schema",
            "--row-count",
            "--host", REDSHIFT_HOST,
            "--password", REDSHIFT_PASS,
        ])
        assert result.exit_code != 0 or "error" in result.output.lower()


class TestPreviewIntegration:
    def test_preview_s3_file(self):
        runner = CliRunner()
        result = runner.invoke(cli, [
            "preview",
            "--file", f"s3://{STAGING_BUCKET}/arrowjet-test/export.parquet",
        ])
        assert result.exit_code == 0, result.output
        assert "Rows:" in result.output
        assert "Schema:" in result.output


# ===================================================================
# IAM auth — CLI commands with --auth-type iam
# Parametrized across provisioned and serverless clusters.
# ===================================================================

def _has_aws():
    try:
        import boto3
        boto3.client("sts").get_caller_identity()
        return True
    except Exception:
        return False


REDSHIFT_SERVERLESS_HOST = os.environ.get("REDSHIFT_SERVERLESS_HOST", "")

_CLUSTER_PARAMS = [
    pytest.param(REDSHIFT_HOST, "awsuser", id="provisioned"),
    pytest.param(
        REDSHIFT_SERVERLESS_HOST, "admin", id="serverless",
        marks=pytest.mark.skipif(
            not REDSHIFT_SERVERLESS_HOST,
            reason="REDSHIFT_SERVERLESS_HOST not set",
        ),
    ),
]


@pytest.mark.skipif(not _has_aws(), reason="AWS credentials not available")
class TestCliIamAuth:
    """Test CLI commands with --auth-type iam across provisioned and serverless."""

    @pytest.mark.parametrize("host,user", _CLUSTER_PARAMS)
    def test_validate_with_iam(self, host, user):
        runner = CliRunner()
        result = runner.invoke(cli, [
            "validate",
            "--table", "benchmark_test_1m",
            "--row-count",
            "--host", host,
            "--user", user,
            "--auth-type", "iam",
        ])
        assert result.exit_code == 0, result.output
        assert "(iam)" in result.output

    @pytest.mark.parametrize("host,user", _CLUSTER_PARAMS)
    def test_export_local_with_iam(self, host, user, tmp_path):
        out = str(tmp_path / "out.parquet")
        runner = CliRunner()
        result = runner.invoke(cli, [
            "export",
            "--query", "SELECT 1 AS x",
            "--to", out,
            "--host", host,
            "--user", user,
            "--auth-type", "iam",
        ])
        assert result.exit_code == 0, result.output
        assert "Exported" in result.output
        assert os.path.exists(out)

    @pytest.mark.parametrize("host,user", _CLUSTER_PARAMS)
    def test_export_s3_with_iam(self, host, user):
        dest = f"s3://{STAGING_BUCKET}/cli-test-iam-export/"
        runner = CliRunner()
        result = runner.invoke(cli, [
            "export",
            "--query", "SELECT 1 AS x",
            "--to", dest,
            "--host", host,
            "--user", user,
            "--auth-type", "iam",
            "--staging-bucket", STAGING_BUCKET,
            "--iam-role", STAGING_IAM_ROLE,
        ])
        assert result.exit_code == 0, result.output
        assert "direct to S3" in result.output

    @pytest.mark.parametrize("host,user", _CLUSTER_PARAMS)
    def test_import_s3_with_iam(self, host, user):
        """Export → Import round-trip with IAM auth."""
        import redshift_connector
        from arrowjet.auth.redshift import resolve_credentials

        # Setup: create target table via IAM-authed connection
        creds = resolve_credentials(
            host=host, auth_type="iam", db_user=user,
            region=os.environ.get("STAGING_REGION", "us-east-1"),
        )
        conn = redshift_connector.connect(**creds.as_kwargs())
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS cli_iam_import_test")
        cursor.execute("CREATE TABLE cli_iam_import_test (x INT)")
        conn.close()

        # Export a single-column result to S3
        dest = f"s3://{STAGING_BUCKET}/cli-test-iam-import-roundtrip/"
        runner = CliRunner()
        result = runner.invoke(cli, [
            "export",
            "--query", "SELECT 1 AS x",
            "--to", dest,
            "--host", host,
            "--user", user,
            "--auth-type", "iam",
            "--staging-bucket", STAGING_BUCKET,
            "--iam-role", STAGING_IAM_ROLE,
        ])
        assert result.exit_code == 0, result.output

        # Import from that S3 path
        result = runner.invoke(cli, [
            "import",
            "--from", dest,
            "--to", "cli_iam_import_test",
            "--host", host,
            "--user", user,
            "--auth-type", "iam",
            "--iam-role", STAGING_IAM_ROLE,
        ])
        assert result.exit_code == 0, result.output
        assert "Imported" in result.output


@pytest.mark.skipif(
    not _has_aws() or not os.environ.get("REDSHIFT_AUTH_SECRET_ARN"),
    reason="AWS credentials or REDSHIFT_AUTH_SECRET_ARN not available",
)
class TestCliSecretsManagerAuth:
    """Test CLI commands with --auth-type secrets_manager."""

    def test_validate_with_secrets_manager(self):
        runner = CliRunner()
        result = runner.invoke(cli, [
            "validate",
            "--table", "benchmark_test_1m",
            "--row-count",
            "--host", REDSHIFT_HOST,
            "--auth-type", "secrets_manager",
            "--secret-arn", os.environ["REDSHIFT_AUTH_SECRET_ARN"],
        ])
        assert result.exit_code == 0, result.output
        assert "1,000,000" in result.output
