"""Test transfer CLI with profiles including Redshift."""
import os, sys
sys.stdout.reconfigure(line_buffering=True)

from pathlib import Path
import arrowjet.cli.config as cfg

# Point to temp config
config_path = Path("/tmp/arrowjet_test_config/config.yaml")
if not config_path.exists():
    print(f"Config not found: {config_path}")
    sys.exit(1)
cfg.CONFIG_FILE = config_path

from click.testing import CliRunner
from arrowjet.cli.main import cli

runner = CliRunner()

# Test 1: PG -> Redshift via profiles
print("=== Test 1: PG -> Redshift (profiles) ===")
result = runner.invoke(cli, [
    "transfer",
    "--from-profile", "test-pg",
    "--to-profile", "test-redshift",
    "--query", "SELECT generate_series(1, 100) AS id",
    "--to-table", "profile_xfer_rs",
])
print(result.output)
print(f"Exit code: {result.exit_code}")
if result.exception:
    import traceback
    traceback.print_exception(type(result.exception), result.exception, result.exception.__traceback__)
assert result.exit_code == 0, "PG -> Redshift failed"

# Verify in Redshift
import redshift_connector
conn = redshift_connector.connect(
    host=os.environ["REDSHIFT_HOST"], port=5439, database="dev",
    user="awsuser", password=os.environ["REDSHIFT_PASS"],
)
conn.autocommit = True
cursor = conn.cursor()
cursor.execute("SELECT COUNT(*) FROM profile_xfer_rs")
count = cursor.fetchone()[0]
print(f"Verified: {count} rows in Redshift")
assert count == 100, f"Expected 100, got {count}"

# Cleanup
cursor.execute("DROP TABLE IF EXISTS profile_xfer_rs")
conn.close()

print("\nALL PROFILE TRANSFER TESTS PASSED")
