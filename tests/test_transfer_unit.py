"""
Unit tests for cross-database transfer  - no real connections needed.
"""

from unittest.mock import MagicMock
import pyarrow as pa
import pytest

from arrowjet.transfer import transfer, TransferResult
from arrowjet.engine import Engine


# --- Helpers ---

def _mock_engine(provider_name, read_rows=100):
    """Create a mock engine that returns fake read results."""
    engine = MagicMock()
    engine.provider = provider_name

    # Mock read_bulk to return an Arrow table
    table = pa.table({
        "id": pa.array(range(read_rows), type=pa.int64()),
        "value": pa.array([float(i) for i in range(read_rows)], type=pa.float64()),
    })
    mock_result = MagicMock()
    mock_result.table = table
    mock_result.rows = read_rows
    engine.read_bulk.return_value = mock_result

    # Mock write_bulk
    mock_write = MagicMock()
    mock_write.rows = read_rows
    engine.write_bulk.return_value = mock_write

    return engine


class TestTransfer:
    def test_basic_transfer(self):
        src = _mock_engine("postgresql", read_rows=50)
        dst = _mock_engine("mysql")
        src_conn = MagicMock()
        dst_conn = MagicMock()

        result = transfer(
            source_engine=src,
            source_conn=src_conn,
            query="SELECT * FROM orders",
            dest_engine=dst,
            dest_conn=dst_conn,
            dest_table="orders",
        )

        assert isinstance(result, TransferResult)
        assert result.rows == 50
        assert result.source_provider == "postgresql"
        assert result.dest_provider == "mysql"
        assert result.dest_table == "orders"

    def test_transfer_calls_read_then_write(self):
        src = _mock_engine("postgresql", read_rows=10)
        dst = _mock_engine("redshift")
        src_conn = MagicMock()
        dst_conn = MagicMock()

        transfer(
            source_engine=src, source_conn=src_conn,
            query="SELECT * FROM t",
            dest_engine=dst, dest_conn=dst_conn,
            dest_table="t",
        )

        # Read was called on source
        src.read_bulk.assert_called_once_with(src_conn, "SELECT * FROM t")

        # Write was called on destination with the Arrow table from read
        dst.write_bulk.assert_called_once()
        write_args = dst.write_bulk.call_args
        assert write_args[0][0] is dst_conn
        assert isinstance(write_args[0][1], pa.Table)
        assert write_args[0][1].num_rows == 10
        assert write_args[0][2] == "t"

    def test_transfer_empty_result(self):
        src = _mock_engine("mysql", read_rows=0)
        dst = _mock_engine("postgresql")

        result = transfer(
            source_engine=src, source_conn=MagicMock(),
            query="SELECT * FROM empty",
            dest_engine=dst, dest_conn=MagicMock(),
            dest_table="target",
        )

        assert result.rows == 0
        # Write should NOT be called for empty results
        dst.write_bulk.assert_not_called()

    def test_transfer_timing_fields(self):
        src = _mock_engine("postgresql", read_rows=100)
        dst = _mock_engine("mysql")

        result = transfer(
            source_engine=src, source_conn=MagicMock(),
            query="SELECT 1",
            dest_engine=dst, dest_conn=MagicMock(),
            dest_table="t",
        )

        assert result.read_time_s >= 0
        assert result.write_time_s >= 0
        assert result.total_time_s >= 0
        assert result.total_time_s >= result.read_time_s

    def test_transfer_repr(self):
        src = _mock_engine("postgresql", read_rows=1000)
        dst = _mock_engine("redshift")

        result = transfer(
            source_engine=src, source_conn=MagicMock(),
            query="SELECT 1",
            dest_engine=dst, dest_conn=MagicMock(),
            dest_table="t",
        )

        r = repr(result)
        assert "TransferResult" in r
        assert "postgresql" in r
        assert "redshift" in r
        assert "1,000" in r


class TestTransferAllProviderCombinations:
    """Test that transfer works for every provider pair."""

    @pytest.mark.parametrize("src_provider,dst_provider", [
        ("postgresql", "mysql"),
        ("postgresql", "redshift"),
        ("mysql", "postgresql"),
        ("mysql", "redshift"),
        ("redshift", "postgresql"),
        ("redshift", "mysql"),
    ])
    def test_provider_pair(self, src_provider, dst_provider):
        src = _mock_engine(src_provider, read_rows=5)
        dst = _mock_engine(dst_provider)

        result = transfer(
            source_engine=src, source_conn=MagicMock(),
            query="SELECT * FROM t",
            dest_engine=dst, dest_conn=MagicMock(),
            dest_table="t",
        )

        assert result.rows == 5
        assert result.source_provider == src_provider
        assert result.dest_provider == dst_provider


class TestTransferPublicAPI:
    def test_transfer_exported_from_arrowjet(self):
        import arrowjet
        assert hasattr(arrowjet, "transfer")
        assert hasattr(arrowjet, "TransferResult")

    def test_transfer_in_all(self):
        import arrowjet
        assert "transfer" in arrowjet.__all__
        assert "TransferResult" in arrowjet.__all__
