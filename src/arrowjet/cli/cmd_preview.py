"""
arrowjet preview  - quickly inspect Parquet files (local or S3).

Examples:
    arrowjet preview --file ./sales.parquet
    arrowjet preview --file s3://bucket/sales/export.parquet
    arrowjet preview --file s3://bucket/sales/ --schema
"""

import click


@click.command()
@click.option("--file", "filepath", required=True, help="Parquet file path (local or s3://)")
@click.option("--schema", "show_schema", is_flag=True, help="Show schema only")
@click.option("--rows", default=10, help="Number of sample rows to show")
@click.option("--region", default="us-east-1", help="AWS region for S3 files")
def preview(filepath, show_schema, rows, region):
    """Preview a Parquet file (schema, row count, sample data)."""
    import pyarrow.parquet as pq

    is_s3 = filepath.startswith("s3://")

    if is_s3:
        import pyarrow.fs as pafs
        s3fs = pafs.S3FileSystem(region=region)
        s3_path = filepath.replace("s3://", "")
        dataset = pq.ParquetDataset(s3_path, filesystem=s3fs)
        table = dataset.read()
    else:
        table = pq.read_table(filepath)

    click.echo(f"File: {filepath}")
    click.echo(f"Rows: {table.num_rows:,}")
    click.echo(f"Columns: {table.num_columns}")
    click.echo(f"Size: {table.nbytes / 1024 / 1024:.1f} MB (in memory)")
    click.echo()

    # Schema
    click.echo("Schema:")
    for field in table.schema:
        click.echo(f"  {field.name}: {field.type}")

    if not show_schema:
        click.echo()
        click.echo(f"Sample ({min(rows, table.num_rows)} rows):")
        sample = table.slice(0, min(rows, table.num_rows)).to_pandas()
        click.echo(sample.to_string(index=False))
