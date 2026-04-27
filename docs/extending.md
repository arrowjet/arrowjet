# Extending Arrowjet

Arrowjet is designed to be extended without modifying core code. This guide covers the four extension points available to plugin authors.

## 1. Lifecycle Hooks

Hook into read and write operations to add validation, metrics, logging, or any custom behavior.

```python
import arrowjet

engine = arrowjet.Engine(provider="postgresql")

# Register a hook that fires after every write
def my_write_hook(engine, result):
    print(f"Wrote {result.rows} rows to {result.target_table}")

engine.on("on_write_complete", my_write_hook)

# Register a hook that fires after every read
def my_read_hook(engine, result):
    print(f"Read {result.rows} rows")

engine.on("on_read_complete", my_read_hook)
```

### Supported events

| Event | Fires when | Callback signature |
|---|---|---|
| `on_write_complete` | After `write_bulk` or `write_dataframe` | `fn(engine, write_result)` |
| `on_read_complete` | After `read_bulk` | `fn(engine, read_result)` |
| `on_transfer_complete` | After `arrowjet.transfer()` | `fn(engine, transfer_result)` |

### Rules

- Multiple hooks can be registered for the same event. They fire in registration order.
- If a hook raises an exception, it's logged as a warning but does not break the operation. The write/read still succeeds.
- The `engine` argument is the Engine instance, so hooks can inspect `engine.provider` or any other state.
- Hooks are per-engine-instance, not global.

## 2. Database Providers

Add support for a new database by implementing a provider. No changes to core needed.

```python
class MyDatabaseProvider:
    """Bulk operations for MyDatabase."""

    @property
    def name(self):
        return "mydatabase"

    @property
    def uses_cloud_staging(self):
        return False  # True if it uses S3/GCS staging like Redshift

    def write_bulk(self, conn, table, target_table):
        # table is a PyArrow Table
        # conn is a DBAPI connection
        # Implement your database's fast bulk write path here
        ...
        return MyWriteResult(rows=table.num_rows, ...)

    def read_bulk(self, conn, query):
        # Execute query and return results as Arrow
        ...
        return MyReadResult(table=arrow_table, rows=arrow_table.num_rows, ...)

    def write_dataframe(self, conn, df, target_table):
        import pyarrow as pa
        table = pa.Table.from_pandas(df, preserve_index=False)
        return self.write_bulk(conn, table, target_table)
```

To use it with the Engine, register it in `Engine._init_*` or use it directly:

```python
provider = MyDatabaseProvider()
provider.write_bulk(conn, arrow_table, "my_table")
```

For cloud-staging providers (like Redshift), implement the `BulkProvider` ABC in `arrowjet.providers.base` which uses `build_export_sql()` and `build_import_sql()`.

## 3. CLI Commands

Add new commands to the `arrowjet` CLI from a plugin package.

```python
import click
from arrowjet.cli.main import register_cli_command

@click.command("my-command")
@click.option("--table", required=True)
def my_command(table):
    """My custom command."""
    click.echo(f"Running on {table}")

# Register it - appears in `arrowjet --help`
register_cli_command(my_command)
```

### Validate enrichment

To add checks to the existing `arrowjet validate` command:

```python
from arrowjet.cli.cmd_validate import register_validate_hook

def my_validate_hook(conn, table, schema_name, provider, flags):
    """Called during arrowjet validate."""
    if flags.get("show_schema"):
        click.echo("  [MyPlugin] Custom schema check passed")

register_validate_hook(my_validate_hook)
```

The `flags` dict contains:
- `suggest_fix`: True if `--suggest-fix` was passed
- `apply_fix`: True if `--apply-fix` was passed
- `show_schema`: True if `--schema` was passed

## 4. Plugin Auto-Discovery

Arrowjet automatically discovers plugins on import. If your package is named `arrowjet_*`, add this to your `__init__.py`:

```python
# mypackage/__init__.py

def _register():
    from arrowjet.cli.main import register_cli_command
    from arrowjet.cli.cmd_validate import register_validate_hook
    # Register your CLI commands and hooks here
    ...

_register()
```

When a user does `import arrowjet`, the core package tries to import known plugin packages:

```python
# In arrowjet/__init__.py
try:
    import arrowjet_pro
except ImportError:
    pass
```

For third-party plugins, users would add `import mypackage` in their code, or the plugin can use Python entry_points for automatic discovery.

## Example: Complete Plugin

Here's a minimal plugin that logs every operation to a file:

```python
# arrowjet_logger/__init__.py
import json
import time

def _log_operation(engine, result):
    entry = {
        "ts": time.time(),
        "provider": engine.provider,
        "rows": result.rows,
        "table": getattr(result, "target_table", "unknown"),
    }
    with open("/tmp/arrowjet_operations.jsonl", "a") as f:
        f.write(json.dumps(entry) + "\n")

def enable(engine):
    engine.on("on_write_complete", _log_operation)
    engine.on("on_read_complete", _log_operation)
    return engine
```

Usage:

```python
import arrowjet
import arrowjet_logger

engine = arrowjet.Engine(provider="postgresql")
arrowjet_logger.enable(engine)

engine.write_dataframe(conn, df, "orders")
# Operation logged to /tmp/arrowjet_operations.jsonl
```
