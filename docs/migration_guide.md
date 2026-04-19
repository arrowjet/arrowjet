# Migration Guide: redshift_connector → Arrowjet

## Safe Mode (Drop-in Replacement)

### Before (redshift_connector)
```python
import redshift_connector

conn = redshift_connector.connect(host="...", database="dev", user="awsuser", password="...")
cursor = conn.cursor()
cursor.execute("SELECT * FROM users")
df = cursor.fetch_dataframe()
```

### After (Arrowjet safe mode)
```python
import arrowjet as arrowjet

conn = arrowjet.connect(host="...", database="dev", user="awsuser", password="...")
df = conn.fetch_dataframe("SELECT * FROM users")
```

Key differences:
- `fetch_dataframe()` is on the connection, not the cursor
- Returns Arrow-backed DataFrame (faster, less memory)
- `fetch_arrow_table()` available for Arrow-native workflows
- `fetch_numpy_array()` available for numpy workflows

## Bulk Write (Replacing write_dataframe)

### Before (redshift_connector — slow INSERT path)
```python
cursor.write_dataframe(df, "target_table")
conn.commit()
```

### After (Arrowjet — COPY path, 3000x+ faster)
```python
conn.write_dataframe(df, "target_table")
# or with Arrow directly:
conn.write_bulk(arrow_table, "target_table")
```

Requires staging config in `connect()`. See Configuration Reference.

## Bulk Read (New Capability)

Not available in redshift_connector. Arrowjet adds:

```python
result = conn.read_bulk("SELECT * FROM large_table")
df = result.to_pandas()
```

3-10x faster than `fetch_dataframe()` for large results (100K+ rows).

## What Stays the Same

- Connection parameters (host, database, user, password, port)
- SQL syntax
- Transaction support (commit, rollback)
- Metadata queries

## What Changes

| Feature | redshift_connector | Arrowjet |
|---|---|---|
| Read (small) | cursor.fetch_dataframe() | conn.fetch_dataframe() |
| Read (large) | cursor.fetch_dataframe() (slow) | conn.read_bulk() (fast) |
| Write | cursor.write_dataframe() (INSERT) | conn.write_dataframe() (COPY) |
| Arrow native | No | Yes (fetch_arrow_table) |
| S3 staging | Manual | Automatic |
| Cleanup | Manual | Configurable policy |
