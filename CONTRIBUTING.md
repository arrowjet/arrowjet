# Contributing to Arrowjet

Thanks for your interest in contributing! Arrowjet is an open source project and we welcome contributions of all kinds.

## What We Need Help With

Check the [Roadmap](https://github.com/arrowjet/arrowjet#roadmap) for open items. Good starting points:

- **New database providers** (Snowflake, BigQuery, Databricks) - see [docs/extending.md](docs/extending.md) for the provider API
- **Homebrew formula** - make `brew install arrowjet` work
- **Docker image** - Dockerfile for CLI usage in CI/CD
- **CSV/JSON staging format** - add format options to COPY/UNLOAD

## Setup

```bash
git clone https://github.com/arrowjet/arrowjet.git
cd arrowjet
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

## Running Tests

```bash
# Unit tests (no database needed)
python -m pytest tests/ -k "unit" -v

# Integration tests (requires database credentials in .env)
cp .env.example .env
# Edit .env with your credentials
python -m pytest tests/ -v
```

## Code Style

- Python 3.10+
- No unicode characters in code or docs (use `->` not arrows, use `-` not em dashes)
- Tests follow `test_{feature}_{unit|integration}.py` naming
- Keep imports simple, avoid circular dependencies

## Submitting Changes

1. Fork the repo
2. Create a branch (`git checkout -b my-feature`)
3. Write tests for your changes
4. Run the test suite
5. Submit a PR with a clear description of what and why

## Adding a Database Provider

This is the most impactful contribution. See [docs/extending.md](docs/extending.md) for the full guide. The short version:

1. Create `src/arrowjet/providers/mydb.py` with `read_bulk()` and `write_bulk()` methods
2. Add `"mydb"` to the `Engine.__init__` provider routing
3. Add unit tests with mocked connections
4. Add integration tests against a real database
5. Update README with the new provider

Look at `providers/postgresql.py` (simplest) or `providers/mysql.py` as templates.

## Questions?

Open an issue or start a discussion on GitHub.
