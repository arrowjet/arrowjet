import yaml
from pathlib import Path


def load_config(path: str = "config.yaml") -> dict:
    config_path = Path(__file__).parent / path
    if not config_path.exists():
        raise FileNotFoundError(
            f"Config file not found: {config_path}\n"
            "Copy config.example.yaml to config.yaml and fill in your details."
        )
    with open(config_path) as f:
        return yaml.safe_load(f)
