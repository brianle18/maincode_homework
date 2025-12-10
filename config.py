import json


def read_config(config_path: str) -> dict:
    """Read configuration from a JSON file."""
    with open(config_path, "r") as file:
        config = json.load(file)
    return config


def parse_config(config: dict) -> None:
    """Parse and validate the configuration dictionary."""
    # TODO: Add more validation
    assert "filename" in config.keys(), "Config must contain 'filename' key."
    if "filters" not in config.keys():
        print("No filters found in config, using default settings")
        config["filters"] = {}
    if "cleaners" not in config.keys():
        print("No cleaners found in config, using default settings")
        config["cleaners"] = {}
    return config


def load_config(config_path: str) -> dict:
    """Load and parse configuration from a JSON file."""
    config = read_config(config_path)
    config = parse_config(config)
    return config


if __name__ == "__main__":
    config = read_config("config.json")
    print(config)
