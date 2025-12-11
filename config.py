import json


def read_config(config_path: str) -> dict:
    """Read configuration from a JSON file."""
    with open(config_path, "r") as file:
        config = json.load(file)
    return config


def parse_config(config: dict) -> dict:
    """Parse and validate the configuration dictionary."""
    # Check input file
    assert "filename" in config.keys(), "Config must contain 'filename' key."
    # TODO: Add more validation for filters and cleaners
    if "filters" not in config.keys():
        print("No filters found in config, using default settings")
        config["filters"] = {}
    if "cleaners" not in config.keys():
        print("No cleaners found in config, using default settings")
        config["cleaners"] = {}
    if "splitter" not in config.keys():
        config["splitter"] = {}
    else:
        assert "test_size" in config["splitter"].keys(), (
            "Splitter config must contain 'test_size' key."
        )
        assert "val_size" in config["splitter"].keys(), (
            "Splitter config must contain 'val_size' key."
        )
        assert "random_state" in config["splitter"].keys(), (
            "Splitter config must contain 'random_state' key."
        )
    # Check output directory
    assert "outname" in config.keys(), "Config must contain 'outname' key."
    if not os.path.exists(os.path.dirname(config["outname"])):
        os.makedirs(os.path.dirname(config["outname"]))
    return config


def load_config(config_path: str) -> dict:
    """Load and parse configuration from a JSON file."""
    config = read_config(config_path)
    config = parse_config(config)
    return config


if __name__ == "__main__":
    config = read_config("config.json")
    print(config)
