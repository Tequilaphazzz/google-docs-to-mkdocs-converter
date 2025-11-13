import sys
import os
from gdoc_converter import core

if __name__ == "__main__":
    """
    Main entry point for running the converter.
    Expects config.json to be in the same directory.

    You can pass an alternative path to the config:
    python run.py /path/to/my_config.json
    """

    # Determine the config path
    default_config_path = os.path.join(os.path.dirname(__file__), "config.json")

    if len(sys.argv) > 1:
        config_path = sys.argv[1]
    else:
        config_path = default_config_path

    if not os.path.exists(config_path):
        print(f"Error: Configuration file not found: {config_path}", file=sys.stderr)
        sys.exit(1)

    # Run the main application core
    exit_code = core.main(config_path)
    sys.exit(exit_code)