import sys
import os
from . import core

if __name__ == "__main__":
    """
    Allows running the package as a module:
    python -m gdoc_converter

    Or with a custom config:
    python -m gdoc_converter /path/to/my_config.json
    """

    # Determine the config path
    default_config_path = os.path.join(os.getcwd(), "config.json")

    if len(sys.argv) > 1:
        config_path = sys.argv[1]
    else:
        config_path = default_config_path

    if not os.path.exists(config_path):
        print(f"Error: Configuration file not found: {config_path}", file=sys.stderr)
        sys.exit(1)

    exit_code = core.main(config_path)
    sys.exit(exit_code)