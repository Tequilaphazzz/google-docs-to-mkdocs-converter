import json
import logging
from typing import Dict, Any, List
from .exceptions import ConfigurationError

logger = logging.getLogger(__name__)

class Config:
    """
    Loads and stores application configuration from a JSON file.
    Provides access to settings via properties.
    """
    def __init__(self, config_path: str):
        if not config_path:
             # This path is used for a special case in chunked processing
             # where we create a modified temporary config object
            self.data: Dict[str, Any] = {}
            return
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                self.data: Dict[str, Any] = json.load(f)
            logger.info(f"Configuration loaded successfully from {config_path}")
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {config_path}")
            raise ConfigurationError(f"Configuration file not found: {config_path}")
        except json.JSONDecodeError:
            logger.error(f"Error decoding JSON from file: {config_path}")
            raise ConfigurationError(f"Error decoding JSON: {config_path}")

    # --- Google API ---
    @property
    def google_sheet_id(self) -> str:
        return self.data['google_api']['sheet_id']

    @property
    def google_scopes(self) -> List[str]:
        return self.data['google_api']['scopes']

    @property
    def google_token_file(self) -> str:
        return self.data['google_api']['token_file']

    @property
    def google_creds_file(self) -> str:
        return self.data['google_api']['credentials_file']

    @property
    def google_range_credentials(self) -> str:
        return self.data['google_api']['sheet_ranges']['credentials']

    @property
    def google_range_links(self) -> str:
        return self.data['google_api']['sheet_ranges']['links']

    # --- Conversion ---
    @property
    def max_doc_size(self) -> int:
        return self.data['conversion']['max_document_size_chars']

    @property
    def chunk_size(self) -> int:
        return self.data['conversion']['chunk_size_chars']

    @property
    def code_fonts(self) -> List[str]:
        return self.data['conversion']['code_fonts']

    @property
    def inline_code_rgb(self) -> float:
        # Value from config (e.g., 217.0) is divided by 255.0, as in the original script
        return self.data['conversion']['inline_code_marker_rgb_float'] / 255.0

    @property
    def inline_code_tolerance(self) -> float:
        return self.data['conversion']['inline_code_marker_tolerance']

    @property
    def code_block_marker(self) -> str:
        return self.data['conversion']['code_block_marker_char']

    # --- Network ---
    @property
    def request_timeout(self) -> int:
        return self.data['network']['request_timeout_seconds']

    @property
    def max_concurrent_downloads(self) -> int:
        return self.data['network']['max_concurrent_image_downloads']

    # --- Logging ---
    @property
    def log_level(self) -> str:
        return self.data['logging']['level']

    @property
    def log_format(self) -> str:
        return self.data['logging']['format']