import os
import logging
from .config import Config
from .exceptions import ConfigurationError
from .sftp import SFTPConfig

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.discovery import build as build_google_sheet

logger = logging.getLogger(__name__)

def get_credentials_from_sheet(config: Config, creds: Credentials) -> SFTPConfig:
    """
    Get SFTP credentials from the 'Credentials' sheet in Google Sheet
    """
    try:
        service = build_google_sheet('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()

        # Get data from the Credentials sheet
        result = sheet.values().get(
            spreadsheetId=config.google_sheet_id,      # Use config
            range=config.google_range_credentials   # Use config
        ).execute()

        values = result.get('values', [])

        if len(values) < 4:
            raise ConfigurationError("Insufficient data in Credentials sheet. Expected values in B1:B4")

        sftp_user = values[0][0] if values[0] else None
        sftp_pass = values[1][0] if values[1] else None
        sftp_host = values[2][0] if len(values) > 2 and values[2] else None
        sftp_port_str = values[3][0] if len(values) > 3 and values[3] else None

        if not sftp_user or not sftp_pass:
            raise ConfigurationError("SFTP credentials are empty in Credentials sheet")

        if not sftp_host:
            raise ConfigurationError("SFTP host is empty in Credentials sheet (cell B3)")

        if not sftp_port_str:
            raise ConfigurationError("SFTP port is empty in Credentials sheet (cell B4)")

        try:
            sftp_port = int(sftp_port_str)
        except ValueError:
            raise ConfigurationError(f"SFTP port must be a number, got: {sftp_port_str}")

        sftp_config = SFTPConfig(sftp_host, sftp_port, sftp_user, sftp_pass)
        sftp_config.validate()

        logger.info(f"Successfully loaded SFTP configuration: {sftp_user}@{sftp_host}:{sftp_port}")
        return sftp_config

    except Exception as e:
        logger.error(f"Failed to get credentials from sheet: {e}")
        raise ConfigurationError(f"Cannot load SFTP configuration: {e}")


def authenticate(config: Config) -> Credentials:
    """Authenticate with Google APIs using OAuth2."""
    creds = None
    scopes = config.google_scopes
    token_file = config.google_token_file
    creds_file = config.google_creds_file

    try:
        if os.path.exists(token_file):
            creds = Credentials.from_authorized_user_file(token_file, scopes)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                logger.info("Refreshing expired credentials...")
                creds.refresh(Request())
            else:
                logger.info("Starting local server for OAuth flow...")
                flow = InstalledAppFlow.from_client_secrets_file(creds_file, scopes)
                creds = flow.run_local_server(port=0)

            with open(token_file, 'w') as token:
                token.write(creds.to_json())

        logger.info("Successfully authenticated with Google APIs")
        return creds

    except Exception as e:
        logger.error(f"Authentication failed: {e}")
        raise ConfigurationError(f"Google API authentication failed: {e}")


def get_gdocs_batch_from_sheet(config: Config, creds: Credentials) -> list:
    """
    Get a batch of Google Docs from the 'Links' sheet in Google Sheet
    """
    try:
        service = build_google_sheet('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()

        result = sheet.values().get(
            spreadsheetId=config.google_sheet_id,  # Use config
            range=config.google_range_links      # Use config
        ).execute()

        values = result.get('values', [])
        output = []

        if not values:
            logger.warning("No data found in Links sheet")
            return output

        # Process each row (skip header)
        for row_index, row in enumerate(values[1:], start=2):
            try:
                if len(row) >= 2:
                    # Check the flag in column D (index 3)
                    publish_flag = row[3] if len(row) >= 4 and row[3].strip() else False

                    if publish_flag and publish_flag.upper() in ['TRUE', 'YES', '1', 'ON', '✓', 'ДА']:
                        doc_url = row[0].strip() if row[0] else None
                        remote_dir = row[1].strip() if row[1] else None
                        file_name = row[2].strip() if len(row) >= 3 and row[2] else None

                        if doc_url and remote_dir:
                            output.append((doc_url, remote_dir, file_name))
                        else:
                            logger.warning(f"Row {row_index}: missing URL or remote directory")

            except Exception as e:
                logger.error(f"Error processing row {row_index}: {e}")
                continue

        logger.info(f"Loaded {len(output)} documents from Links sheet")
        return output

    except Exception as e:
        logger.error(f"Failed to get documents list from sheet: {e}")
        raise ConfigurationError(f"Cannot load documents list: {e}")


def get_document_name(document_id: str, creds: Credentials) -> str:
    """
    Get the title (name) of a Google Doc
    """
    try:
        docs_service = build('docs', 'v1', credentials=creds)
        doc = docs_service.documents().get(documentId=document_id, fields='title').execute()
        title = doc.get('title', 'output')
        logger.debug(f"Retrieved document title: {title}")
        return title
    except Exception as e:
        logger.error(f"Failed to get document title for {document_id}: {e}")
        raise