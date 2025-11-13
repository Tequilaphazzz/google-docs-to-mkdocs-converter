import os
import logging
import tempfile
import shutil
import time

from .config import Config
from .exceptions import ConfigurationError
from . import google_services
from . import sftp as sftp_client
from .utils import normalize_filename, run_async_in_thread, extract_gdoc_id_from_url
from .converters.main_converter import convert_gdoc_to_markdown_large

# Get the root logger
logger = logging.getLogger()


def setup_logging(config: Config):
    """Sets up the basic logging configuration."""
    logging.basicConfig(
        level=config.log_level.upper(),
        format=config.log_format
    )
    # Suppress overly verbose loggers from Google libraries
    logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)
    logging.getLogger('google.auth.transport.requests').setLevel(logging.WARNING)


def main(config_path: str) -> int:
    """Main function that orchestrates the conversion process."""
    try:
        # 1. Load config and set up logging
        config = Config(config_path)
        setup_logging(config)
        logger.info("=== Starting Google Docs to Markdown converter ===")

        # 2. Authenticate with Google
        logger.info("Authenticating with Google APIs...")
        creds = google_services.authenticate(config)

        # 3. Get SFTP configuration
        logger.info("Loading SFTP configuration from Google Sheet...")
        sftp_config = google_services.get_credentials_from_sheet(config, creds)

        # 4. Get the list of documents
        logger.info("Loading document list from Google Sheet...")
        jobs = google_services.get_gdocs_batch_from_sheet(config, creds)

        if not jobs:
            logger.warning("No documents found for processing.")
            return 0

        logger.info(f"Found {len(jobs)} documents to process")

        # 5. Process each document
        for job_index, (doc_url, remote_dir, custom_filename) in enumerate(jobs, start=1):
            logger.info(f"\n=== Processing document {job_index}/{len(jobs)} ===")
            logger.info(f"URL: {doc_url}")
            logger.info(f"Remote directory: {remote_dir}")

            try:
                # Validate URL and extract ID
                if not doc_url or not ("docs.google.com" in doc_url or len(doc_url) == 44):
                    logger.error(f"Invalid Google Docs URL or ID: {doc_url}")
                    continue

                doc_id = extract_gdoc_id_from_url(doc_url)
                logger.info(f"Document ID: {doc_id}")

                if not doc_id or len(doc_id) < 20:
                    logger.error(f"Could not extract valid document ID from: {doc_url}")
                    continue

                # Determine filename
                if custom_filename and custom_filename.strip():
                    md_filename = custom_filename.strip()
                    if not md_filename.endswith('.md'):
                        md_filename += '.md'
                    image_prefix = md_filename.replace('.md', '')
                else:
                    title = google_services.get_document_name(doc_id, creds)
                    md_filename = normalize_filename(title) + ".md"
                    image_prefix = normalize_filename(title)

                logger.info(f"Filename: {md_filename}")

                # Create temporary directory
                temp_dir = tempfile.mkdtemp()
                images_dir = os.path.join(temp_dir, "images")
                md_path = os.path.join(temp_dir, md_filename)

                # --- Run conversion ---
                # Use our async runner
                run_async_in_thread(
                    convert_gdoc_to_markdown_large,
                    doc_id, md_path, images_dir, creds, config, image_prefix
                )

                # --- Upload to SFTP ---
                remote_md_path = os.path.join(remote_dir, md_filename).replace("\\", "/")
                sftp_client.sftp_upload_file(md_path, remote_md_path, sftp_config)

                if os.path.exists(images_dir) and os.listdir(images_dir):
                    remote_images_dir = os.path.join(remote_dir, "images").replace("\\", "/")
                    sftp_client.sftp_upload_directory(images_dir, remote_images_dir, sftp_config)
                    logger.info(f"Images directory uploaded to {remote_images_dir}")

                logger.info(f"✅ Document {job_index} processed successfully")

                # Clean up
                shutil.rmtree(temp_dir)
                time.sleep(2)  # Pause between documents

            except Exception as e:
                logger.error(f"❌ Failed to process document {job_index} ({doc_url}): {e}", exc_info=True)
                continue

        logger.info("=== Batch processing complete! ===")

    except ConfigurationError as e:
        logger.critical(f"Configuration error: {e}")
        return 1
    except Exception as e:
        logger.critical(f"Unexpected error: {e}", exc_info=True)
        return 1

    return 0