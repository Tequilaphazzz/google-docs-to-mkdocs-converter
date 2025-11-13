import os
import paramiko
import logging
from .exceptions import ConfigurationError

logger = logging.getLogger(__name__)


class SFTPConfig:
    """Data class for SFTP configuration"""

    def __init__(self, host, port, user, password):
        self.host = host
        self.port = port
        self.user = user
        self.password = password

    def validate(self):
        """Validate SFTP configuration"""
        if not all([self.host, self.user, self.password]):
            raise ConfigurationError("SFTP configuration is incomplete")
        if not isinstance(self.port, int) or self.port <= 0:
            raise ConfigurationError("SFTP port must be a positive integer")


def sftp_upload_file(local_path: str, remote_path: str, sftp_config: SFTPConfig):
    """
    Upload a single file to a remote server via SFTP
    """
    transport = None
    sftp = None

    try:
        # Establish connection
        transport = paramiko.Transport((sftp_config.host, sftp_config.port))
        transport.connect(username=sftp_config.user, password=sftp_config.password)
        sftp = paramiko.SFTPClient.from_transport(transport)

        # Create remote directory if it doesn't exist
        remote_dir = os.path.dirname(remote_path)
        _create_remote_directory(sftp, remote_dir)

        # Upload file
        sftp.put(local_path, remote_path)
        logger.info(f"File uploaded successfully: {local_path} -> {remote_path}")

    except Exception as e:
        logger.error(f"SFTP upload failed for {local_path}: {e}")
        raise
    finally:
        if sftp:
            sftp.close()
        if transport:
            transport.close()


def sftp_upload_directory(local_dir: str, remote_dir: str, sftp_config: SFTPConfig):
    """
    Upload an entire directory to a remote server via SFTP
    """
    transport = None
    sftp = None

    try:
        transport = paramiko.Transport((sftp_config.host, sftp_config.port))
        transport.connect(username=sftp_config.user, password=sftp_config.password)
        sftp = paramiko.SFTPClient.from_transport(transport)

        # Create root remote directory
        _create_remote_directory(sftp, remote_dir)

        # Walk through and upload all files
        for root, dirs, files in os.walk(local_dir):
            for file in files:
                local_file_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_file_path, local_dir)
                remote_file_path = os.path.join(remote_dir, relative_path).replace("\\", "/")

                remote_file_dir = os.path.dirname(remote_file_path)
                if remote_file_dir != remote_dir:
                    _create_remote_directory(sftp, remote_file_dir)

                sftp.put(local_file_path, remote_file_path)
                logger.debug(f"Uploaded file: {local_file_path} -> {remote_file_path}")

        logger.info(f"Directory {local_dir} uploaded successfully to {remote_dir}")

    except Exception as e:
        logger.error(f"SFTP directory upload failed for {local_dir}: {e}")
        raise
    finally:
        if sftp:
            sftp.close()
        if transport:
            transport.close()


def _create_remote_directory(sftp, remote_dir: str):
    """
    Recursively create a remote directory.
    """
    dirs = remote_dir.strip('/').split('/')
    current_dir = ''
    if remote_dir.startswith('/'):
        current_dir = '/'

    for dir_name in dirs:
        if not dir_name:
            continue
        current_dir = os.path.join(current_dir, dir_name).replace("\\", "/")
        try:
            sftp.chdir(current_dir)
        except IOError:
            logger.debug(f"Creating remote directory: {current_dir}")
            sftp.mkdir(current_dir)
            sftp.chdir(current_dir)