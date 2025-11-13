# 🚀 Google Docs to MkDocs Converter

This ETL script automates the process of converting Google Docs into Markdown (with HTML elements), asynchronously downloads all related images, and uploads the final `.md` files and image folders to a remote server via SFTP.

The project is designed to automate documentation updates for platforms like **MkDocs**.

## 🔗 Central Management Sheet

The entire process is centrally managed via a Google Sheet. You will need to make a copy of this template or configure your own sheet based on it.

**Template Link:**
`https://docs.google.com/spreadsheets/d/1h_t9wM_3MpNVzVtmFmoGFa_8h8UXGbV6pLu9b4BIduU/edit`

This sheet contains two required tabs:

1.  **`Credentials`**: Stores your SFTP credentials (host, port, username, password).
2.  **`Links`**: Contains the list of documents to process. The script will only process rows where the `D (Publish)` column is set to `TRUE` (or `YES`, `✓`, etc.).

-----

## ✨ Key Features

  * 📜 **Batch Processing:** Automatically scans the Google Sheet and processes all documents marked for publication.
  * 🔄 **Advanced Conversion:**
      * Converts Google Doc content into clean Markdown.
      * Correctly converts Google Docs tables (including `rowspan` and `colspan`) into an **HTML** representation, as standard Markdown does not support complex tables.
      * Handles lists (nested, numbered, bulleted).
      * Recognizes `inline-code` (by its gray background) and code blocks (by style).
  * ⚡ **Asynchronous Image Downloading:** Uses `asyncio` and `aiohttp` to download dozens of images in parallel, significantly speeding up the process.
  * ⬆️ **SFTP Upload:** Automatically uploads the generated `.md` file and its accompanying `images/` folder to the correct directory on a remote server.
  * 🔗 **Link Handling:** Converts internal links to headings and bookmarks into HTML anchors (`{#slug}`) compatible with MkDocs.
  * 🐘 **Large Document Support:** Can check a document's size and process it in chunks to avoid API timeouts or limits.

-----

## 💻 Tech Stack

  * **Python 3.8+**
  * **Google API:**
      * `google-api-python-client` (for Google Docs & Google Sheets API)
      * `google-auth-oauthlib` (for OAuth2 authentication)
  * **Asynchronous Requests:**
      * `asyncio`
      * `aiohttp` (for fast image downloading)
  * **SFTP:**
      * `paramiko` (for connecting and uploading files to the server)

-----

## 🧭 How It Works (Overview)

1.  The script is launched via `run.py`.
2.  Configuration is loaded from `config.json` (Sheet ID, paths, timeouts).
3.  Authentication with the Google API occurs via `credentials.json` and `token.json` (OAuth2).
4.  The script accesses the Google Sheet (ID from the config):
      * Reads SFTP credentials from the `Credentials` tab.
      * Reads the list of documents to process from the `Links` tab.
5.  For each document in the list:
      * The `converters.main_converter` module is executed.
      * The document is parsed, and all image information is collected.
      * Images are asynchronously downloaded to a temporary folder.
      * Text, lists, and tables are converted to Markdown/HTML.
      * Post-processing is applied (cleanup, anchor generation).
      * The final `.md` file is saved to the temporary folder.
6.  The `sftp.py` module connects to the server and uploads the `.md` file and the `images/` folder to the directory specified in the sheet.
7.  The temporary folder is deleted.

-----

## 📂 Project Structure

The project has a modular structure for easy maintenance and testing.

```bash
google-docs-to-mkdocs-converter/
│
├── gdoc_converter/         # The main package source code
│   ├── __init__.py
│   ├── __main__.py         # Allows running as a package: `python -m gdoc_converter`
│   ├── core.py             # The main orchestrator, contains the main() function
│   ├── config.py           # Class for loading and validating config.json
│   ├── exceptions.py       # Custom exceptions (DocumentSizeError, etc.)
│   ├── google_services.py  # All functions for interacting with Google APIs
│   ├── sftp.py             # Functions and class for SFTP operations
│   ├── utils.py            # Helper utilities (run_async, normalize_filename)
│   │
│   └── converters/         # 🧠 The Core: parsing and conversion logic
│       ├── __init__.py
│       ├── main_converter.py # Main converter module (chunking, assembly)
│       ├── images.py         # Async image downloading
│       ├── lists.py          # Processing for bulleted and numbered lists
│       ├── tables.py         # Conversion of tables to HTML
│       ├── text.py           # Processing text, links, and inline code
│       └── post_processor.py # Final cleanup and formatting for Markdown
│
├── run.py                  # 🚀 The main file to run the script
├── config.json             # ⚙️ Your configuration file (must be created)
├── requirements.txt        # 📦 Python dependency list
├── .gitignore              # 🙈 File to ignore secrets
│
├── credentials.json        # (Secret, generated by Google, not in Git)
└── token.json              # (Secret, generated on first run, not in Git)
```

-----

## 🛠️ Setup and Usage Guide

### Step 1: Clone and Install Dependencies

1.  Clone the repository:
    ```bash
    git clone https://github.com/your-username/google-docs-to-mkdocs-converter.git
    cd google-docs-to-mkdocs-converter
    ```
2.  Install all required dependencies:
    ```bash
    pip install -r requirements.txt
    ```

### Step 2: Configure Google API

1.  Go to the [Google Cloud Console](https://console.cloud.google.com/).
2.  Create a new project (or select an existing one).
3.  Enable the **Google Docs API** and **Google Sheets API** for your project.
4.  Go to "Credentials".
5.  Create an "OAuth client ID".
6.  Select "Desktop app" as the application type.
7.  Download the JSON file with your credentials.
8.  Rename this file to `credentials.json` and **place it in the project root**.

### Step 3: Configure the Google Sheet

1.  **Make a copy** of [this template](https://docs.google.com/spreadsheets/d/1h_t9wM_3MpNVzVtmFmoGFa_8h8UXGbV6pLu9b4BIduU/edit).
2.  **Fill out the `Credentials` tab**:
      * `B1`: SFTP User
      * `B2`: SFTP Password
      * `B3`: SFTP Host (e.g., `sftp.example.com`)
      * `B4`: SFTP Port (usually `22`)
3.  **Fill out the `Links` tab**:
      * `Column A (Doc URL)`: The URL of your Google Doc.
      * `Column B (Remote Dir)`: The path on the SFTP server (e.g., `/var/www/mkdocs/docs/guides`).
      * `Column C (Custom Filename)`: (Optional) The desired filename (e.g., `my-guide.md`). If left empty, a name will be generated from the document's title.
      * `Column D (Publish)`: Set to `TRUE`, `YES`, or `✓` for the script to process this row.

### Step 4: Create `config.json`

Create a `config.json` file in the project root. It must contain your Google Sheet ID and other settings.

**Example `config.json`:**

```json
{
  "google_api": {
    "sheet_id": "1I2fJDAXP2ZyOLJA4iVxX7BbLNBEO_5j7AmYtcuxjtlw",
    "scopes": [
      "https://www.googleapis.com/auth/documents.readonly",
      "https://www.googleapis.com/auth/drive.readonly",
      "https://www.googleapis.com/auth/spreadsheets.readonly"
    ],
    "token_file": "token.json",
    "credentials_file": "credentials.json",
    "sheet_ranges": {
      "credentials": "Credentials!B1:B4",
      "links": "Links!A:D"
    }
  },
  "conversion": {
    "max_document_size_chars": 150000,
    "chunk_size_chars": 50000,
    "code_fonts": [ "consolas", "courier", "monaco", "monospace" ],
    "inline_code_marker_rgb_float": 217.0,
    "inline_code_marker_tolerance": 0.25,
    "code_block_marker_char": ""
  },
  "network": {
    "request_timeout_seconds": 120,
    "max_concurrent_image_downloads": 3
  },
  "logging": {
    "level": "INFO",
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
  }
}
```

**Important:** Replace `"sheet_id"` with the ID of your Google Sheet from Step 3.

### Step 5: First Run (Authentication)

The first time you run the script, it will open a browser window for Google authentication.

1.  Run the script:
    ```bash
    python run.py
    ```
2.  A browser window will open asking for permissions.
3.  **Choose the Google account** that has access to both the Google Doc and the Google Sheet.
4.  Grant all requested permissions.
5.  After successful authentication, a `token.json` file will be created in the project root. This file will be used for all future runs.

### Step 6: Normal Operation

Once `token.json` is created, all subsequent runs will execute automatically without opening a browser.

```bash
python run.py
```


The script will now process the documents according to your Google Sheet.
