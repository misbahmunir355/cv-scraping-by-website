# Rozee.pk CV Downloader

A Python script to download CVs from Rozee.pk with robust error handling and resume capabilities.

![Python](https://img.shields.io/badge/python-3.7+-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)

## Features

- Downloads CVs by city and keyword search
- Automatic retries with exponential backoff
- Resume interrupted downloads
- Duplicate file handling (skip/rename/overwrite)
- Detailed logging
- SQLite database tracking
- Configurable settings

## Prerequisites

- Python 3.7+
- Required packages:

##File Structure 
rozee-cv-downloader/
├── jobsearch.py            # Main script
├── config.json             # Configuration file
├── cv_downloader.db        # SQLite database (created automatically)
├── cv_downloader.log       # Log file (created automatically)
├── downloaded_cvs/         # Directory for downloaded CVs
│   └── [City Name]/        # Organized by city
├── README.md               # This file
└── requirements.txt        # Dependencies

-Disclaimer
This project is for educational purposes only. The author is not responsible for any misuse of this tool or violation of terms of service.
