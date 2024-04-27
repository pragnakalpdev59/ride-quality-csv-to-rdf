```markdown
# CSV to RDF Conversion

## Introduction

This project facilitates the conversion of CSV (Comma Separated Values) data collected from sensors in vehicles into RDF (Resource Description Framework) format. RDF provides a standardized way to describe data and its relationships, enabling semantic interoperability and efficient data processing.

## Configuration

Before getting started, ensure to configure the project settings in the `config.ini` files located in various directories. Adjust the file paths according to your requirements and obtain necessary API keys as instructed in the provided links.

If the `config.ini` file is not visible, check in `config/config.ini`.

## Getting Started

To begin using the project, follow these steps:

1. **Set Up Virtual Environment:**
   - Create a virtual environment and activate it to isolate project dependencies.

2. **Install Requirements:**
   - Use `pip` to install the required packages listed in `requirements.txt` within the virtual environment.

   ```bash
   pip install -r requirements.txt
   ```

   Note: On Raspberry Pi, some libraries may not install within a virtual environment. In such cases, install the requirements directly without a virtual environment.

3. **Troubleshooting:**
   - If any issues arise during setup or execution, refer to the `app.log` file for error logs and debugging information.

## Project Components

### 1. Aggregation Point Directory

This directory is intended for deployment on the aggregation point machine. It includes the following functionalities:

- **Input/RDF Folder:**
  - Location for storing RDF files received from vehicles.
  - RDF files are processed to generate JSON files containing data updates, which are then used to update the aggregation point RDF file.
  - Historical data is archived in the `old_data` directory.

### 2. Data Reasoning and NLP

Deployed on the aggregation point machine, this component currently requires manual execution of `summary.py` to generate textual summaries from RDF data.

### 3. Processing on Vehicle

Installed on the Raspberry Pi of the vehicle, this component consists of the following files and functionalities:

- **CSV Handling (`csv_handling/csv_handler.py`):**
  - Monitors changes in the CSV file where sensor data is logged.

- **RDF Handling (`rdf_handling/rdf_generator.py`):**
  - Generates RDF graphs from CSV data.

- **Combined Triples:**
  - Additional files in the `rdf_handling` directory are used for creating combined triples files, although their use case is currently undeveloped.

- **Output Folder:**
  - Automatically created if it does not exist, to store generated RDF files.

Note: `file_upload()` function is currently commented out, intended for simulating communication between the vehicle and aggregation point via a Flask API. In IoT implementations, communication handling would differ.

### 4. Flask API

This folder is designed to reside on the main server hosting the website `https://roadsemantics.in/`. It facilitates the transmission of updated RDF files and text files from the aggregation point to the server.

## Conclusion

This README provides an overview of the project structure, functionalities, and setup instructions. For detailed implementation and usage guidance, refer to the individual files and directories within the project.
```

Feel free to adjust the content according to any additional details or changes you'd like to incorporate!
There are comments added everywhere check them to understand the code better.