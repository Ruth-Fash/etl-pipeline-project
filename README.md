# etl-pipeline-project

## CSV ETL Pipeline (CLI-based)

 Café Sales ETL Pipeline (CLI-based with Pandas & PostgreSQL)

This project is a CLI-based ETL (Extract, Transform, Load) pipeline built in Python using **Pandas**.

It is designed for a café that collects daily sales data in CSV format. At the end of each day, the café staff upload their raw sales CSV into a designated folder.Using the CLI, they can then run the pipeline to:

- **Extract**: Load raw sales data from the uploaded CSV file.
- **Transform**: Clean and standardize the data (remove null values, fix column names, format data).
- **Load**:
  - Save the transformed data into a new CSV file for archiving and backup.
  - Upload the cleaned dataset into a **PostgreSQL database** for reporting, analysis, or integration.

The pipeline allows the café team to run each step (Extract, Transform, Load) individually when needed, giving them flexibility and control over their data processing.


## Table of Contents

- [Features](#features)
- [Technologies Used](#technologies-used)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [CLI Usage](#cli-usage)
- [Example Workflow](#example-workflow)
- [Input &amp; Output Schema](#input--output-schema)
- [Archiving](#archiving)s
- [Logging &amp; Error Handling](#logging--error-handling)
- [Troubleshooting](#troubleshooting)
- [Future Improvements](#future-improvements)
- [License](#license)
- [Author](#author)

## Features

* **Stepwise CLI interface** : Run Extract, Transform, and Load independently, offering flexibility and control over the data processing workflow.
* **Data Extraction** : Loads raw sales data from multiple CSV files within a designated folder and combines them into a single pandas DataFrame.
* **Data Normalization** : The pipeline normalizes the single, unnormalized sales data into three separate, structured tables:** **`products`,** **`orders`, and** **`order_items`**.
* **Data Transformation** :
  * Generates universally unique identifiers (UUIDs) for orders, products, and individual order items.
  * Cleans and standardizes data by handling white spaces, converting data types, and splitting concatenated "Drinks Ordered" entries into individual rows.
  * Uses** ****Fuzzy Matching** with the** **`rapidfuzz`** library to correct misspelled product and branch names against a list of valid options.
  * Handles and removes rows with missing product or price data, saving them to a separate file (**`rows_with_missing.csv`**) for review.
* **Data Validation** : A crucial step using the **`pandera` library to validate the schema of each transformed table (`products`,** **`orders`, and** **`order_items`**) before loading, ensuring data integrity and consistency.
* **Data Loading** :
  * The transformed data is saved to new CSV files in a **`transformed_data`** folder for archiving and backup.
  * Loads the cleaned and validated tables into a** ****PostgreSQL** database for reporting and analysis.
* **Folder-based Workflow and Archiving** : Follows a clear folder-based workflow (**`raw_data`,** **`extracted_data`,** **`transformed_data`). After each successful ETL step, the processed data folders are moved to their respective** **`archive`** directories, ensuring a clean and manageable file system.
* **Robust Error Handling** : Includes **`try-except` blocks to handle various errors, such as** **`FileNotFoundError`,** **`EmptyDataError`, and** **`SchemaError`,** with informative messages to guide the user.

## Technologies Used

* **Python** : The core programming language for the entire ETL pipeline.
* **`pandas`** : A powerful data manipulation and analysis library used extensively for data extraction, cleaning, and transformation.
* **`pandera`** : A data validation library used to validate the structure and content of the pandas DataFrames against a predefined schema.
* **`rapidfuzz`** : A library for fast fuzzy string matching, utilized to correct spelling mistakes in product and branch names.
* **`uuid`** : The standard Python library for generating universally unique identifiers to create unique keys for orders, products, and order items.
* **`pathlib`** : A modern library for handling filesystem paths in an object-oriented way, simplifying file and directory operations.
* **`shutil`** : The standard library for high-level file operations, used to move and archive files and directories after each ETL step.
* **`os` and** **`sys`** : Standard libraries for interacting with the operating system and controlling the application's runtime environment.
* **`PostgreSQL`** : The relational database management system where the cleaned and normalized data is loaded for long-term storage and analysis.

## Prerequisites

* **Python 3.x** : Ensure you have a compatible version of Python installed.
* **Docker and Docker Compose** : These are required to run the PostgreSQL database and Adminer containers. Use the provided **`docker-compose.yml`** file to launch your database environment
* **PostgreSQL** : You will need a running PostgreSQL database to load the final, transformed data.
* **Database Credentials** : The project's loading function requires database connection details (host, user, password, database name). These must be configured for the **`load_to_database`** function to work correctly.
* **Dependencies** : All necessary Python libraries are included in the **`requirements.txt`** file and will be installed during the **`Installation`** process.
* **Files and Folders** : The repository already includes the necessary folder structure and data files (**`valid_branch_list.csv` and** **`valid_drinks_list.csv`**), so no manual creation or download is required.

## Installation

git clone 

create venv and instlal depenscies using requirment.txt

create .env file
