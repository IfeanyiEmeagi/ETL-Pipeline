# Enterprise Customer Data ETL Pipeline

This project demonstrates a robust, enterprise-grade ETL (Extract, Transform, Load) pipeline built with Python, orchestrated by Prefect, and designed to load data into Google BigQuery. It solves a common business problem of consolidating disparate customer data sources into a single, clean, and actionable dataset.

## The Problem: A Fragmented View of the Customer

Imagine a growing company, "OmniRetail," that uses different systems to manage its operations:
*   **Sales System**: Exports basic customer information as a `.csv` file.
*   **Marketing Platform**: Provides customer subscription details in `.json` format.
*   **Legacy Billing System**: Generates billing and payment information as an `.xml` file.

This data fragmentation makes it impossible to get a unified 360-degree view of the customer. Answering simple questions like "Who are our most valuable subscribers?" or "What is the preferred payment method for customers in London?" is a manual, error-prone, and time-consuming task.

This ETL pipeline was built to solve that exact problem by automating the process of data consolidation.

## Features

*   **Multi-Source Extraction**: Ingests data from various file formats (`.csv`, `.json`, `.xml`).
*   **Robust Transformations**:
    *   Standardizes field names to a consistent `snake_case` format.
    *   Cleans and formats data (e.g., standardizing UK phone numbers, formatting dates).
    *   Masks sensitive Personally Identifiable Information (PII) like credit card numbers and CVVs.
    *   Joins data from all sources to create a unified customer profile.
*   **Modern Workflow Orchestration**: Uses **Prefect** to build a resilient, observable, and reliable dataflow.
*   **Cloud Data Warehousing**: Loads the final, clean data into **Google BigQuery** for scalable analytics.

## Technology Stack

*   **Language**: Python 3.13
*   **Orchestration**: Prefect 2.0
*   **Data Warehouse**: Google BigQuery
*   **Core Libraries**: No third-party data manipulation libraries like Pandas were used, demonstrating foundational data processing skills.

## ETL Pipeline Overview

The pipeline is orchestrated as a Prefect flow with three main stages:

1.  **`Extract`**:
    *   A dynamic task reads data from each source file (`.csv`, `.json`, `.xml`).
    *   Each file is processed in parallel.

2.  **`Transform`**:
    *   The raw data from all sources is passed to a central transformation task.
    *   It performs all the cleaning, standardization, masking, and joining logic.
    *   A unique `customer_id` is generated for each unified record.

3.  **`Load`**:
    *   The transformed, clean list of customer records is loaded into a `customers_transformed` table in Google BigQuery.
    *   The table is created if it doesn't exist and is overwritten on each run (`WRITE_TRUNCATE`).

## Setup and Installation

Follow these steps to get the project running on your local machine.

### Prerequisites
*   Python 3.13+
*   A Google Cloud Platform (GCP) project.
*   `gcloud` CLI installed and authenticated.
*   A GCP Service Account with "BigQuery Data Editor" and "BigQuery Job User" roles.

### 1. Clone the Repository
```bash
git clone <your-repository-url>
cd ETL_Project
```

### 2. Install Dependencies
It's recommended to use a virtual environment.
```bash
python -m venv venv
source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
pip install -r requirements.txt
```
*(Note: You will need to create a `requirements.txt` file containing `prefect`, `prefect-gcp`, and `google-cloud-bigquery`)*

### 3. Configure Prefect GCP Credentials
Create a Prefect credentials block to securely store your GCP service account key.

1.  Start the Prefect UI:
    ```bash
    prefect server start
    ```
2.  Navigate to **Blocks** > **Create Block** > **GCP Credentials**.
3.  Give it a **Block Name** of `gcp-etl-creds`.
4.  Open your downloaded service account JSON key file and copy its contents into the **Service Account Info** field.
5.  Click **Create**.

## How to Run the Pipeline

Modify the `PROJECT_ID` and `DATASET_ID` constants at the bottom of `prefect_etl_pipeline.py` with your GCP project and desired BigQuery dataset name.

Then, execute the script:
```bash
python prefect_etl_pipeline.py
```

You can monitor the run in real-time from the Prefect UI (usually at `http://127.0.0.1:4200`). After a successful run, you can query the `customers_transformed` table in your BigQuery dataset.

---

## License

This project is licensed under the Apache License 2.0. See the [LICENSE](LICENSE) file for details.
