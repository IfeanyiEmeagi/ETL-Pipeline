# Build an enterprise application that extracts data from multiple sources, transform and load them into data warehouse without using third party python library.

# Step 1 - Extract data
# Step 2 - Transform
# Step 3 - Load into Google Bigquery (data warehouse)

import csv
import datetime
import json
import re
import xml.etree.ElementTree as ET

try:
    from google.cloud import bigquery
    from google.cloud.exceptions import NotFound as BigQueryNotFound
except ImportError:
    print(
        "Warning: google-cloud-bigquery is not installed. The 'load' step will not work."
    )

from prefect import task, flow
from prefect_gcp.credentials import GcpCredentials
from prefect.futures import PrefectFuture


def extracts(file_path: str) -> list:
    """
    Extracts data from a file and returns a dictionary with the data.

    Input: file_path - path to the file
    Output: list object with the data
    """
    file_format = file_path.split(".")[-1].lower()

    # Extract the CSV file
    if file_format == "csv":
        data = []
        try:
            with open(file_path, "r") as file:
                reader = csv.reader(file)
                header = next(reader)  # Read the header row
                for row in reader:
                    data_dict = {}  # Initialize dictionary for each row
                    for i in range(len(row)):
                        data_dict[header[i]] = row[i]
                    data.append(data_dict)  # Append the complete row dictionary
        except Exception as e:
            print(f"Error reading CSV file: {e}")
        return data

    # Extract the JSON file
    elif file_format == "json":
        try:
            with open(file_path, "r") as file:
                data = json.load(file)
        except Exception as e:
            print(f"Error reading JSON file: {e}")
        return data

    # Extract the XML file
    elif file_format == "xml":
        try:
            tree = ET.parse(file_path)
            root = tree.getroot()
            # Extract customer data
            customer_billing_data = []
            for customer in root.findall("Customer"):
                billing_info = {}

                # Extract direct child elements and handle nested ones
                for child in customer:
                    if len(child) > 0:  # Check if the child element has nested elements
                        for nested_child in child:
                            billing_info[f"{child.tag}_{nested_child.tag}"] = (
                                nested_child.text or ""
                            )
                    else:
                        billing_info[child.tag] = child.text or ""

                customer_billing_data.append(billing_info)
            return customer_billing_data
        except Exception as e:
            print(f"Error reading XML file: {e}")
    else:
        print("Format not supported")


# Step 2 - Transformation


class DataTransformer:
    def snake_case(self, text: str) -> str:
        """Converts a string to snake_case."""
        text = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", text)
        text = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", text)
        return text.lower()

    def flatten_nested_dictionaries(self, data: list) -> list:
        """
        Flatten nested dictionaries within a list of dictionaries.

        Input: data - list of dictionaries
        Output: list of dictionaries with flattened nested dictionaries
        """
        flattened_data = []
        for row in data:
            new_row = {}
            for key, value in row.items():
                if isinstance(value, dict):
                    for sub_key, sub_value in value.items():
                        new_row[f"{key}_{sub_key}"] = sub_value
                else:
                    new_row[key] = value
            flattened_data.append(new_row)
        return flattened_data

    def standardize_keys(self, data: list) -> list:
        """
        Standardize dictionary keys to snake_case.

        Input: data - list of dictionaries
        Output: list of dictionaries with standardized keys
        """
        standardized_data = []
        for row in data:
            new_row = {}
            for key, value in row.items():
                new_key = self.snake_case(key)
                new_row[new_key] = value
            standardized_data.append(new_row)
        return standardized_data

    def standardize_phone_key(self, data: list) -> list:
        """
        Standardizes the phone number key from 'phone_no' to 'phone_number'.

        Args:
            data (list): A list of dictionaries.

        Returns:
            list: The list of dictionaries with the phone key standardized.
        """
        for row in data:
            if "phone_no" in row:
                row["phone_number"] = row.pop("phone_no")
        return data

    def format_phone_number(self, phone_number: str) -> str | None:
        """
        Formats a UK phone number to +44 XXXX XXX XXX format.

        Input: phone_number - Input phone number string
        Output: Formatted UK phone number or None if invalid
        """
        # Remove all non-digit characters
        digits = re.sub(r"\D", "", phone_number)

        # Handle different length scenarios for UK phone numbers
        if len(digits) < 10 or len(digits) > 14:
            return None

        # If number starts with international code, remove it
        if digits.startswith("44"):
            digits = digits[2:]

        # If number starts with 0, remove it for standard formatting
        if digits.startswith("0"):
            digits = digits[1:]

        # Ensure we have exactly 10 digits
        if len(digits) != 10:
            return None

        # Format UK phone number: +44 XXXX XXX XXX
        formatted_number = f"+44 {digits[:4]} {digits[4:7]} {digits[7:]}"

        return formatted_number

    def format_date(
        self,
        date_input: str,
        input_format: str = "%Y-%m-%d",
        output_format: str = "%Y-%m-%d",
    ) -> str | None:
        """
        Formats a date string from one format to another.

        Input:
            date_input (str): Input date string
            input_format (str): Format of the input date string (default: "%Y-%m-%d")
            output_format (str): Desired format for the output date string (default: "%Y-%m-%d")

        Output:
            str: Formatted date string or None if invalid
        """
        try:
            date_obj = datetime.datetime.strptime(date_input, input_format)
            formatted_date = date_obj.strftime(output_format)
            return formatted_date
        except ValueError:
            return None

    def mask_card_details_cvv(self, merged_data: list) -> list:
        """
        Mask the card details and leave that last 4 and the cvv and leave the last digit

        Input: merged_data - list of dictionaries
        Output: list of dictionaries with masked card details
        """
        masked_data = []

        for row in merged_data or []:
            new_row = row.copy()
            card_number = new_row.get("card_details__card_number", "")
            if card_number and isinstance(card_number, str):
                new_row["card_details__card_number"] = (
                    "*" * (len(new_row["card_details__card_number"]) - 4)
                    + new_row["card_details__card_number"][-4:]
                )

            cvv = new_row.get("card_details_cvv", "")
            if cvv and isinstance(cvv, str):
                new_row["card_details_cvv"] = (
                    "*" * (len(new_row["card_details_cvv"]) - 1)
                    + new_row["card_details_cvv"][-1:]
                )

            masked_data.append(new_row)

        return masked_data

    def add_customer_id(self, data: list) -> list:
        """
        Adds a sequential customer_id to each record, starting from 1.

        Args:
            data (list): A list of dictionaries representing customer records.

        Returns:
            list: The list of dictionaries with 'customer_id' added to each.
        """
        data_with_id = []
        for i, row in enumerate(data, start=1):
            row["customer_id"] = i
            data_with_id.append(row)
        return data_with_id

    def join(self, csv_data: list, json_data: list, xml_data: list) -> list:
        """
        Joins data from different sources based on common keys, prioritizing CSV data and merging others.

        Input:
            csv_data (list): Data extracted from CSV
            json_data (list): Data extracted from JSON
            xml_data (list): Data extracted from XML

        Output:
            list: Joined data with unique customer information
        """

        customers = []

        # Populate customers dictionary with CSV data
        for record in csv_data or []:
            customers.append(
                record.copy()
            )  # Create a copy to avoid modifying the original list

        # Merge JSON data
        for record in json_data or []:
            # Find the matching record in customers based on first_name, last_name, and customer_type
            for customer_record in customers:
                if (
                    customer_record.get("first_name", "")
                    == record.get("first_name", "")
                    and customer_record.get("last_name", "")
                    == record.get("last_name", "")
                    and customer_record.get("customer_type", "")
                    == record.get("customer_type", "")
                    and customer_record.get("company_name", "")
                    == record.get("company_name", "")
                ):
                    customer_record.update(record)

        # Merge XML data
        for record in xml_data or []:
            # Find the matching record in customers based on first_name, last_name, and phone_number or company_name
            for customer_record in customers:
                if (
                    customer_record.get("first_name", "")
                    == record.get("first_name", "")
                    and customer_record.get("last_name", "")
                    == record.get("last_name", "")
                    and customer_record.get("phone_number", "")
                    == record.get("phone_number")
                ):
                    customer_record.update(record)

        return customers


# Step 3 - Load


def load_to_bigquery(
    data: list[dict],
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    table_id: str,  # This argument is now passed from the flow
    schema: list[bigquery.SchemaField],
):
    """
    Loads data into a specified BigQuery table.

    Creates the dataset and table if they don't exist.

    Args:
        data (list[dict]): The data to load.
        client (bigquery.Client): An authenticated BigQuery client.
        project_id (str): Your Google Cloud project ID.
        dataset_id (str): The BigQuery dataset ID.
        table_id (str): The BigQuery table ID.
        schema (list[bigquery.SchemaField]): The schema of the BigQuery table.
    """
    if not data:
        print(f"No data provided to load into {table_id}. Skipping.")
        return

    dataset_ref = client.dataset(dataset_id)
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset {dataset_id} already exists.")
    except BigQueryNotFound:
        print(f"Dataset {dataset_id} not found, creating it.")
        client.create_dataset(dataset_ref, exists_ok=True)

    table_ref = dataset_ref.table(table_id)
    try:
        client.get_table(table_ref)
        print(f"Table {table_id} already exists.")
    except BigQueryNotFound:
        print(f"Table {table_id} not found, creating it.")
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table, exists_ok=True)

    print(f"Loading data into {dataset_id}.{table_id}...")
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_TRUNCATE",  # Overwrite Or "WRITE_APPEND" to append
    )

    try:
        load_job = client.load_table_from_json(data, table_ref, job_config=job_config)
        load_job.result()  # Wait for the job to complete
        print(
            f"Successfully loaded {load_job.output_rows} rows into {dataset_id}.{table_id}."
        )
    except Exception as e:
        print(f"Failed to load data into BigQuery: {e}")
        if hasattr(e, "errors"):
            print("BigQuery errors:", e.errors)


@task(name="Define BigQuery Schema")
def get_customer_schema() -> list:
    """Returns the schema for the customer BigQuery table."""
    return [
        bigquery.SchemaField("customer_id", "INTEGER"),
        bigquery.SchemaField("customer_type", "STRING"),
        bigquery.SchemaField("first_name", "STRING"),
        bigquery.SchemaField("last_name", "STRING"),
        bigquery.SchemaField("company_name", "STRING"),
        bigquery.SchemaField("email", "STRING"),
        bigquery.SchemaField("phone_number", "STRING"),
        bigquery.SchemaField("dob", "DATE"),
        bigquery.SchemaField("sex", "STRING"),
        bigquery.SchemaField("subscription_type", "STRING"),
        bigquery.SchemaField("payment_method", "STRING"),
        bigquery.SchemaField("billing_address__street", "STRING"),
        bigquery.SchemaField("billing_address__city", "STRING"),
        bigquery.SchemaField("billing_address__postcode", "STRING"),
        bigquery.SchemaField("card_details__card_number", "STRING"),
        bigquery.SchemaField("card_details__expiry_date", "STRING"),
        bigquery.SchemaField("card_details_cvv", "STRING"),
    ]


@task(name="Extract Data Task")
def extract_task(file_path: str) -> list:
    """Prefect task to extract data from a single file."""
    print(f"Extracting data from: {file_path}")
    return extracts(file_path)


@task(name="Transform Data Task")
def transform_task(csv_data: list, json_data: list, xml_data: list) -> list:
    """
    Prefect task to transform and merge data from all sources.
    """
    print("Starting data transformation...")

    transformer = DataTransformer()

    try:
        # Ensure all inputs are lists of dicts, handling cases where a file might be missing
        csv_data = csv_data or []
        if isinstance(json_data, dict):
            json_data = [json_data]
        if isinstance(xml_data, dict):
            xml_data = [xml_data]

        # Apply transformations to CSV data
        standardized_csv = []
        if csv_data:
            for row in csv_data:
                if not isinstance(row, dict):
                    continue
                if "phone_number" in row and row["phone_number"] is not None:
                    row["phone_number"] = transformer.format_phone_number(
                        row["phone_number"]
                    )
                if "dob" in row and row["dob"] is not None:
                    row["dob"] = transformer.format_date(row["dob"])
            flattened_csv = transformer.flatten_nested_dictionaries(csv_data)
            standardized_csv = transformer.standardize_keys(flattened_csv)

        # Apply transformations to XML data
        standardized_xml = []
        if xml_data:
            for row in xml_data:
                if not isinstance(row, dict):
                    continue
                if "PhoneNo" in row and row["PhoneNo"] is not None:
                    row["PhoneNo"] = transformer.format_phone_number(row["PhoneNo"])
            flattened_xml = transformer.flatten_nested_dictionaries(xml_data)
            keys_standardized_xml = transformer.standardize_keys(flattened_xml)
            standardized_xml = transformer.standardize_phone_key(keys_standardized_xml)

        # Apply transformations to JSON data
        standardized_json = []
        if json_data:
            json_list = [r for r in json_data if isinstance(r, dict)]
            flattened_json = transformer.flatten_nested_dictionaries(json_list)
            standardized_json = transformer.standardize_keys(flattened_json)

        # Join the data using the DataTransformer class
        joined_data = transformer.join(
            standardized_csv, standardized_json, standardized_xml
        )

        # Add a sequential customer_id to the joined data
        data_with_id = transformer.add_customer_id(joined_data)

        # Mask the card details and the cvv
        transformed_data = transformer.mask_card_details_cvv(data_with_id)

        print("Transformation complete.")
        return transformed_data
    except Exception as e:
        print(f"Error during transformation: {e}")
        raise


@task(name="Load Data to BigQuery Task")
def load_task(
    data: list, project_id: str, dataset_id: str, table_id: str, schema: list
):
    """Prefect task to load data into BigQuery."""
    # Load the credentials block created in the Prefect UI
    gcp_credentials = GcpCredentials.load("gcp-etl-creds")

    # Initialize the BigQuery client with the loaded credentials
    client = bigquery.Client(
        project=project_id,
        credentials=gcp_credentials.get_credentials_from_service_account(),
    )

    load_to_bigquery(
        data=data,
        client=client,
        project_id=project_id,  # Still needed for some client operations
        dataset_id=dataset_id,
        table_id=table_id,
        schema=schema,
    )


@flow(name="ETL Pipeline Flow")
def etl_pipeline_flow(file_paths: list, project_id: str, dataset_id: str):
    """The main Prefect flow to orchestrate the ETL process."""

    # --- Extract ---
    # Use a dictionary to hold futures for extracted data
    extracted_data = {"csv": None, "json": None, "xml": None}
    for path in file_paths:
        file_format = path.split(".")[-1].lower()
        if file_format in extracted_data:
            # Submit task to run and store its future
            extracted_data[file_format] = extract_task(path)

    # --- Transform ---
    transformed_data = transform_task(
        csv_data=extracted_data["csv"],
        json_data=extracted_data["json"],
        xml_data=extracted_data["xml"],
    )

    # --- Load ---
    schema = get_customer_schema()
    load_task(
        data=transformed_data,
        project_id=project_id,
        dataset_id=dataset_id,
        table_id="customers_transformed",
        schema=schema,
    )


file_paths = [
    "data/customers_billing.xml",
    "data/customers.csv",
    "data/customers_subscriptions.json",
]


if __name__ == "__main__":
    PROJECT_ID = "etl-project-475811"  # <-- Project ID
    DATASET_ID = "customer_data_prefect"  # Using a new dataset for clarity

    etl_pipeline_flow(file_paths, PROJECT_ID, DATASET_ID)
