# Build an enterprise application that extracts data from multiple sources, transform and load them into data warehouse without using third party python library.

# Step 1 - Extract data
# Step 2 - Transform
# Step 3 - Load into Google Bigquery (data warehouse)

import csv
import datetime
import json
import re
import xml.etree.ElementTree as ET

# Extract Data


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

    # Load the txt file
    elif file_format == "txt":
        try:
            with open(file_path, "r") as txt_file:
                content = txt_file.read()

                # Split the content by employee_id to separate the individual flaggged prompts
                flagged_prompts = content.split("Employee ID:")[
                    1:
                ]  # Skip the first empty string

                flagged_prompts_obj = []

                for flagged_prompt in flagged_prompts:
                    # split each flagged_prompt
                    items = str(flagged_prompt).strip().split("\n")

                    # create the dictionary object
                    prompt_dict = {
                        "employee_id": items[0].strip(),
                        "timestamp": items[1].split("Timestamp: ")[1].strip(),
                        "prompt": items[2].split("Prompt: ")[1].strip(),
                    }
                    flagged_prompts_obj.append(prompt_dict)
            return flagged_prompts_obj

        except FileNotFoundError:
            print(f"Error reading TXT file: {e}")
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

        # Use a dictionary to store unique customer records, using a composite key
        # Using a combination of 'first_name', 'last_name', and 'phone_number' as the key
        def merge_dictionaries(dicts: list) -> dict:
            """
            Merge multiple dictionaries into one.
            In case of key conflicts, later dictionaries take precedence.

            Args: dicts: list of dictionaries to merge.

            Returns:
            dict: A merged dictionary.
            """
            merged_dict = {}
            for d in dicts:
                merged_dict.update(d)
            return merged_dict

        # Create a lookup dictionary for json and xml and txt_data
        json_lookup = {
            (row["customer_type"], row["first_name"], row["last_name"]): row
            for row in json_data
            if row
            and "customer_type" in row
            and "first_name" in row
            and "last_name" in row
        }
        xml_lookup = {
            (
                row["first_name"],
                row["last_name"],
                row["phone_no"],
            ): row
            for row in xml_data
            if row and "phone_no" in row and "first_name" in row and "last_name" in row
        }

        joined_data: list[dict] = []
        for customer in csv_data:
            customer_type = customer.get("customer_type", "")
            first_name = customer.get("first_name", "")
            last_name = customer.get("last_name", "")
            phone_number = customer.get("phone_number", "")

            # Search for the json info
            json_info = json_lookup.get((customer_type, first_name, last_name), {})
            xml_info = xml_lookup.get((first_name, last_name, phone_number), {})

            # Merge the data from all sources
            merged_row = merge_dictionaries([customer, json_info, xml_info])

            # Append the merged row if something is there
            if merged_row:
                joined_data.append(merged_row)

        return joined_data


def extract_data(file_paths: str):
    """
    Extracts data from a file and returns a dictionary with the data.

    Input: file_paths - path to the file
    Output: list object with the data
    """
    csv_data = None
    json_data = None
    xml_data = None
    txt_data = None

    # Extract data from each file based on its type
    for file_path in file_paths:
        file_format = file_path.split(".")[-1].lower()
        if file_format == "csv":
            csv_data = extracts(file_path)
        elif file_format == "json":
            json_data = extracts(file_path)
        elif file_format == "xml":
            xml_data = extracts(file_path)
        elif file_format == "txt":
            txt_data = extracts(file_path)
        else:
            print(f"Unsupported file format for: {file_path}")

    return csv_data, json_data, xml_data, txt_data


# Step 3 - Load (Todo)


def main(file_paths: list):
    """
    Main function to extract, and transform.

    Input: file_paths - list of file paths
    Output: transformed data
    """
    csv_data, json_data, xml_data, txt_data = extract_data(file_paths)

    # Instantiate the data transformer
    transformer = DataTransformer()

    # Apply transformations to CSV data
    standardized_csv = None
    if csv_data:
        # Apply phone number and date formatting to CSV data
        for row in csv_data:
            if (
                "phone_number" in row and row["phone_number"] is not None
            ):  # Check if 'phone_number' is not None
                row["phone_number"] = transformer.format_phone_number(
                    row["phone_number"]
                )
            if "dob" in row and row["dob"] is not None:  # Check if 'dob' is not None
                row["dob"] = transformer.format_date(row["dob"])

        flattened_csv = transformer.flatten_nested_dictionaries(csv_data)
        standardized_csv = transformer.standardize_keys(flattened_csv)

    # Apply transformations to XML data (assuming phone number formatting is needed)
    standardized_xml = None
    if xml_data:
        for row in xml_data:
            if (
                "PhoneNo" in row and row["PhoneNo"] is not None
            ):  # Check if 'PhoneNo' is not None
                row["PhoneNo"] = transformer.format_phone_number(row["PhoneNo"])

        flattened_xml = transformer.flatten_nested_dictionaries(xml_data)
        standardized_xml = transformer.standardize_keys(flattened_xml)

    # Apply transformations to JSON data
    standardized_json = None
    if json_data:
        flattened_json = transformer.flatten_nested_dictionaries(json_data)
        standardized_json = transformer.standardize_keys(flattened_json)

    # Apply transformations to TXT data
    standardized_txt = None
    if txt_data:
        flattened_txt = transformer.flatten_nested_dictionaries(txt_data)
        standardized_txt = transformer.standardize_keys(flattened_txt)

    # Join the data using the DataTransformer class
    joined_data = transformer.join(
        standardized_csv, standardized_json, standardized_xml
    )

    # Masked the card details and the cvv
    transformed_data = transformer.mask_card_details_cvv(joined_data)

    # For now, let's just return the transformed data for demonstration
    return (
        standardized_csv,
        standardized_json,
        standardized_xml,
        standardized_txt,
        transformed_data,
    )


file_paths = [
    "/content/drive/MyDrive/customers_billing.xml",
    "/content/drive/MyDrive/customers.csv",
    "/content/drive/MyDrive/customers_subscriptions.json",
]

csv_data, json_data, xml_data, txt_data = extract_data(file_paths)

standardized_csv, standardized_json, standardized_xml, standardized_txt, joined_data = (
    main(file_paths)
)
