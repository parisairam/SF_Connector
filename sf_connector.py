import requests
import logging
import io 
import os
from datetime import datetime
import csv
import xml.etree.ElementTree as ET
import re
import time


class SalesforceDataExtractor:
    def __init__(self, credentials_file, object_name=None, soql_query=None, api_mode='rest', pk_chunking=False,
                 chunk_params=None, output_file='output.txt', output_format='txt', delimiter='~|',
                 batch_size=200, concurrency_mode="Parallel"):
        self.credentials = self.load_credentials(credentials_file)
        self.client_id = self.credentials['client_id']
        self.client_secret = self.credentials['client_secret']
        self.username = self.credentials['username']
        self.password = self.credentials['password']
        self.security_token = self.credentials['security_token']

        self.object_name = object_name
        self.soql_query = soql_query
        self.api_mode = api_mode.lower()  # 'rest' or 'bulk'
        self.pk_chunking = pk_chunking
        self.chunk_params = chunk_params or {}
        self.output_file = output_file
        self.output_format = output_format.lower()  # 'txt', 'csv', 'xml'
        self.delimiter = delimiter
        self.batch_size = batch_size
        self.concurrency_mode = concurrency_mode

        self.access_token = None
        self.instance_url = None

        self.setup_logging()
        self.test_logging()

    def setup_logging(self):
        log_dir = "logs"
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
    
        current_log = os.path.join(log_dir, "current_log.log")
        if os.path.exists(current_log):
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            os.rename(current_log, os.path.join(log_dir, f"history_{timestamp}.log"))
    
        # Reset logging handlers to avoid duplicates
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
    
        logging.basicConfig(
            level=logging.DEBUG,  # Capture all levels
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(current_log),  # File handler for logging to file
                logging.StreamHandler()           # Stream handler for console output
            ]
    )

        
    def test_logging(self):
        logging.info("This is a test log entry.")
        logging.warning("Test warning log.")
        logging.error("Test error log.")
    
    def load_credentials(self, credentials_file):
        try:
            credentials = {}
            with open(credentials_file, 'r') as file:
                for line in file:
                    key, value = line.strip().split('=')
                    credentials[key.strip()] = value.strip()
            logging.info("Credentials loaded successfully.")
            return credentials
        except Exception as e:
            logging.error(f"Error reading credentials file: {str(e)}")
            raise

    def authenticate(self):
        auth_url = "https://cognizant-3ca-dev-ed.develop.my.salesforce.com/services/oauth2/token"
        payload = {
            'grant_type': 'password',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'username': self.username,
            'password': f"{self.password}{self.security_token}"
        }

        try:
            logging.info("Authenticating with Salesforce...")
            response = requests.post(auth_url, data=payload)
            response.raise_for_status()

            auth_response = response.json()
            self.access_token = auth_response['access_token']
            self.instance_url = auth_response['instance_url']
            logging.info("Authentication successful.")
        except requests.exceptions.HTTPError as http_err:
            logging.error(f"HTTP error during authentication: {http_err.response.text}")
            raise
        except Exception as err:
            logging.error(f"General error during authentication: {str(err)}")
            raise

    def describe_object(self):
        if not self.object_name:
            logging.error("Object name must be provided for describe call.")
            raise ValueError("Object name is required if SOQL is not provided.")

        describe_url = f"{self.instance_url}/services/data/v52.0/sobjects/{self.object_name}/describe"
        headers = {
            'Authorization': f"Bearer {self.access_token}",
            'Content-Type': 'application/json'
        }

        try:
            logging.info(f"Describing object: {self.object_name}")
            response = requests.get(describe_url, headers=headers)
            response.raise_for_status()

            fields = response.json()['fields']
            field_names = [field['name'] for field in fields]
            compound_fields = [field['name'] for field in fields if field['compoundFieldName']]

            logging.info(f"Retrieved {len(field_names)} fields for {self.object_name}.")
            logging.info(f"Compound fields detected: {compound_fields}.")
            return field_names, compound_fields
        
        except requests.exceptions.HTTPError as http_err:
            logging.error(f"HTTP error during describe call: {http_err.response.text}")
            raise
        except Exception as err:
            logging.error(f"General error during describe call: {str(err)}")
            raise


    def extract_fields_from_soql(self, soql_query):
        field_match = re.findall(r"SELECT (.*?) FROM", soql_query, re.IGNORECASE)
        if field_match:
            fields = [field.strip() for field in field_match[0].split(',')]
            logging.info(f"Fields extracted from SOQL: {fields}")
            return fields
        else:
            logging.error("Failed to extract fields from SOQL query.")
            raise ValueError("Invalid SOQL query format.")

    def check_compound_fields_in_soql(self, fields, compound_fields):
        compound_fields_in_query = [field for field in fields if field in compound_fields]

        if compound_fields_in_query:
            logging.warning(f"Compound field(s) found in SOQL: {compound_fields_in_query}. Switching to REST API.")
            self.api_mode = 'rest'
        else:
            logging.info("No compound fields in the SOQL query. Proceeding with Bulk API.")


    def fetch_data_rest(self, fields):
        query = self.soql_query or f"SELECT {','.join(fields)} FROM {self.object_name}"
        headers = {
            'Authorization': f"Bearer {self.access_token}",
            'Content-Type': 'application/json'
        }

        try:
            logging.info(f"Executing REST query: {query}")
            response = requests.get(
                f"{self.instance_url}/services/data/v52.0/query", headers=headers, params={'q': query})
            response.raise_for_status()
            data = response.json()
            logging.info(f"REST API fetched {len(data.get('records', []))} records.")
            return data.get('records', [])
        except requests.exceptions.HTTPError as http_err:
            logging.error(f"HTTP error during REST query: {http_err.response.text}")
            raise
        except Exception as err:
            logging.error(f"General error during REST query: {str(err)}")
            raise

    def fetch_data_bulk(self, fields):
    # Initiate Bulk Query
        job_url = f"{self.instance_url}/services/data/v52.0/jobs/query"
        headers = {
            'Authorization': f"Bearer {self.access_token}",
            'Content-Type': 'application/json'
        }
        query = self.soql_query or f"SELECT {','.join(fields)} FROM {self.object_name}"

        try:
            logging.info("Starting Bulk query job.")
            job_payload = {"query": query, "operation": "query"}
            job_response = requests.post(job_url, headers=headers, json=job_payload)
            job_response.raise_for_status()

            job_id = job_response.json().get('id')
            logging.info(f"Bulk Job {job_id} created. Waiting for completion...")

            # Check job status
            while True:
                status_response = requests.get(f"{job_url}/{job_id}", headers=headers)
                status_response.raise_for_status()
                status = status_response.json()
                if status['state'] == 'JobComplete':
                    logging.info("Bulk query job completed.")
                    break
                elif status['state'] in ['Failed', 'Aborted']:
                    raise Exception(f"Bulk Job failed with state: {status['state']}")
                time.sleep(5)

            # Download results
            result_url = f"{job_url}/{job_id}/results"
            result_response = requests.get(result_url, headers=headers)
            result_response.raise_for_status()
            csv_data = result_response.content.decode('utf-8')

            # Parse CSV data
            csv_reader = csv.DictReader(io.StringIO(csv_data))
            records = [row for row in csv_reader]
            logging.info(f"Fetched {len(records)} records via Bulk API.")
            return records

        except requests.exceptions.HTTPError as http_err:
            logging.error(f"HTTP error during Bulk query: {http_err.response.text}")
            raise
        except Exception as err:
            logging.error(f"General error during Bulk query: {str(err)}")
            raise


    def save_to_file(self, records):
        if not records:
            logging.warning("No records to write.")
            return

        # Exclude 'attributes' column
        filtered_records = [{k: v for k, v in record.items() if k != 'attributes'} for record in records]

        if self.output_format == 'txt':
            self.write_txt(self.output_file, filtered_records)
        elif self.output_format == 'csv':
            self.write_csv(self.output_file, filtered_records)
        elif self.output_format == 'xml':
            self.write_xml(self.output_file, filtered_records)
        else:
            logging.error("Unsupported output format.")
            raise ValueError("Invalid output format specified.")

    def write_txt(self, filename, data):
        if not data:
            logging.warning("No data to write.")
            return

        # Open the file in write mode
        with open(filename, 'w') as txtfile:
            # Write the data with the user-defined delimiter
            for record in data:
                # Use the delimiter for each field in the record
                line = self.delimiter.join(str(record.get(field, '')) for field in record.keys())
                txtfile.write(f"{line}\n")
    
        logging.info(f"Data successfully written to {filename} in TXT format.")

    def write_csv(self, filename, data):
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
        logging.info(f"Data successfully written to {filename} in CSV format.")

    def write_xml(self, filename, data, root_element="Records", record_element="Record"):
        root = ET.Element(root_element)
        for record in data:
            record_node = ET.SubElement(root, record_element)
            for key, value in record.items():
                child = ET.SubElement(record_node, key)
                child.text = str(value)
        tree = ET.ElementTree(root)
        tree.write(filename, encoding='utf-8', xml_declaration=True)
        logging.info(f"Data successfully written to {filename} in XML format.")



    def run(self):
        try:
            self.authenticate()

            if self.soql_query:
                # Extract fields from the SOQL query and check for compound fields
                fields = self.extract_fields_from_soql(self.soql_query)
                _, compound_fields = self.describe_object()
                self.check_compound_fields_in_soql(fields, compound_fields)
            else:
                # If no SOQL query, just describe the object and check for compound fields
                fields, compound_fields = self.describe_object()
                # Switch to REST API automatically if compound fields are found
                if compound_fields:
                    logging.warning(f"Compound fields detected: {compound_fields}. Switching to REST API.")
                    self.api_mode = 'rest'

            # Fetch data using either REST or Bulk API based on the determined mode
            if self.api_mode == 'bulk':
                records = self.fetch_data_bulk(fields)
            else:
                records = self.fetch_data_rest(fields)

            # Save the fetched data to a file
            self.save_to_file(records)

        except Exception as e:
            logging.error(f"Process failed: {str(e)}")
            raise





# Example usage
if __name__ == "__main__":
    extractor = SalesforceDataExtractor(
        credentials_file="creds.txt",
        object_name="Account",
        soql_query=None,
        api_mode='bulk',
        output_file='Account.csv',
        output_format='txt',
        delimiter='|'
    )
    extractor.run()