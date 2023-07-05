# Bigquery workflows small snippets #
from google.cloud import bigquery

def bq_test(client):
    query = "Select current_date() AS Current_Date"
    query_result = client.query(query)
    print("Connection success", query_result.result())

def bq_retrieve(client):
    query = input("Enter the query: ")
    df = client.query(query).to_dataframe()
    df.to_json(path_or_buf='< SINK RESULT_DATA PATH >', orient='table')
    print("Data Fetched")

def extract_csv(client):
    table_id = input("Enter the Dataset-table ID: ")
    gcs_path = input("Enter the GCS path: ")
    result = client.extract_table(table_id, gcs_path)
    print("Extraction Result: ", result)

def extract_json(client):
    table_id = input("Enter the Dataset-table ID: ")
    gcs_path = input("Enter the GCS path: ")
    job_config = bigquery.ExtractJobConfig()
    job_config.destination_format = bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON
    result = client.extract_table(table_id, gcs_path, job_config=job_config)
    print("Extraction Result: ", result)