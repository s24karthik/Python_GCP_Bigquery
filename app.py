# The codebase to work with Google Bigquery #
from workflows import bq_base
from util import auth_token
from google.cloud import bigquery

# Getting connected to the client #
class connect():
    credientials, project_id = auth_token()
    client = bigquery.Client(project=project_id, credentials=credientials)
    print("Connection success: ", client)
    print("""Enter the option 1 - 4: 
                1. Test connect
                2. Retrieve data
                3. Extract data to .csv
                4. Extract data to .json
        """)
    val = int(input())
    if val == 1:
        # Bigquery test connect #
        bq_base.bq_test(client)

    elif val == 2:
        # Bigquery retrieve data in json format #
        bq_base.bq_retrieve(client)

    elif val == 3:
       # Extracting data from bq and to .csv storage file #
        bq_base.extract_csv(client)
    
    elif val == 4:
    # Extracting data from bq and to .json file #
        bq_base.extract_json(client)
        
connect