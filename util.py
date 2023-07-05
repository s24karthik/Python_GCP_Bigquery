# The Utility/support file to manage the workflow #
from google.oauth2 import service_account

# To get authorized to Bigquery client #
def auth_token():
    file = input("Enter the file path of service key: ")
    credientials = service_account.Credentials.from_service_account_file(file)
    project_id = credientials.project_id
    return credientials, project_id