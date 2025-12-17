import requests
import msal
from dotenv import load_dotenv
import os
load_dotenv()

username=os.getenv('BI_USERNAME')
password=os.getenv('BI_PASSWORD')
def request_access_token():
    app_id = os.getenv('CLIENT_ID')
    tenant_id =os.getenv('TENANT_ID')

    authority_url = 'https://login.microsoftonline.com/' + tenant_id
    scopes = ['https://analysis.windows.net/powerbi/api/.default']

    # Step 1. Generate Power BI Access Token
    client = msal.PublicClientApplication(app_id, authority=authority_url)
    token_response = client.acquire_token_by_username_password(username=username, password=password, scopes=scopes)
    if not 'access_token' in token_response:
        raise Exception(token_response['error_description'])

    access_id = token_response.get('access_token')
    return access_id

access_id = request_access_token()

dataset_id = os.getenv('DATASET_ID')
endpoint = f'https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/refreshes'
headers = {
    'Authorization': f'Bearer ' + access_id
}

response = requests.post(endpoint, headers=headers)
if response.status_code == 202:
    print('Dataset refreshed')
else:
    print(response.reason)
    print(response.json())