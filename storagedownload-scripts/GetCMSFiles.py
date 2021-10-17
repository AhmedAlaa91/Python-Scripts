import os

import pyodbc
import pandas as pd



from azure.storage.blob import ContainerClient

from azure.storage.blob import BlobServiceClient
from azure.storage.blob import BlobClient


server = ''
database = ''
username = ''
password = ''
InitialCatalog=''
driver= '{ODBC Driver 17 for SQL Server}'
cnxn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

sql  =""" select wrapperid from gearup_partnerzone where source ='CMS' and FileType <> 'Link' and status ='PUBLISHED' and uploaded is null """


files_df =pd.read_sql_query(sql,cnxn)

#----------------------------------------------------------


from requests_oauth2client import OAuth2Client, ClientSecretPost, OAuth2ClientCredentialsAuth
import requests

import json



token_endpoint = "https://gu-squidex-prm.azurewebsites.net/identity-server/connect/token"
client = OAuth2Client(token_endpoint, ClientSecretPost("gearup-uat:reader", "xt2ircyd0zohpogxpdqjg9xzxq4isxxmyvhxunumld0x"))
#token = client.client_credentials(scope="squidex-api") # you may pass additional kw params such as resource, audience, or whatever your AS needs

auth = OAuth2ClientCredentialsAuth(client, scope="squidex-api") # you may additional kw params such as scope, resource, audience or whatever param the AS use to grant you access
response = requests.get("https://gu-squidex-prm.azurewebsites.net/api/content/gearup-uat/graphql", auth=auth)


query2="""
{
  findAssetContent(id:"6d9ad4f6-04fe-46a0-a80f-390e24394e9b") {
    id
    flatData {
      asset {
        id
        sourceUrl
        fileName
      }
    }
  }
}
"""
#----------------------------------------------------------

from azure.storage.blob import BlobSasPermissions
from azure.storage.blob._shared_access_signature import BlobSharedAccessSignature, generate_blob_sas
from datetime import datetime, timedelta

import requests

AZURE_ACC_NAME = 'gusquidexstoragepremprm'
AZURE_PRIMARY_KEY = 'PJkhgcKNbr9AkzeelA9XYEcHhucVxYHTh1/pQcPmuvhXaqg+poYOsENVXSQoaoMCUx4rYEuKshvdJHk/8tslxQ=='
AZURE_CONTAINER = 'etc-squidex-assets'
AZURE_BLOB='d3d6aa7f-eb8c-435d-badb-217531f695f1_0'
expiry= datetime.utcnow() + timedelta(minutes=5)

blobSharedAccessSignature = BlobSharedAccessSignature(AZURE_ACC_NAME, AZURE_PRIMARY_KEY)

sasToken = blobSharedAccessSignature.generate_blob(AZURE_CONTAINER, AZURE_BLOB, expiry=expiry, permission="r")



def get_blob_sas(account_name,account_key, container_name, blob_name):
    sas_blob = generate_blob_sas(account_name=account_name,
                                container_name=container_name,
                                blob_name=blob_name,
                                account_key=account_key,
                                permission=BlobSasPermissions(read=True),
                                expiry=datetime.utcnow() + timedelta(hours=1))

    return sas_blob


def get_content(filename,wrapperid):
    blob = get_blob_sas(AZURE_ACC_NAME,AZURE_PRIMARY_KEY, AZURE_CONTAINER, wrapperid)
    url = 'https://'+AZURE_ACC_NAME+'.blob.core.windows.net/'+AZURE_CONTAINER+'/'+wrapperid+'?'+blob






    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open('C:\\Users\\v-aelkholy\\Downloads\\' + filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                #if chunk:
                f.write(chunk)
    print(filename + "  " + "downlowaded")


#----------------------------------------------------------


def upload_file( file_name,wrapperid):
    # Create blob with same name as local file name
    block_blob_service = BlobServiceClient.from_connection_string(
        "DefaultEndpointsProtocol=https;AccountName=korrecordingstorage;AccountKey=J0XvBKlMs4/PJWTpD6MWKKkgVSmXTu59wKJczhzCohdFdhzu3xBXNiJX0Oq6KPOlxrSf2BbILYnIRlO2BO2USw==;EndpointSuffix=core.windows.net")

    generator = block_blob_service.get_container_client("gearupfiles")
    generator.get_blob_client(blob=file_name)

    # Get full path to the file
    upload_file_path = os.path.join("C:\\Users\\v-aelkholy\\Downloads\\", file_name)

    # Create blob on storage
    # Overwrite if it already exists!

    print(f"uploading file - {file_name}")
    with open(upload_file_path, "rb") as data:
        generator.upload_blob(data=data,name=file_name, overwrite=True,blob_type="BlockBlob")
    os.remove('C:\\Users\\v-aelkholy\\Downloads\\' + filename )
    sql_query ="update gearup_partnerzone set uploaded='yes' where wrapperid= ? "
    cursor.execute(sql_query, wrapperid)
    cursor.commit()

    print(filename + "  " + "uploaded")

#----------------------------------------------------------
for index, row in files_df.iterrows():
    query3 = query2.replace("6d9ad4f6-04fe-46a0-a80f-390e24394e9b", str(row['wrapperid']))
    r = requests.post("https://gu-squidex-prm.azurewebsites.net/api/content/gearup-uat/graphql", json={'query': query3},
                      auth=auth)
    y = json.loads(r.text)

    content = str(y["data"]["findAssetContent"])

    if (content != "None" ):
        sourceurl = str(y["data"]["findAssetContent"]["flatData"]["asset"][0]["sourceUrl"]).replace(
            "https://gusquidexstoragepremprm.blob.core.windows.net/etc-squidex-assets/", "")
        filename = y["data"]["findAssetContent"]["flatData"]["asset"][0]["fileName"]



        get_content(filename, sourceurl)
        upload_file(filename, str(row['wrapperid']))




