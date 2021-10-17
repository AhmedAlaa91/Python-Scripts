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

sql  =""" select wrapperid  , replace(trim(Filename),'|','and') as title ,'gearup.microsoft.com/download/' + wrapperid as ExternalUrl from gearup_partnerzone where FileType <> 'Link' and status ='PUBLISHED'   and trim(title) ='10 Azure Stack HCI Facts'"""


files_df =pd.read_sql_query(sql,cnxn)


#----------------------------------------------------------

from azure.storage.blob import BlobSasPermissions
from azure.storage.blob._shared_access_signature import BlobSharedAccessSignature, generate_blob_sas
from datetime import datetime, timedelta

import requests

AZURE_ACC_NAME = 'azurepartners'
AZURE_PRIMARY_KEY = 's80+pMFBV9a5bh2qg5A1g98bXnyAEuXL2ZOo9Dk4W3Ow1MlUQiHRX17ITwC9ACC33/EIwSzMy0TQ9Y405INZtQ=='
AZURE_CONTAINER = 'gearupfiles'
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


def get_content(filename,wrapperid,url):
    blob = get_blob_sas(AZURE_ACC_NAME,AZURE_PRIMARY_KEY, AZURE_CONTAINER, wrapperid)






    with requests.get('https://'+url, stream=True) as r:
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
        "DefaultEndpointsProtocol=https;AccountName=azurepartners;AccountKey=s80+pMFBV9a5bh2qg5A1g98bXnyAEuXL2ZOo9Dk4W3Ow1MlUQiHRX17ITwC9ACC33/EIwSzMy0TQ9Y405INZtQ==;EndpointSuffix=core.windows.net")

    generator = block_blob_service.get_container_client("gearupfiles")
    generator.get_blob_client(blob=file_name)

    # Get full path to the file
    upload_file_path = os.path.join("C:\\Users\\v-aelkholy\\Downloads\\", file_name)

    # Create blob on storage
    # Overwrite if it already exists!

    print(f"uploading file - {file_name}")
    with open(upload_file_path, "rb") as data:
        generator.upload_blob(data=data,name=file_name, overwrite=True,blob_type="BlockBlob")
    generator.get_blob_client(blob=file_name).set_blob_metadata({"wrapperid": wrapperid})
    os.remove('C:\\Users\\v-aelkholy\\Downloads\\' + file_name )
    #sql_query ="update gearup_partnerzone set uploaded='yes' where wrapperid= ? "
    #cursor.execute(sql_query, wrapperid)
    #cursor.commit()

    print(file_name + "  " + "uploaded")

#----------------------------------------------------------
for index, row in files_df.iterrows():
    get_content(str(row['title']), str(row['wrapperid']),str(row['ExternalUrl']))
    upload_file(str(row['title']), str(row['wrapperid']))
