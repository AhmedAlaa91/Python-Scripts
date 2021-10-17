
from __future__ import unicode_literals
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

sql  =""" select  WrapperId , trim(title) as title, replace(trim(title),'|' ,' ') as title2 ,ExternalUrl from gearup_partnerzone where source ='EXTERNAL' and status='PUBLISHED' and FileType='Video' and ExternalUrl like '%youtu%' and  ExternalUrl not like '%list%' and  uploaded is null and title <> 'Azure Cosmos DB - YouTube' and ExternalUrl <> 'www.youtube.com/watch' """


files_df =pd.read_sql_query(sql,cnxn)

#----------------------------------------------------------

def upload_image( file_name , wrapperid_v):
    # Create blob with same name as local file name
    block_blob_service = BlobServiceClient.from_connection_string(
        "DefaultEndpointsProtocol=https;AccountName=korrecordingstorage;AccountKey=J0XvBKlMs4/PJWTpD6MWKKkgVSmXTu59wKJczhzCohdFdhzu3xBXNiJX0Oq6KPOlxrSf2BbILYnIRlO2BO2USw==;EndpointSuffix=core.windows.net")

    generator = block_blob_service.get_container_client("gearupfiles")
    generator.get_blob_client(blob=file_name)

    # Get full path to the file
    upload_file_path = os.path.join("C:\\Users\\v-aelkholy\\Downloads", file_name+".mp4")

    # Create blob on storage
    # Overwrite if it already exists!

    print(f"uploading file - {file_name}")
    with open(upload_file_path, "rb") as data:
        generator.upload_blob(data=data,name=file_name, overwrite=True,blob_type="BlockBlob")
    generator.get_blob_client(blob=file_name).set_blob_metadata({"wrapperid": wrapperid_v})


def upload_all_images_in_folder():
    # Get all files with jpg extension and exclude directories
    all_file_names = [f for f in os.listdir("C:\\Users\\v-aelkholy\\Blobs\\finals")]

    # Upload each file
    for file_name in all_file_names:
        upload_image(file_name)



#----------------------------------------------------------


import youtube_dl






#----------------------------------------------------------
for index, row in files_df.iterrows():
    link=str(row['ExternalUrl'])
    fname=str(row['title'])
    fname2 = str(row['title2'])
    wrapperid=str(row['WrapperId'])
    ydl_opts = {'outtmpl': 'C:/Users/v-aelkholy/Downloads/' + fname2.replace('?','') + '.%(ext)s'}
    print(fname2)

    with youtube_dl.YoutubeDL(ydl_opts) as ydl:
        ydl.download([link])
        info_dict = ydl.extract_info(link, download=False)
        video_title = info_dict.get('title', None)

    file_title=video_title.replace('|','_')


    if file_title is not None :
        # YouTube(link).streams.get_lowest_resolution().download('C:\\Users\\v-aelkholy\\Downloads',fname2)

        upload_image(fname2.replace(':','#').replace('?','') , wrapperid)

        sql_query = "update gearup_partnerzone set uploaded='yes' where wrapperid= ? "
        cursor.execute(sql_query, wrapperid)
        cursor.commit()

        os.remove('C:\\Users\\v-aelkholy\\Downloads\\' + fname2.replace(':','#').replace('?','')+".mp4" )
        print("File Removed!")





