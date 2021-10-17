import pyodbc
import pandas as pd

from datetime import datetime




server = ''
database = ''
username = ''
password = ''
InitialCatalog=''
driver= '{ODBC Driver 17 for SQL Server}'
#cnxn = pyodbc.connect(''DRIV'ER={ODBC Driver 13 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cnxn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()


sql_main = '''  Select ScheduleUniqueID , EventDate , [workshop status] as workshop,InstructorEmail, Country , Area , InstructorOrg , PartnerName , PartnerID
From vw_All_AIW_CL_Sch_DelSch
where PartnerName is null and instructororg is not null
 '''

df_main =pd.read_sql_query(sql_main,cnxn)


sql_check1=""" Select  distinct top 1 InstructorEmail ,InstructorOrg ,PartnerName ,PartnerID ,Country ,Area 
 from Cloudlabs_Schedule_fuzzy
 where (LOWER(PartnerName) like lower( ? ) or lower(InstructorOrg) like lower( ? )) """


sql_check2=""" Select  distinct top 1 InstructorEmail ,InstructorOrg ,PartnerName ,PartnerID ,Country ,Area 
 from Cloudlabs_Schedule_fuzzy
 where (LOWER(PartnerName) like lower( ? ) or lower(InstructorOrg) like lower( ? )) """





if not df_main.empty :

    for index, row in df_main.iterrows():
        ScheduleUniqueID = str(row['ScheduleUniqueID'])
        df_check1 = pd.read_sql_query(sql_check1, cnxn, params=['%'+str(row['InstructorOrg'])+'%','%'+str(row['InstructorOrg'])+'%'])
        if  not df_check1.empty:
            print(ScheduleUniqueID)
            instructororg = str(row['InstructorOrg'])
            InstructorEmail = str(row['InstructorEmail'])
            InstructorOrg = str(row['InstructorOrg'])
            Country = str(row['Country'])
            Area = str(row['Area'])
            for index, row in df_check1.iterrows():

                PartnerName = str(row['PartnerName'])
                PartnerID = str(row['PartnerID'])



                sql_insert = " insert into Cloudlabs_Schedule_fuzzy (ScheduleUniqueID, InstructorEmail, InstructorOrg, PartnerName, PartnerID,Country, Area) values (?, ?, ?, ?, ?,?, ?) "
                cursor.execute(sql_insert, ScheduleUniqueID, InstructorEmail, instructororg, PartnerName, PartnerID,
                               Country, Area)
                cursor.commit()












