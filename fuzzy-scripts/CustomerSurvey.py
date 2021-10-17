import pyodbc
import pandas as pd
from fuzzywuzzy import fuzz

from datetime import datetime


def days_between(d1, d2):
    d1 = datetime.strptime(d1, "%Y-%m-%d")
    d2 = datetime.strptime(d2, "%Y-%m-%d")
    return abs((d2 - d1).days)



server = ''
database = ''
username = ''
password = ''
InitialCatalog=''
driver= '{ODBC Driver 17 for SQL Server}'
cnxn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

sql  =""" 


    SELECT cust.EntryDate ,   cust.azurewk  , format(cast(cust.dateofwk as date),'yyyy-MM-dd') AS dateofwk ,cust.wkid , cust.[Full Name] AS fullname 
, cust.[Job Title] AS jobtitle
,cust.[Company Name] AS companyname , cust.[Partner Organization] AS partnerorganizaiton ,
cust.eventwasvalue , cust.contentdelievered , cust.understandingandprofiency , cust.gainednewskills , 
cust.presentation , cust.microsoftistrusted , cust.resultofattending , cust.anyfeedback ,cust.yournextstepazure
, cust.recommend , cust.[areas of improvement] AS areasofimprovment ,
cust.[Spent amount of time] AS spentamountoftime , cust.[like Microsoft products and services] AS likemicrosoft
, cust.[Microsoft Partners may contact me] AS microsoftpartnerscontactme , 
cust.modified , cust.created   , cust.[Survey ID] as surveyid from global_survey_Customer_new3 cust  where
 not exists (select * from [global_survey_Customer_FuzzyALL] f where format(cast(f.entrydate as date),'yyyy-MM-dd') = format(cast(cust.entrydate as date),'yyyy-MM-dd'))
  union all
  SELECT cust.EntryDate ,   cust.azurewk  , format(cast(cust.dateofwk as date),'yyyy-MM-dd') AS dateofwk ,cust.wkid , cust.[Full Name] AS fullname 
, cust.[Job Title] AS jobtitle
,cust.[Company Name] AS companyname , cust.[Partner Organization] AS partnerorganizaiton ,
cust.eventwasvalue , cust.contentdelievered , cust.understandingandprofiency , cust.gainednewskills , 
cust.presentation , cust.microsoftistrusted , cust.resultofattending , cust.anyfeedback ,cust.yournextstepazure
, cust.recommend , cust.[areas of improvement] AS areasofimprovment ,
cust.[Spent amount of time] AS spentamountoftime , cust.[like Microsoft products and services] AS likemicrosoft
, cust.[Microsoft Partners may contact me] AS microsoftpartnerscontactme , 
cust.modified , cust.created , null from  global_survey_Customer_logicapp cust where
 not exists (select * from [global_survey_Customer_FuzzyALL] f where format(cast(f.entrydate as date),'yyyy-MM-dd') = format(cast(cust.entrydate as date),'yyyy-MM-dd'))   
   """


partner_dataframe =pd.read_sql_query(sql,cnxn)

#-------------------------------

sql2="""         select ScheduleUniqueID , Country , eventdate ,  ISNULL( InstructorOrg , 
SUBSTRING(SUBSTRING(InstructorEmail, CHARINDEX('@', InstructorEmail) + 1, LEN(InstructorEmail)),0,
CHARINDEX('.',SUBSTRING(InstructorEmail, CHARINDEX('@', InstructorEmail) + 1, LEN(InstructorEmail))) )) AS [InstructorOrg]  , 
replace(trim(REPLACE (trim(trackname),'Azure Immersion Workshop:','')),'Artificial Intelligence (AI)','AI') as trackname , ScheduleUniqueID as eveid
 from Cloudlabs_schedule """
dict_list=[]

schedule_dataframe =pd.read_sql_query(sql2,cnxn)


sql3="""         select ScheduleUniqueID , Country , eventdate ,  ISNULL( InstructorOrg , 
SUBSTRING(SUBSTRING(InstructorEmail, CHARINDEX('@', InstructorEmail) + 1, LEN(InstructorEmail)),0,
CHARINDEX('.',SUBSTRING(InstructorEmail, CHARINDEX('@', InstructorEmail) + 1, LEN(InstructorEmail))) )) AS [InstructorOrg]  , 
replace(trim(REPLACE (trim(trackname),'Azure Immersion Workshop:','')),'Artificial Intelligence (AI)','AI') as trackname , ScheduleUniqueID as eveid
 from Cloudlabs_schedule where ScheduleUniqueID = ? """


for index, row in partner_dataframe.iterrows():
    dict_ = {}
    dict_.update({"EntryDate": str(row['EntryDate'])})
    dict_.update({"azurewk": str(row['azurewk'])})
    dict_.update({"dateofwk": str(row['dateofwk'])})

    dict_.update({"fullname": str(row['fullname'])})
    dict_.update({"jobtitle": str(row['jobtitle'])})
    dict_.update({"companyname": str(row['companyname'])})
    dict_.update({"partnerorganizaiton": str(row['partnerorganizaiton'])})
    dict_.update({"eventwasvalue": str(row['eventwasvalue'])})
    dict_.update({"contentdelievered": str(row['contentdelievered'])})
    dict_.update({"understandingandprofiency": str(row['understandingandprofiency'])})
    dict_.update({"gainednewskills": str(row['gainednewskills'])})
    dict_.update({"presentation": str(row['presentation'])})
    dict_.update({"microsoftistrusted": str(row['microsoftistrusted'])})
    dict_.update({"resultofattending": str(row['resultofattending'])})
    dict_.update({"anyfeedback": str(row['anyfeedback'])})
    dict_.update({"yournextstepazure": str(row['yournextstepazure'])})
    dict_.update({"recommend": str(row['recommend']).strip()})
    dict_.update({"areasofimprovment": str(row['areasofimprovment'])})
    dict_.update({"spentamountoftime": str(row['spentamountoftime'])})
    dict_.update({"likemicrosoft": str(row['likemicrosoft'])})
    dict_.update({"microsoftpartnerscontactme": str(row['microsoftpartnerscontactme'])})
    dict_.update({"modified": str(row['modified'])})
    dict_.update({"created": str(row['created'])})
    dict_.update({"wkid": str(row['wkid'])})
    dict_.update({"ScheduleUniqueID": str(" ")})
    dict_.update({"Country": str(" ")})
    dict_.update({"surveyid": str(row['surveyid'])})




    dfcheck=pd.read_sql_query(sql3,cnxn,params=[str(row['wkid']).strip().replace(".","")])
    eveid=str(row['wkid']).strip().replace(".","")



    if dfcheck.empty :
        if  eveid :
           
            for index, rw in schedule_dataframe.iterrows():
                Ratiosr = fuzz.ratio(str(row['azurewk']).lower().replace(" ", ""),str(rw['trackname']).lower().replace(" ", ""))
                if Ratiosr > 80:
                    #print("if1-1")
                    dataframe_1=schedule_dataframe[schedule_dataframe['trackname'] ==str(rw['trackname'])]
                    RatioOrg = fuzz.ratio(str(row['partnerorganizaiton']).lower().replace(" ", ""),str(rw['InstructorOrg']).lower().replace(" ", ""))
                    if RatioOrg > 80:
                        #print("if1-2")
                        dataframe_2 = dataframe_1[dataframe_1['InstructorOrg'] == str(rw['InstructorOrg'])]
                        for index, r in dataframe_2.iterrows():
                            if  days_between(str(row['dateofwk']), str(r['eventdate'])) <= 7:
                                #print("1")
                                dict_.update({"ScheduleUniqueID": str(r['ScheduleUniqueID'])})
                                dict_.update({"Country": str(r['Country'])})



                                break
                            else :
                                #print("2")
                                RatioOrg = fuzz.token_sort_ratio(str(row['wkid']).lower().replace(" ", ""),str(r['ScheduleUniqueID']).lower().replace(" ", ""))
                                if RatioOrg >= 80:
                                    dict_.update({"ScheduleUniqueID": str(r['ScheduleUniqueID'])})
                                    dict_.update({"Country": str(r['Country'])})
                    else:
                        #print("3")
                        RatioOrg = fuzz.token_sort_ratio(str(row['wkid']).lower().replace(" ", ""),str(rw['ScheduleUniqueID']).lower().replace(" ", ""))
                        if RatioOrg >= 80:
                            dict_.update({"ScheduleUniqueID": str(rw['ScheduleUniqueID'])})
                            dict_.update({"Country": str(rw['Country'])})


        else :
            #print("4")

            for index, rw in schedule_dataframe.iterrows():
                Ratiosr = fuzz.ratio(str(row['azurewk']).lower().replace(" ", ""),str(rw['trackname']).lower().replace(" ", ""))
                if Ratiosr > 80:
                    dataframe_1 = schedule_dataframe[schedule_dataframe['trackname'] == str(rw['trackname'])]
                    for index, ww in dataframe_1.iterrows():
                        RatioOrg = fuzz.token_sort_ratio(str(row['wkid']).lower().replace(" ", ""),str(ww['ScheduleUniqueID']).lower().replace(" ", ""))
                        if RatioOrg >= 80:
                            dict_.update({"ScheduleUniqueID": str(ww['ScheduleUniqueID'])})
                            dict_.update({"Country": str(ww['Country'])})
                            break

    else :
        print("if2")
        for index, rr in dfcheck.iterrows():
            dict_.update({"ScheduleUniqueID": str(rr['ScheduleUniqueID'])})

            dict_.update({"Country": str(rr['Country'])})





    dict_list.append(dict_)

df_dict = pd.DataFrame(dict_list)

for i,s in df_dict.iterrows():
    sql="insert INTO [AIW-PROD-DB].[dbo].[global_survey_Customer_FuzzyALL]  VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    cursor.execute(sql, ( str(s['EntryDate']),str(s['azurewk']),str(s['dateofwk']),str(s['fullname']),str(s['jobtitle']),str(s['companyname']),str(s['partnerorganizaiton']),str(s['eventwasvalue']),str(s['contentdelievered']),str(s['understandingandprofiency']),str(s['gainednewskills']),str(s['presentation']),str(s['microsoftistrusted']),str(s['resultofattending']),str(s['anyfeedback']),str(s['yournextstepazure']),str(s['recommend']),str(s['areasofimprovment']),str(s['spentamountoftime']),str(s['likemicrosoft']),str(s['microsoftpartnerscontactme']),str(s['modified']),str(s['created']),str(s['wkid']),str(s['ScheduleUniqueID']),str(s['Country']),str(s['surveyid'])))
    cursor.commit()



sql_update_logs = "insert into  daily_tasks_logs values ('customersurvey' , GETDATE()) ;"
cursor.execute(sql_update_logs)
cursor.commit()

