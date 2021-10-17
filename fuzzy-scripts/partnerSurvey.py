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
password = '
InitialCatalog = 'MALDB'
driver = '{ODBC Driver 17 for SQL Server}'
# cnxn = pyodbc.connect(''DRIV'ER={ODBC Driver 13 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cnxn = pyodbc.connect(
    'DRIVER=' + driver + ';SERVER=' + server + ';PORT=1433;DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
cursor = cnxn.cursor()

sql = """  


 select distinct
[dateofsubmission] as dateofsubmission,[fullname] as fullname,	[companyname] as companname,	[rolename] as rolname,
	[azurews] as workshop,	[dateofws] as dateworkshop,	[numberofattendees] as numberofattendees,	[didteamsmeet] as didteamsmeet,	[didtheholdgowell] as didtheholdgowell,[didyouencounter] as didyouencounter,	[isthereanythingelse] as isthereanythingelse,
	[Teams_Meeting_Function_Problems] as Teams_Meeting_Function_Problems,[HOL_virtual_platform] as  HOL_virtual_platform,[Contact Lab Support] as ContactLabSupport ,[What Problems you encounter] as WhatProblemsyouencounter,
	[WhatHowAttendeeSpecific Issue] as WhatHowAttendeeSpecificIssue ,[Have other insights] as have_other_insight,[Event ID] as eventid ,
	 [MPN_locationID] as MPN_locationID
from global_survey_Partner  cust
where not exists (select * from global_survey_partner_Fuzzy f where format(cast(f.dateofsubmission as date),'yyyy-MM-dd') = format(cast(cust.dateofsubmission as date),'yyyy-MM-dd') and trim(f.fullname) = trim(cust.fullname))

  union
  select distinct
[dateofsubmission] as dateofsubmission,[fullname] as fullname,	[companyname] as companname,	[rolename] as rolname,
	[azurews] as workshop,	[dateofws] as dateworkshop,	[numberofattendees] as numberofattendees,	[didteamsmeet] as didteamsmeet,	[didtheholdgowell] as didtheholdgowell,[didyouencounter] as didyouencounter,	[isthereanythingelse] as isthereanythingelse,
	[Teams_Meeting_Function_Problems] as Teams_Meeting_Function_Problems,[HOL_virtual_platform] as  HOL_virtual_platform,[Contact Lab Support] as ContactLabSupport ,[What Problems you encounter] as WhatProblemsyouencounter,
	[WhatHowAttendeeSpecific Issue] as WhatHowAttendeeSpecificIssue ,[Have other insights] as have_other_insight,[Event ID] as eventid 
	, [MPN_locationID] as MPN_locationID
from global_survey_Partner_logicapp  cust
where not exists (select * from global_survey_partner_Fuzzy f where format(cast(f.dateofsubmission as date),'yyyy-MM-dd') = format(cast(cust.dateofsubmission as date),'yyyy-MM-dd') and trim(cust.fullname) = trim(f.fullname))

  """

partner_dataframe = pd.read_sql_query(sql, cnxn)

# -------------------------------

sql2 = """         select ScheduleUniqueID , Country , eventdate ,  ISNULL( InstructorOrg , 
SUBSTRING(SUBSTRING(InstructorEmail, CHARINDEX('@', InstructorEmail) + 1, LEN(InstructorEmail)),0,
CHARINDEX('.',SUBSTRING(InstructorEmail, CHARINDEX('@', InstructorEmail) + 1, LEN(InstructorEmail))) )) AS [InstructorOrg]  , 
replace(trim(REPLACE (trim(trackname),'Azure Immersion Workshop:','')),'Artificial Intelligence (AI)','AI') as trackname , scheduleuniqueid as eveid
 from Cloudlabs_schedule  where ScheduleUniqueID <> '.' """
dict_list = []

schedule_dataframe = pd.read_sql_query(sql2, cnxn)

sql3 = """         select ScheduleUniqueID , Country , eventdate ,  ISNULL( InstructorOrg , 
SUBSTRING(SUBSTRING(InstructorEmail, CHARINDEX('@', InstructorEmail) + 1, LEN(InstructorEmail)),0,
CHARINDEX('.',SUBSTRING(InstructorEmail, CHARINDEX('@', InstructorEmail) + 1, LEN(InstructorEmail))) )) AS [InstructorOrg]  , 
replace(trim(REPLACE (trim(trackname),'Azure Immersion Workshop:','')),'Artificial Intelligence (AI)','AI') as trackname , scheduleuniqueid as eveid
 from Cloudlabs_schedule where scheduleUNIQUEid = ? """

for index, row in partner_dataframe.iterrows():
    dict_ = {}
    dict_.update({"dateofsubmission": str(row['dateofsubmission'])})
    dict_.update({"fullname": str(row['fullname'])})
    dict_.update({"companname": str(row['companname'])})
    dict_.update({"rolname": str(row['rolname'])})
    dict_.update({"workshop": str(row['workshop'])})
    dict_.update({"dateworkshop": str(row['dateworkshop'])})
    dict_.update({"numberofattendees": str(row['numberofattendees'])})
    dict_.update({"didteamsmeet": str(row['didteamsmeet'])})
    dict_.update({"didtheholdgowell": str(row['didtheholdgowell'])})
    dict_.update({"didyouencounter": str(row['didyouencounter'])})
    dict_.update({"isthereanythingelse": str(row['isthereanythingelse'])})
    dict_.update({"Teams_Meeting_Function_Problems": str(row['Teams_Meeting_Function_Problems'])})
    dict_.update({"HOL_virtual_platform": str(row['HOL_virtual_platform'])})
    dict_.update({"ContactLabSupport": str(row['ContactLabSupport'])})
    dict_.update({"WhatProblemsyouencounter": str(row['WhatProblemsyouencounter'])})
    dict_.update({"WhatHowAttendeeSpecificIssue": str(row['WhatHowAttendeeSpecificIssue'])})
    dict_.update({"have_other_insight": str(row['have_other_insight'])})
    dict_.update({"eventid": str(row['eventid'])})
    dict_.update({"MPN_locationID": str(row['MPN_locationID'])})

    dfcheck = pd.read_sql_query(sql3, cnxn, params=[str(row['eventid']).strip().replace(".", "")])
    eveid = str(row['eventid']).strip().replace(".", "")
    dict_list.append(dict_)

    if dfcheck.empty:
        if eveid:
            ##print("if1")

            for index, rw in schedule_dataframe.iterrows():
                Ratiosr = fuzz.ratio(str(row['workshop']).lower().replace(" ", ""),
                                     str(rw['trackname']).lower().replace(" ", ""))
                if Ratiosr > 80:
                    # print("if1-1")
                    dataframe_1 = schedule_dataframe[schedule_dataframe['trackname'] == str(rw['trackname'])]
                    RatioOrg = fuzz.ratio(str(row['companname']).lower().replace(" ", ""),
                                          str(rw['InstructorOrg']).lower().replace(" ", ""))
                    if RatioOrg > 80:
                        # print("if1-2")
                        dataframe_2 = dataframe_1[dataframe_1['InstructorOrg'] == str(rw['InstructorOrg'])]
                        for index, r in dataframe_2.iterrows():
                            if days_between(str(row['dateworkshop']), str(r['eventdate'])) <= 7:
                                # print("1")
                                dict_.update({"ScheduleUniqueID": str(r['ScheduleUniqueID'])})
                                dict_.update({"Country": str(r['Country'])})

                                break
                            else:
                                # print("2")
                                RatioOrg = fuzz.token_sort_ratio(str(row['eventid']).lower().replace(" ", ""),
                                                                 str(r['ScheduleUniqueID']).lower().replace(" ", ""))
                                if RatioOrg >= 90:
                                    dict_.update({"ScheduleUniqueID": str(r['ScheduleUniqueID'])})
                                    dict_.update({"Country": str(r['Country'])})
                    else:
                        RatioOrg = fuzz.token_sort_ratio(str(row['eventid']).lower().replace(" ", ""),
                                                         str(rw['ScheduleUniqueID']).lower().replace(" ", ""))
                        if RatioOrg >= 90:
                            dict_.update({"ScheduleUniqueID": str(rw['ScheduleUniqueID'])})
                            dict_.update({"Country": str(rw['Country'])})


        else:

            for index, rw in schedule_dataframe.iterrows():
                Ratiosr = fuzz.ratio(str(row['workshop']).lower().replace(" ", ""),
                                     str(rw['trackname']).lower().replace(" ", ""))
                if Ratiosr > 80:
                    dataframe_1 = schedule_dataframe[schedule_dataframe['trackname'] == str(rw['trackname'])]
                    for index, ww in dataframe_1.iterrows():
                        RatioOrg = fuzz.token_sort_ratio(str(row['eventid']).lower().replace(" ", ""),
                                                         str(ww['ScheduleUniqueID']).lower().replace(" ", ""))
                        if RatioOrg >= 90:
                            dict_.update({"ScheduleUniqueID": str(ww['ScheduleUniqueID'])})
                            dict_.update({"Country": str(ww['Country'])})
                            break


    else:
        for index, rr in dfcheck.iterrows():
            dict_.update({"ScheduleUniqueID": str(rr['ScheduleUniqueID'])})

            dict_.update({"Country": str(rr['Country'])})

df_dict = pd.DataFrame(dict_list)

for i, s in df_dict.iterrows():
    sql = ''' insert INTO [AIW-PROD-DB].[dbo].[global_survey_Partner_Fuzzy] (	[dateofsubmission] ,
	[fullname] ,
	[companyname] ,
	[rolename] ,
	[azurews] ,
	[dateofws] ,
	[numberofattendees] ,
	[didteamsmeet] ,
	[didtheholdgowell] ,
	[didyouencounter] ,
	[isthereanythingelse] ,
	[Teams_Meeting_Function_Problems] ,
	[HOL_virtual_platform] ,
	[Contact Lab Support] ,
	[What Problems you encounter] ,
	[WhatHowAttendeeSpecific Issue] ,
	[Have other insights],
	[Event ID] ,
	[Schedule ID] ,
	[Country] ,
	[MPN_locationID] )  VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) '''
    #    #sql="insert INTO [AIW-PROD-DB].[dbo].[aiwevents_daily]  ( eventid ,PartnerName ,TPAccountName ,tpid ,WorkshopType ,SubSegmentName )  values (?,?,?,?,?,?)";
    cursor.execute(sql, (
    str(s['dateofsubmission']), str(s['fullname']), str(s['companname']), str(s['rolname']), str(s['workshop']),
    str(s['dateworkshop']), str(s['numberofattendees']), str(s['didteamsmeet']), str(s['didtheholdgowell']),
    str(s['didyouencounter']), str(s['isthereanythingelse']), str(s['Teams_Meeting_Function_Problems']),
    str(s['HOL_virtual_platform']), str(s['ContactLabSupport']), str(s['WhatProblemsyouencounter']),
    str(s['WhatHowAttendeeSpecificIssue']), str(s['have_other_insight']), str(s['eventid']), str(s['ScheduleUniqueID']),
    str(s['Country']), str(s['MPN_locationID'])))
    cursor.commit()


sql_update_logs = "insert into  daily_tasks_logs values ('partnersurveys' , GETDATE()) ;"
cursor.execute(sql_update_logs)
cursor.commit()
