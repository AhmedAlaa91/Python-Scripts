import numpy as nump
import pandas as pd
from fuzzywuzzy import fuzz, process
import pyodbc
from datetime import datetime

now = datetime.now()

server = ''
database = ''
username = ''
password = ''
InitialCatalog=''
driver= '{ODBC Driver 17 for SQL Server}'

cnxn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()







current_time = now.strftime("%H:%M:%S")
print("Current Time =", current_time)


sql1 = ''' select ScheduleUniqueID , Country , eventdate ,  InstructorEmail , ISNULL( InstructorOrg , 
SUBSTRING(SUBSTRING(InstructorEmail, CHARINDEX('@', InstructorEmail) + 1, LEN(InstructorEmail)),0,
CHARINDEX('.',SUBSTRING(InstructorEmail, CHARINDEX('@', InstructorEmail) + 1, LEN(InstructorEmail))) )) AS [InstructorOrg]  , 
replace(trim(REPLACE (trim(trackname),'Azure Immersion Workshop:','')),'Artificial Intelligence (AI)','AI') as trackname , scheduleuniqueid as eveid
 from Cloudlabs_Schedule_Updates 
'''


df_cloudlabs =pd.read_sql_query(sql1,cnxn)

#df_cloudlabs= pd.read_csv('C:\\excel sheet\\cloud_schedule_17mar21.csv')
df_cloudlabs_sorted= df_cloudlabs.sort_values(by='Country', ascending=True, na_position='last')
dataf= pd.read_csv('eligible partner.csv')
def match_term(dataf,country,term ,min_score_r=89, min_score_partial=89 , min_score_tokensort=20 , min_socre_tokenset=89):
    max_score_partial=-1
    max_score_tokensort = -1
    max_score_tokenset= -1
    max_score_r=-1
    max_name =""
    max_id =""
    max_area=""
    max_country=""
    max_seg=""
    max_subeg=""
    #dataf_f = dataf[dataf['areaname'] == 'APAC']

    if not dataf.empty:
        #print("dataf not empty")
        dataf_f = dataf[dataf['Subsidiary'] == country]
        #print(country)



    # for term2 in list_part :
    for index, row in dataf_f.iterrows():

        score_r= fuzz.ratio(term, str(row['Partner Name']).lower().replace(" ", ""))
        score_p = fuzz.partial_ratio(term, str(row['Partner Name']).lower().replace(" ", ""))
        score_tsort = fuzz.token_sort_ratio(term, str(row['Partner Name']).lower().replace(" ", ""))
        score_tset = fuzz.token_set_ratio(term, str(row['Partner Name']).lower().replace(" ", ""))
        id=row['PartnerONE ID']
        #area=row['Area']
        country=row['Subsidiary']




        if (
                (  (score_r > min_score_r) & (score_r > max_score_r) &                         (score_p > min_score_partial) & (score_p >= max_score_partial)   & ( score_tset >= max_score_tokenset)  & (score_tset >min_socre_tokenset))
            or  ( ( score_tsort > max_score_tokensort)  & (score_tsort >min_score_tokensort) & (score_p > min_score_partial) & (score_p >= max_score_partial)   & ( score_tset >= max_score_tokenset)  & (score_tset >min_socre_tokenset) )
            or  ((score_p > 75) & (score_p > max_score_partial) )
        ):



            max_name=str(row['Partner Name']).lower()
            max_score_r=score_r
            max_score_partial = score_p
            max_score_tokensort = score_tsort
            max_score_tokenset = score_tset
            #max_area=area
            max_id = id
            max_country=country
            #max_seg=segment
            #max_subeg=subsegment
            #print(max_score_r)

    return max_name,max_id,max_country,max_area

 #   return max_name,max_id,max_country,max_area

dict_list=[]

for index, row in df_cloudlabs_sorted.iterrows():
        #print(str(row['Organization']).lower())
        max_name,max_id,max_country,max_area = match_term(dataf,str(row['country']),str(row['InstructorOrg']).lower().replace(" ", ""))
        match=row['country'],row['ScheduleUniqueID'],row['InstructorEmail'],row['InstructorOrg'],max_name,max_id,max_country,max_area

        #print (match[4])

        dict_={}
        dict_.update({"country1": match[0]})
        dict_.update({"ScheduleUniqueID": match[1]})
        dict_.update({"InstructorEmail": match[2]})
        dict_.update({"InstructorOrg":match[3]})
        dict_.update({"PartnerName": match[4]})
        dict_.update({"PartnerID": match[5]})
        dict_.update({"country2": match[6]})
        dict_.update({"Area": match[7]})




       # dict_.update({"part_score": match[2]})
        #dict_.update({"part_id": match[3]})
        dict_list.append(dict_)

df_dict=pd.DataFrame(dict_list)


df_fict_notnull =df_dict.loc[df_dict['country2'] != '']
#df_fict_null = df_dict.loc[df_dict['country'] == '']




df_fict_notnull.to_csv('fuzzy.csv', index=False, header=True)

now2 = datetime.now()



df_cloudlabs= pd.read_csv('fuzzy.csv')


for i, row in df_cloudlabs.iterrows():
    sql="insert INTO [AIW-PROD-DB].[dbo].[Cloudlabs_Schedule_fuzzy] ( ScheduleUniqueID ,InstructorEmail ,InstructorOrg ,PartnerName ,PartnerID ,Country ,Area )  VALUES (?,?,?,?,?,?,?)"
    cursor.execute(sql, (str(row['ScheduleUniqueID']), str(row['InstructorEmail']), str(row['InstructorOrg']), str(row['PartnerName']).strip(), row['PartnerID'], str(row['Country']),str(" ")))
    cursor.commit()



current_time2 = now2.strftime("%H:%M:%S")

sql_del = "delete from Cloudlabs_Schedule_Updates"
cursor.execute(sql_del)
cursor.commit()


sql_update_logs = "insert into  daily_tasks_logs values ('fuzzyschedule' , GETDATE()) ;"
cursor.execute(sql_update_logs)
cursor.commit()

print("Current Time =", current_time2)