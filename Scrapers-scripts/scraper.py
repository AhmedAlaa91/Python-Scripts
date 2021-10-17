import requests
import urllib
import pandas as pd
from requests_html import HTML
from requests_html import HTMLSession
from urllib.parse import urlparse
from fuzzywuzzy import fuzz
import pyodbc
import array

from bs4 import BeautifulSoup
import time



server = ''
database = ''
username = ''
password = ''
InitialCatalog=''
driver= '{ODBC Driver 17 for SQL Server}'

cnxn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

#################################################################################################



sql1 = '''  select  partnerid as id , s.organizationname as partnername, s.subsidary as country 
 from  MPNPrivacy s  '''


df_partners =pd.read_sql_query(sql1,cnxn)


def get_source(url):

    try:
        session = HTMLSession()
        response = session.get(url)
        return response

    except Exception:
        pass


def get_hyperlinks(url):
    try:
        reqs = requests.get(url)
        soup = BeautifulSoup(reqs.text, 'html.parser')

        urls = []
        for link in soup.find_all('a'):

            if (str(link.get('href')).find("privacy")) != -1:
                return (url + link.get('href'))

    except Exception:
        pass




def scrape_google(query):

    query = urllib.parse.quote_plus(query)
    response = get_source("https://www.google.com/search?q=" + query)

    links = list(response.html.absolute_links)
    google_domains = ('https://www.google.',
                      'https://google.',
                      'https://webcache.googleusercontent.',
                      'http://webcache.googleusercontent.',
                      'https://policies.google.',
                      'https://support.google.',
                      'https://maps.google.',
                      'https://linkedin.',
                      'https://www.linkedin.',
                      'https://translate.google.',
                      'https://.linkedin.',
                      'https://martechseries.com')
    arr1 =[]

    for url in links[:]:
        if url.startswith(google_domains):
            links.remove(url)


    for url in links[:]:
        links.append(urlparse(url).netloc)
        t = urlparse(url).netloc
        arr1.append(t)





    return arr1












for index, row in df_partners.iterrows():
   
    partner_name=str(row['partnername']).split(',')[0]
    partner1=str(row['partnername'])
    country_name =str(row['country'])
    targetname=partner_name+ ' ' + country_name
    print(row['id'])
    print(partner1)


    list_urls=scrape_google(partner1.replace(',',' '))
    row_id=row['id']

    for url in list_urls:
        target_url = ''
        linkk= ''
        url_term = ((url.partition(".")[2]).split(".", 1)[0])

        if (fuzz.partial_ratio(url_term.lower(), targetname.lower())) > 70:
            print(url)
            target_url = url

            linkk=get_hyperlinks('https://' + target_url)
            print (linkk)
            break

    if  linkk =='' :
        
        list_urls = scrape_google(partner1)
       
        for url in list_urls:
            target_url = ''
            linkk = ''
            url_term = ((url.partition(".")[2]).split(".", 1)[0])



            if (fuzz.partial_ratio(url_term.lower(), targetname.lower())) >= 70:
                print(url)
                target_url = url
     
                linkk = get_hyperlinks('https://' + target_url)
                print(linkk)
                break




    sql_del = "update  MPNPrivacy set found=? , privacyurl=? where partnerid=? "
    cursor.execute(sql_del,target_url,linkk,row_id)
    cursor.commit()

  
