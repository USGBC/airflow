from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import snowflake.connector
import json
import pymongo
import pandas as pd
import pytz

import pandas as pd
from eventbrite import Eventbrite
from pandas import json_normalize
import requests
import snowflake.connector
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine, text
from snowflake.connector.pandas_tools import pd_writer
from datetime import datetime


import time
kk = time.time()
def migrate_data_to_snowflake():
    engine2 = create_engine(URL(
    user='venkatlambda', 
    password='Snowflake_123',
    account='xvb64103', 
    warehouse='WH',
    database='RAW_DEV', 
    schema='EVENTBRITE',
    ))
    conn2 = engine2.connect()

    #get credentials json
    # scrts = os.path.join( os.getcwd(), '..', 'secrets.json' )
    tables = {
        "pacific":{
            
            "attende_table":"EB_ATTENDEE_PACIFIC",
            "event_table" : 'EB_EVENT_PACIFIC',
            "venue_table" : 'EB_VENUE_PACIFIC'
        },
        "mountain":{
            "attende_table":"EB_ATTENDEE_MOUNTAIN",
            "event_table" : 'EB_EVENT_MOUNTAIN',
            "venue_table" : 'EB_VENUE_MOUNTAIN'
        },
        "west_north_central":{
            "attende_table":"EB_ATTENDEE_WEST_NORTH_CENTRAL",
            "event_table" : 'EB_EVENT_WEST_NORTH_CENTRAL',
            "venue_table" : 'EB_VENUE_WEST_NORTH_CENTRAL'
        },
        "east_north_central":{
            "attende_table":"EB_ATTENDEE_EAST_NORTH_CENTRAL",
            "event_table" : 'EB_EVENT_EAST_NORTH_CENTRAL',
            "venue_table" : 'EB_VENUE_EAST_NORTH_CENTRAL'
        },
        "south_central":{
            "attende_table":"eb_attendee_south_central",
            "event_table" : 'EB_EVENT_SOUTH_CENTRAL',
            "venue_table" : 'EB_VENUE_SOUTH_CENTRAL'
        },
        "south_atlantic":{
            "attende_table":"EB_ATTENDEE_SOUTH_ATLANTIC",
            "event_table" : 'EB_EVENT_SOUTH_ATLANTIC',
            "venue_table" : 'EB_VENUE_SOUTH_ATLANTIC'
        },
        "mid_atlantic_new_england":{
            "attende_table":"EB_ATTENDEE_MID_ATLANTIC_NEW_ENGLAND",
            "event_table" : 'EB_EVENT_MID_ATLANTIC_NEW_ENGLAND',
            "venue_table" : 'EB_VENUE_MID_ATLANTIC_NEW_ENGLAND'
        },
        "middle_atlantic":{
            "attende_table":"EB_ATTENDEE_MIDDLE_ATLANTIC",
            "event_table" : 'EB_EVENT_MIDDLE_ATLANTIC',
            "venue_table" : 'EB_VENUE_MIDDLE_ATLANTIC'
        },
        "gbci":{
            "attende_table":"EB_ATTENDEE_GBCI",
            "event_table" : 'EB_EVENT_GBCI',
            "venue_table" : 'EB_VENUE_GBCI'
        }
    }

    secrets = pd.read_json('secrets.json')
    dw_access = secrets.loc["login"]["dw"] + ":" + secrets.loc["pw"]["dw"] + "@" + secrets.loc["address"]["dw"]
    eb_keys = secrets[secrets["eventbrite"].notna()]["eventbrite"].to_dict()
    print(eb_keys)


    #placeholder functionality for missing pagination in get_event_attendees
    def get_page(pn, event_id, auth):  #pn is page number
        pn=str(pn)
        event_id = str(event_id)
        auth = str(auth)
        response = requests.get(
            "https://www.eventbriteapi.com/v3/events/"+event_id+"/attendees/?page="+pn+"&token="+auth, 
            verify = True, 
        )   
        return response.json()

    #regions and api keys
    regions = eb_keys

    #regions = {'pacific': '4CZE6SWTNCKWT6YEGK6Z','mountain': 'J4CVQX7YABDSWS5GWHLJ','west_north_central': 'CJUJNKLMRBJSSHA7JNZM','east_north_central': 'JHV3UZQGLAX6ZKL7PSEC','south_central': 'OF4DMT47BFSKKLM4TMKA','south_atlantic': 'JQMOSVZQ4ESH3X6N5FFX','mid_atlantic_new_england': 'I3JRZZCR63DYCTY4625N','middle_atlantic':'I3JRZZCR63DYCTY4625N','gbci': 'S7GA6J6N3P2UC3KKYUKV',}
    # regions = {'south_atlantic': 'JQMOSVZQ4ESH3X6N5FFX'}
    # regions = { "mid_atlantic_new_england": "I3JRZZCR63DYCTY4625N"}
    for r in regions:
        ss = time.time()
        print(r,'->',regions[r])

        #set up dataframes

        evDF_all = pd.DataFrame()
        atDF_all = pd.DataFrame()
        venDF_all = pd.DataFrame()

        print("region: "+ str(r))
        #create connection to regional user's API key
        eb = Eventbrite(regions[r])
        idz = eb.get_user()['id']
        print(eb.get_user())
        print(idz)
        headers = {'Authorization': f'Bearer {regions[r]}'}
        org_response = requests.get(f'https://www.eventbriteapi.com/v3/users/{idz}/organizations/', headers=headers)
        organizations = org_response.json()
        org_id = organizations['organizations'][0]['id']

        more_venues = True
        venue_page = 1
        while more_venues == True:
            #get events for the user's region
            url = f'https://www.eventbriteapi.com/v3/organizations/{org_id}/venues/?page={venue_page}'
            ven_response = requests.get(url, headers=headers)
            venues = ven_response.json()
            venDF = json_normalize(venues['venues'])
            venDF_all = pd.concat([venDF_all, venDF], ignore_index=True, sort=True)
            more_venues = venues['pagination']['has_more_items']
            print("venue page: " + str(venue_page))
            if more_venues == True:
                venue_page += 1
        
        venDF_all.set_index('id',inplace = True)
        venDF_all = venDF_all.fillna('').replace({None: ''})
        venDF_all.drop('address.localized_multi_line_address_display', axis = 1, inplace = True)
        ven_columns = venDF_all.columns.tolist()
        formatted_venue_cols = ', '.join(f'"{column}"' for column in ven_columns)
        venDF_all = venDF_all.astype(str)
        try:
            venDF_all.to_sql(tables[r]['venue_table'].lower(), con=engine2,if_exists='append', index=False,method=pd_writer)
        except Exception as err:
            print("ERROR ON VENUE INSERTION " + str(r) + " ::: " + str(err))
        #handle event pagination
        more_events = True
        event_page = 1

        #loop through each page of responses for events, handling pagination
        while more_events == True:
            #get events for the user's region
            url = f'https://www.eventbriteapi.com/v3/organizations/{org_id}/events/?page={event_page}'
            event_response = requests.get(url, headers=headers)
            events = event_response.json()
            evDF = json_normalize(events['events'])
            evDF_all = pd.concat([evDF_all, evDF], ignore_index=True, sort=True)
            more_events = events['pagination']['has_more_items']
            print("event page: " + str(event_page))
            if more_events == True:
                event_page += 1

            for ev_id in evDF['id']:
                print("event: " + str(ev_id))
                more_attend = True
                attend_page = 1
                while more_attend == True:
                    attendees = get_page(pn = attend_page, event_id = ev_id, auth = regions[r])
                    atDF = json_normalize(attendees['attendees'])
                    atDF_all = pd.concat([atDF_all, atDF], ignore_index=True, sort=True)
                    more_attend = attendees['pagination']['has_more_items']
                    print("attend page: " + str(attend_page))
                    if more_attend == True:
                        attend_page +=1
                        
        evDF_all.set_index('id',inplace = True)
        atDF_all.set_index('id',inplace = True)

        atDF_all['answers'] = atDF_all['answers'].astype(str)
        atDF_all['barcodes'] = atDF_all['barcodes'].astype(str)
        
        evDF_all.rename(columns={'description.text': 'description.varchar'},inplace=True, errors='raise')
        evDF_all = evDF_all.fillna('').replace({None: ''})
        evDF_all.drop(['name.text'], axis=1, inplace=True)
        evDF_all = evDF_all.astype(str)
        
        atDF_all = atDF_all.fillna('').replace({None: ''})
        atDF_all = atDF_all.astype(str)

        try:
            evDF_all.to_sql(tables[r]['event_table'].lower(), con=engine2,if_exists='append', index=False,method=pd_writer)
        except Exception as err:
            print("ERROR ON EVENT INSERTION " + str(r) + " ::: " + str(err))
        try:
            atDF_all.to_sql(tables[r]['attende_table'].lower(), con=engine2,if_exists='append', index=False,method=pd_writer)
        except Exception as err:
            print("ERROR ON ATTENDEE INSERTION " + str(r) + " ::: " + str(err))
        print("TIME  :::"+ str(time.time()-ss))
        
        insert_query = text("""
        INSERT INTO RAW_DEV.ETL_CONTROL.ETL_CONTROL
        (job_name, Job_frequency, tableName, start_date, last_run_date)
        VALUES (:job_name, :job_frequency, :table_name, :start_date, :last_run_date)
        """)

        # Parameters for the insert query
        params = {
            'job_name': 'T_EVENTBRITE_SNF',
            'job_frequency': 'DAILY',
            'table_name': 'EVENTBRITE',
            'start_date': datetime.utcnow(),
            'last_run_date': datetime.utcnow(),
        }
        conn2.execute(insert_query, params)
    print("FINAL TIME" + str(time.time()-kk))

dag = DAG('Test_Dag', description='test Dag', start_date=datetime(2024, 2, 14), catchup=False)

hello_operator = PythonOperator(task_id='hello_task1', python_callable=migrate_data_to_snowflake, dag=dag)

hello_operator 
