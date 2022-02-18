import requests
import os
from datetime import datetime
import json
import pickle

#leggo json
with open('dictionary_scraping/db_ciclismo_scraping_quality.pkl', 'rb') as f:
        diz = pickle.load(f)

#CHIAMATE API NECESSARIE - Visual crossing permette 1000 chiamate al giorno
counter=0
for el in diz:
    if 'location' in diz[el].keys():
        counter+=1
print('Il numero di chiamate API necessarie è: ', counter)


#inizializzazione variabili ciclo for
i=0
counter_api=0
date_dic={'gennaio': '01','febbraio': '02','marzo': '03','aprile': '04','maggio': '05','giugno': '06','luglio': '07','agosto': '08','settembre': '09','ottobre': '10','novembre': '11', 'dicembre': '12'}


for id_activity in diz:
    dic_values = diz[id_activity] #estraggo il dizionario per valori API
    date_list = dic_values['daytime']['day'].split(' ') #'daytime': {'day_week': 'Giovedì', 'day': '3 febbraio 2022'},
    date = date_list[2]+'-'+date_dic[date_list[1]]+'-'+date_list[0] #converto la data

    if ('location' in dic_values.keys()):
        if counter_api<900:
            user_api='insert_apiKEY'
        elif counter_api>=900 or counter_api<1800:
            user_api='insert_apiKEY'
        elif counter_api>=1800 or counter_api<2700:
            user_api='insert_apiKEY'
        else:
            user_api='insert_apiKEY'
        #print(counter_api)
        #print(user_api)
            
        #Select location
        location = dic_values['location'].split(',')[0].strip().replace(' ', '')
        
        #Build API call
        complete_api_link = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/weatherdata/history?goal=history&aggregateHours=24&startDateTime="+date+"T00:00:00&endDateTime="+date+"T23:00:00&contentType=json&unitGroup=us&locations="+location+"&key="+user_api
        api_link = requests.get(complete_api_link)
        api_data=api_link.text
        data_weather=api_link.json()

        #enrichment - wheater values
        if('locations' in data_weather.keys()):
            diz[id_activity]['weather_API'] = data_weather['locations'][location]['values'][0]
    #diz[i][istance]['daytime']['day'] = date #Vedere se mettere o meno
        counter_api+=1

with open('db_ciclismo_enrichment.json', 'w') as f:
    json.dump(diz, f)
    