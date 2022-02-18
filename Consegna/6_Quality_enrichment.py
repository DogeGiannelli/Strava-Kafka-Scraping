import pickle
import pymongo
from pymongo import MongoClient
from json import loads
from datetime import datetime
import time
import json
import deepl

diz = json.load(open('dictionary_scraping/db_ciclismo_enrichment.json')) 

#DELETE NULL API
for i in diz:
    if 'weather_API' in diz[i].keys() and diz[i]['weather_API']['info'] == 'No data available':
        diz[i].pop('weather_API')

   
#Document list with STRAVA keys 'weather'
c=0
b=0
d=0
for i in diz:
    if 'weather' in diz[i].keys():
        c+=1
    elif 'weather_API' in diz[i].keys():
        b+=1
    elif 'weather_API' in diz[i].keys() or 'weather' in diz[i].keys():
        d+=1
    else:
        pass

print('Il numero di documenti con la chiave weather di STRAVA è: ',c)
print('Il numero di documenti con una corretta API call è: ',b)
print('Il numero di documenti con una entrambi weather è: ',d)
#Document list with keys 'weather_API' or 'weather'


#ADD unit and conversion
diz_unit={"wdir":'°',"temp":'°C',"maxt":'°C',"visibility":'μ',"wspd":'km/h',
"datetimeStr":"","solarenergy":'J/m^2',"heatindex":'°C',
"cloudcover":'%',"mint":'°C',"datetime":'',"precip":'mm',"solarradiation":'W/m^2',
"weathertype":"","snowdepth":'mm',"sealevelpressure":'mb',"snow":'mm',"dew":'°C',
"humidity":'%',"precipcover":'%',"wgust":'km/h',"conditions":"",
"windchill":'°C',"info":'null'}

#IF_condition value
n=None

for i in diz:
    if 'weather_API' in diz[i].keys():
        #CONVERSION
        
        #degreF to °C -- °C=(T(°F) - 32) × 5/9
        if type(diz[i]['weather_API']['temp']) != type(n):
            diz[i]['weather_API']['temp'] = round((diz[i]['weather_API']['temp'] - 32) * 0.56, 2)
        if type(diz[i]['weather_API']['maxt']) != type(n):    
            diz[i]['weather_API']['maxt'] = round((diz[i]['weather_API']['maxt'] - 32) * 0.56, 2)
        if type(diz[i]['weather_API']['heatindex']) != type(n):
            diz[i]['weather_API']['heatindex'] = round((diz[i]['weather_API']['heatindex'] - 32) * 0.56, 2)
        if type(diz[i]['weather_API']['mint']) != type(n):
            diz[i]['weather_API']['mint'] = round((diz[i]['weather_API']['mint'] - 32) * 0.56, 2)
        if type(diz[i]['weather_API']['dew']) != type(n):
            diz[i]['weather_API']['dew'] = round((diz[i]['weather_API']['dew'] - 32) * 0.56, 2)
        if type(diz[i]['weather_API']['windchill']) != type(n):
            diz[i]['weather_API']['windchill'] = round((diz[i]['weather_API']['windchill'] - 32) * 0.56, 2)
        #mph to kmh -- kmh = mph*1.609344
        if type(diz[i]['weather_API']['wspd']) != type(n):
            diz[i]['weather_API']['wspd'] = round(diz[i]['weather_API']['wspd'] * 1.61, 2)
        if type(diz[i]['weather_API']['wgust']) != type(n):
            diz[i]['weather_API']['wgust'] = round(diz[i]['weather_API']['wgust'] * 1.61, 2)
        #inches to mm -- mm = in*25,4
        if type(diz[i]['weather_API']['precip']) != type(n):
            diz[i]['weather_API']['precip'] = round(diz[i]['weather_API']['precip'] * 25.4, 2)
        if type(diz[i]['weather_API']['snowdepth']) != type(n):
            diz[i]['weather_API']['snowdepth'] = round(diz[i]['weather_API']['snowdepth'] * 25.4, 2)
        if type(diz[i]['weather_API']['snow']) != type(n):
            diz[i]['weather_API']['snow'] = round(diz[i]['weather_API']['snow'] * 25.4, 2)
        
        #ADD unit
        for key_unit in diz_unit.keys():
            if type(diz[i]['weather_API'][key_unit]) != type(n):
                diz[i]['weather_API'][key_unit] = str(diz[i]['weather_API'][key_unit]) + str(diz_unit[key_unit])


#Rename weather_API_features and mantain only relevant.
vuoto={}

for i in diz.keys():
    if 'weather_API' in diz[i].keys():
        vuoto['Direzione del vento'] = diz[i]['weather_API']['wdir']
        vuoto['Temperatura'] = diz[i]['weather_API']['temp']
        vuoto['Visibilita'] = diz[i]['weather_API']['visibility']
        vuoto['Velocità del vento'] = diz[i]['weather_API']['wspd']
        vuoto['Copertura nuvolosa'] = diz[i]['weather_API']['cloudcover']
        vuoto['Precipitazioni'] = diz[i]['weather_API']['precip']
        vuoto['Radiazioni solari'] = diz[i]['weather_API']['solarradiation']
        vuoto['Neve'] = diz[i]['weather_API']['snow']
        vuoto['Escursione Termica'] = diz[i]['weather_API']['dew']
        vuoto['Umidità'] = diz[i]['weather_API']['humidity']
        vuoto['Copertura precipitazioni'] = diz[i]['weather_API']['precipcover']
        vuoto['Picco raffica'] = diz[i]['weather_API']['wgust']
        vuoto['Condition'] = diz[i]['weather_API']['conditions']
        diz[i]['weather_API'] = vuoto


for i in diz.keys():
    if 'weather_API' in diz[i].keys() and 'weather' not in diz[i].keys():
        diz[i]['weather'] = diz[i]['weather_API']
        diz[i].pop('weather_API')
    elif 'weather_API' in diz[i].keys() and 'weather' in diz[i].keys():
        diz[i]['weather']['Direzione del vento'] = diz[i]['weather_API']['Direzione del vento']
        diz[i]['weather']['Visibilita'] = diz[i]['weather_API']['Visibilita']
        diz[i]['weather']['Copertura nuvolosa'] = diz[i]['weather_API']['Copertura nuvolosa']
        diz[i]['weather']['Precipitazioni'] = diz[i]['weather_API']['Precipitazioni']
        diz[i]['weather']['Radiazioni solari'] = diz[i]['weather_API']['Radiazioni solari']
        diz[i]['weather']['Neve'] = diz[i]['weather_API']['Neve']
        diz[i]['weather']['Escursione Termica'] = diz[i]['weather_API']['Escursione Termica']
        diz[i]['weather']['Copertura precipitazioni'] = diz[i]['weather_API']['Copertura precipitazioni']
        diz[i]['weather']['Picco raffica'] = diz[i]['weather_API']['Picco raffica']
        diz[i].pop('weather_API')
    else:
        pass



# Inizializzazione traduttore
translator = deepl.Translator('INSERT PRIVATE KEY') 


# Traduzione titolo
for el in diz:
    string = diz[el]['title']
    result = translator.translate_text(string, target_lang='IT') 
    diz[el]['title']  = result.text
    

# Traduzione commenti
for el in diz:
    if 'comments' in diz[el]['social'].keys():
        li = []
        for k in diz[el]['social']['comments']:
            result = translator.translate_text(k, target_lang='IT') 
            li.append(result.text)
        diz[el]['social']['comments'] = li


# Traduzione condizione meteo
for el in diz:
    if 'weather' in diz[el].keys():
        if 'Condition' in diz[el]['weather'].keys():
            string = diz[el]['weather']['Condition']
            result = translator.translate_text(string, target_lang='IT') 
            diz[el]['weather']['Condition']  = result.text


# Traduzione in italiano delle chiavi
diz2 = {}

for el in diz:
    diz2[el] = {}
    diz2[el]['ID'] = el
    diz2[el]['Atleta'] = diz[el]['athlete']
    diz2[el]['Titolo'] = diz[el]['title']
    diz2[el]['Attività'] = diz[el]['activity']
    diz2[el]['Tempo'] = {}
    if 'day' in diz[el]['daytime'].keys():
        diz2[el]['Tempo']['Giorno'] = diz[el]['daytime']['day']
    if 'day_week' in diz[el]['daytime'].keys():
        diz2[el]['Tempo']['Giorno Settimana'] = diz[el]['daytime']['day_week']
    if 'time' in diz[el]['daytime'].keys():
        diz2[el]['Tempo']['Orario'] = diz[el]['daytime']['time']
    diz2[el]['Social'] = {}
    diz2[el]['Social']['Kudos'] = diz[el]['social']['kudos']
    if 'comments' in diz[el]['social'].keys():
        diz2[el]['Social']['Commenti'] = diz[el]['social']['comments']
    diz2[el]['Statistiche'] = {}
    if 'distanza' in diz[el]['statistics'].keys():
        diz2[el]['Statistiche']['Distanza'] = diz[el]['statistics']['distanza']
    if 'tempo in movimento' in diz[el]['statistics'].keys():
        diz2[el]['Statistiche']['Tempo in movimento'] = diz[el]['statistics']['tempo in movimento']
    if 'passo medio' in diz[el]['statistics'].keys():
        diz2[el]['Statistiche']['Passo medio'] = diz[el]['statistics']['passo medio']
    if 'sforzo relativo' in diz[el]['statistics'].keys():
        diz2[el]['Statistiche']['Sforzo relativo'] = diz[el]['statistics']['sforzo relativo']
    if 'dislivello' in diz[el]['statistics'].keys():
        diz2[el]['Statistiche']['Dislivello'] = diz[el]['statistics']['dislivello']
    if 'calorie' in diz[el]['statistics'].keys():
        diz2[el]['Statistiche']['Calorie'] = diz[el]['statistics']['calorie']
    if 'tempo trascorso' in diz[el]['statistics'].keys():
        diz2[el]['Statistiche']['Tempo trascorso'] = diz[el]['statistics']['tempo trascorso']
    if 'potenza media ponderata' in diz[el]['statistics'].keys():
        diz2[el]['Statistiche']['Potenza media ponderata'] = diz[el]['statistics']['potenza media ponderata']
    if 'lavoro totale' in diz[el]['statistics'].keys():
        diz2[el]['Statistiche']['Lavoro totale'] = diz[el]['statistics']['lavoro totale']
    if 'carico di allenamento' in diz[el]['statistics'].keys():
        diz2[el]['Statistiche']['Carico di allenamento'] = diz[el]['statistics']['carico di allenamento']
    if 'intensita' in diz[el]['statistics'].keys():
        diz2[el]['Statistiche']['Intensità'] = diz[el]['statistics']['intensita']
    if 'location' in diz[el].keys():
        diz2[el]['Location'] = diz[el]['location']
    if 'weather' in diz[el].keys():
        diz2[el]['Meteo'] = diz[el]['weather']


# Correzione per il valore Overcast e traduzione Condition
for el in diz2:
    if 'Meteo' in el.keys():
        el['Meteo']['Condizione'] = el['Meteo']['Condition']   
        if el['Meteo']['Condizione'] == 'Sovraccarico':
            el['Meteo']['Condizione'] = 'Nuvoloso'
        el['Meteo'].pop('Condition')


# Lettura degli attributi 'Neve' e 'Picco raffica' come flessibili
for el in diz2:
    if 'Meteo' in el.keys():  
        if 'Neve' in el['Meteo'].keys(): 
            if el['Meteo']['Neve'] is None:
                el['Meteo'].pop('Neve')
        if 'Picco raffica' in el['Meteo'].keys(): 
            if el['Meteo']['Picco raffica'] is None:
                el['Meteo'].pop('Picco raffica')




client = MongoClient('localhost:27017')
###database 
db = client["Strava"]
###Collection
collection = db["Ciclismo"]
##
for el in diz2:
    #Insert document in Ciclismo collection
    collection.insert_one(el)

