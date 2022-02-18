import requests
import os
from datetime import datetime
import json
import pickle


#leggo file creato dal Consumer
with open('dictionary_scraping/diz_CICLISMO.pkl', 'rb') as f:
        diz = pickle.load(f)
        
#Sistemo la data e la location
for i in diz.keys():
    if 'day' in diz[i]['daytime'].keys():
        lista_day = diz[i]['daytime']['day'].split(' ')
        if len(lista_day) > 3:
            diz[i]['daytime']['day'] = ' '.join(lista_day[:3])
            
#Sistemo la data
for i in diz.keys():
    if 'day' in diz[i]['daytime'].keys():
        if '\n' in diz[i]['daytime']['day']:
            diz[i]['daytime']['day'] = diz[i]['daytime']['day'].split('\n')[0]

#Modifica delle ttività che riportano titolo errato a causa dello split('|')
diz['6625108389']['activity'] = 'Pedalata virtuale'
diz['6625083189']['activity'] = 'Pedalata virtuale'
diz['6625647789']['activity'] = 'Pedalata virtuale'
diz['6625588689']['activity'] = 'Pedalata virtuale'
diz['6625005189']['activity'] = 'Pedalata virtuale'
diz['6624958289']['activity'] = 'Ciclismo al coperto'
diz['6624901189']['activity'] = 'Pedalata virtuale'

#FORMAT STATISTICS
for i in diz.keys():
    if (len(diz[i]['statistics'])==1):
        diz[i]['statistics'] = {'tempo in movimento':diz[i]['statistics'][0]}
    elif (len(diz[i]['statistics'])==2):
        diz[i]['statistics'] = {'distanza':diz[i]['statistics'][0], 'tempo in movimento':diz[i]['statistics'][1]}
    elif (len(diz[i]['statistics'])==3):
        diz[i]['statistics'] = {'distanza':diz[i]['statistics'][0], 'tempo in movimento':diz[i]['statistics'][1], 'passo medio':diz[i]['statistics'][2]}
    else:
        diz[i]['statistics'] = {'distanza':diz[i]['statistics'][0], 'tempo in movimento':diz[i]['statistics'][1], 'passo medio':diz[i]['statistics'][2], 'sforzo relativo':diz[i]['statistics'][3]}


#FORMAT STATISTICS ADVANCED
for i in diz.keys():
    if 'statistics_advanced' in diz[i].keys():
        if (len(diz[i]['statistics_advanced'])==2):
            diz[i]['statistics'].update({'potenza media ponderata':diz[i]['statistics_advanced'][0], 'lavoro totale':diz[i]['statistics_advanced'][1]})
            diz[i].pop('statistics_advanced')
        else:
            diz[i]['statistics'].update({'potenza media ponderata':diz[i]['statistics_advanced'][0], 'lavoro totale':diz[i]['statistics_advanced'][1], 'carico di allenamento':diz[i]['statistics_advanced'][2], 'intensita':diz[i]['statistics_advanced'][3]})
            diz[i].pop('statistics_advanced')


#FORMAT STATISTICS MORE
for i in diz.keys():
    if 'more_statistics' in diz[i].keys():
        if (len(diz[i]['more_statistics'])==1):
            diz[i]['statistics'].update({'tempo trascorso':diz[i]['more_statistics'][0]})
            diz[i].pop('more_statistics')
        elif (len(diz[i]['more_statistics'])==2):
            diz[i]['statistics'].update({'dislivello':diz[i]['more_statistics'][0], 'tempo trascorso':diz[i]['more_statistics'][1]})
            diz[i].pop('more_statistics')
        else:
            diz[i]['statistics'].update({'dislivello':diz[i]['more_statistics'][0], 'calorie':diz[i]['more_statistics'][1], 'tempo trascorso':diz[i]['more_statistics'][2]})
            diz[i].pop('more_statistics')
        if 'passo medio' in diz[i]['statistics'].keys():
            diz[i]['statistics']['dislivello'] = diz[i]['statistics']['passo medio'] 
            diz[i]['statistics'].pop('passo medio')


#Verifica dei valori assunti e della frequenza associata, cambiare nomeKey con il field che si sta analizzando
dic={}
for el in diz:
    activity = diz[el]['nomeKey']
    if activity not in dic:
        dic[activity]=1
    else:
        dic[activity]+=1
print(len(dic.keys()))
dic

a_file = open("dictionary_scraping/db_ciclismo_scraping_quality.pkl", "wb")
pickle.dump(diz, a_file)
a_file.close()