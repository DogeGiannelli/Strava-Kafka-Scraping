#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from kafka import KafkaConsumer
from pymongo import MongoClient
from json import loads
import pickle



consumer = KafkaConsumer(
     'Sport_Acquatici',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='latest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))



diz_NUOTO={}
for message in consumer:
    message = message.value
    diz_NUOTO.update(message)

a_file = open("dictionary_scraping/diz_NUOTO.pkl", "wb")
pickle.dump(diz_NUOTO, a_file)
a_file.close()