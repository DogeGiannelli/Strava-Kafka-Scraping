#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from kafka import KafkaConsumer
from datetime import datetime
import time
import pickle


consumer = KafkaConsumer(
     'Corsa',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='latest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))



diz_CORSA={}
for message in consumer:
    message = message.value
    diz_CORSA.update(message)

a_file = open("dictionary_scraping/diz_CORSA.pkl", "wb")
pickle.dump(diz_CORSA, a_file)
a_file.close()
