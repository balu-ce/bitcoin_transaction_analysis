#!/usr/bin/python
import websocket
import _thread 
import time
import json
from kafka import KafkaProducer

import datetime


import redis
r = redis.Redis(
    host='localhost',
    port=6379)
    
key_value = 0
count = 1
prev_time = 0
    
producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda v: json.dumps(v).encode('utf-8'))

current_milli_secs = lambda: int(round(time.time()*1000))



def on_error(ws, error):
    print(error)

def on_close(ws):
    print("### closed ###")
    


def on_message(ws, message):
    j = json.loads(message)
    t_time = (j["x"]["time"])
    temp =  int(t_time/60)
    now = datetime.datetime.fromtimestamp(t_time).strftime("%H:%M")
    r.zadd('min_count_time_ref', {now : temp } ) 
    r.hincrby('min_count',now,1)
    for x in j["x"]["out"]:
        if x['value'] != 0 :
           r.zadd('addr', {x['addr'] : temp } )
           r.hincrby('address_count',x['addr'] ,x['value'])
    r.zadd('all_transaction', {message : temp } )
    producer.send('bitcoin',message)

 
   


def on_open(ws):
    ws.send(json.dumps({"op":"unconfirmed_sub"}))


if __name__ == "__main__":
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp("wss://ws.blockchain.info/inv",
                                on_message = on_message,
                                on_error = on_error,
                                on_close = on_close)
    ws.on_open = on_open

    ws.run_forever()