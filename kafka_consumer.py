
from kafka import KafkaConsumer
import json
import time

import redis
r = redis.Redis(
    host='localhost',
    port=6379)
    
consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
                                 auto_offset_reset='earliest',
                                 value_deserializer=lambda m: json.loads(m.decode('utf-8')))
consumer.subscribe(['bitcoin'])

current_secs = lambda: int(round(time.time()))


count = 0
while True:
    raw_msgs = consumer.poll(timeout_ms=100000)
    for tp, msgs in raw_msgs.items():
        for msg in msgs:
            msg = json.loads(msg.value)
            if current_secs() - msg['x']['time'] <= 10800:
               print(msg)
            