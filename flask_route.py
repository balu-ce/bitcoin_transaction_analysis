
import redis
r = redis.Redis(
    host='localhost',
    port=6379)
    
from flask import Flask
app = Flask(__name__)
import time 
import json 
from flask import jsonify

from flask import request

current_secs = lambda: int(round(time.time()))

@app.route('/transactions_count_per_minute')
def index():
    min_value = request.args.get('min_value', default = 1, type = int)
    temp = current_secs()/60
    y = r.zrevrangebyscore('min_count_time_ref', temp , temp-59)
    myList  = []
    for x in y : 
        count = r.hget('min_count',x.decode("utf-8"))    
        j = {"time":x.decode("utf-8"),"count":count.decode("utf-8")}
        myList.append(j)
    return jsonify(myList)

@app.route('/show_transactions')
def show_transactions():
    y = r.zrevrange('all_transaction', 0 ,99)
    myList  = []
    for x in y :
        z = json.loads(x)
        myList.append(z)
    return jsonify(myList)

def sort_count(json):
    try:
        return int(json['count'])
    except KeyError:
        return 0
        
@app.route('/high_value_addr')
def high_value_addr():
    temp = current_secs()/60
    y = r.zrevrangebyscore('addr', temp , temp-179)
    myList  = []
    for x in y : 
        count = r.hget('address_count',x.decode("utf-8"))    
        j = {"address":x.decode("utf-8"),"total_value":count.decode("utf-8")}
        myList.append(j)
    myList.sort(key=sort_count, reverse=True)
    return jsonify(myList)
            
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3000)