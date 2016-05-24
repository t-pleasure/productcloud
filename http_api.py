import json, os
import amazon_products.product_status as pstatus
from amazon_products.product import Product
from data_services.mapstore import KVDiskStore
from data_services.queue import KafkaTopicQueue
from urllib3 import PoolManager
from flask import Flask, request
app = Flask(__name__)

PROCESSING_RETRY_THRESHOLD = 5*600 # seconds
N_HASH_PARTITIONS=3

queues = [KafkaTopicQueue("topic_%d"%n) for n in range(N_HASH_PARTITIONS)]
def pid_to_queue(pid):
  return queues[hash(pid) % N_HASH_PARTITIONS]

product_status_db = KVDiskStore("product_status", 
                      serialize=lambda p:p.tojson(),
                      deserialize=pstatus.json2product
                      )

pool = PoolManager()
AGGREGATOR_URL=os.environ.get("AGGREGATOR_URL", "localhost:5555")
def get_latest_k_words(): 
  return pool.urlopen('GET', AGGREGATOR_URL).data

@app.route('/', methods=["GET","POST"])
def default():
    #get current time
    cur_time = pstatus.timestamp_now()

    if "product_url" not in request.args:  
      return get_latest_k_words()
    
    # extract product id from request
    purl = request.args['product_url']
    pid = Product.url_to_productid(purl)
    # check state to see whether or not product_id has been processed or is being processed
    status = product_status_db.get(pid)
    if status:
      if status.type == "COMPLETED":
        return json.dumps({"status": status.type})
      elif status.type in ["PROCESSING", "SUBMITTED"] and cur_time - status.time_stamp < PROCESSING_RETRY_THRESHOLD:
        return json.dumps({"status": status.type})
 
    # no status message available which means we need to check for validity
    # check to make sure the product_id is valid
    # and if it is, add the product_id to the valid queue
    if Product.isvalid_pid(pid):
      pid_to_queue(pid).push(str(pid))
      product_status_db.put(pid, pstatus.SubmittedStatus(pid))
      return json.dumps({"status": "SUBMITTED"})
    # if the product id is not valid, record this
    else:
      product_status_db.put(pid, pstatus.Invalid(pid))
      return json.dumps({"status": "INVALID_PRODUCT_ID"})
      

@app.route('/product_status')
def info():
    return str(product_status_db.items())

if __name__ == '__main__':
    app.debug = True
    app.run()
