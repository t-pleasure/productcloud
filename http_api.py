"""
The HTTP server where clients can submit requests to for processing
"""
import argparse, json, os
import amazon_products.product_status as pstatus
from amazon_products.product import Product
from data_services.mapstore import KVDiskStore
from data_services.queue import KafkaTopicQueue
from urllib3 import PoolManager
from flask import Flask, request
app = Flask(__name__)


### PARSE ARGUMENTS ###
parser = argparse.ArgumentParser()
parser.add_argument('--debug', action="store", default=False, dest='debug', help="set to True if debug messages desired", type=bool)
parser.add_argument('--port', action="store", default=8090, dest='port', help="port to bind to", type=int)
parser.add_argument('--aggregator_url', action="store", default="localhost:5555", dest='aggregator_url', help="url of aggregator daemon", type=str)
parser.add_argument('--k', action="store", default=100, dest='k', help="number of top largest words to find", type=int)
parser.add_argument('--processing_timeout_secs', action="store", default=120,
                    dest='processing_timeout_secs', 
                    help="the number of seconds a product can be in the processing state before retries are permitted", 
                    type=int)
parser.add_argument('--product_status_db_file', action="store", dest='product_status_db_file', default="product_status", help="file to persist product status db data store")
# -- REQUIRED ARG --
parser.add_argument('--topics', action="store", default=[], nargs="*", dest='topics', help="kafka topics to WRITE TO", required=True)
args = parser.parse_args()

# define constants
PROCESSING_RETRY_THRESHOLD = args.processing_timeout_secs
N_HASH_PARTITIONS=len(args.topics)
task_queues = [KafkaTopicQueue(topic) for topic in args.topics]

# helper function to obtain the correct kafka queue + topic
def pid_to_queue(pid):
  return task_queues[hash(pid) % N_HASH_PARTITIONS]

# data store for product -> processing status
product_status_db = KVDiskStore(args.product_status_db_file, 
                      serialize=lambda p:p.tojson(),
                      deserialize=pstatus.json2product
                      )

# helper functions and variables for communicating with aggregator daemon
pool = PoolManager()
AGGREGATOR_URL = args.aggregator_url
def get_latest_k_words(): 
  # GET LATEST TOP K WORDS FROM AGGREGATOR DAEMON
  return pool.urlopen('GET', AGGREGATOR_URL).data


###########
# ROUTES
###########
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
        return json.dumps({"status": status.type, "pid": pid})
      elif status.type in ["PROCESSING", "SUBMITTED"] and cur_time - status.time_stamp < PROCESSING_RETRY_THRESHOLD:
        return json.dumps({"status": status.type, "pid": pid})
 
    # product has not been processed yet, so first check to see if it's even valid
    if not Product.isvalid_pid(pid):
      product_status_db.put(pid, pstatus.InvalidStatus(pid))
      return json.dumps({"status": "INVALID_PRODUCT_ID", "pid": pid})
    # push the current product into the task queue and update its status
    pid_to_queue(pid).push(str(pid))
    product_status_db.put(pid, pstatus.SubmittedStatus(pid))
    return json.dumps({"status": "SUBMITTED", "pid": pid})
      

@app.route('/product_status')
def info():
    return str(product_status_db.items())

if __name__ == '__main__':
    app.debug = args.debug
    app.run(port=args.port)
