#!/usr/bin/python
"""
STAND ALONE SCRIPT TO RUN SINGLE PROCESS SOLUTION.
"""

import json, os, threading
import amazon_products.product_status as pstatus
from amazon_products.product import Product
from data_services.mapstore import KVDiskStore, WordCountDisk
from data_services.queue import KafkaTopicQueue
from algos.heaps import MaxCapacityPQDict
from urllib3 import PoolManager
from flask import Flask, request
app = Flask(__name__)

# DataStore to persist information about status of products and if we've processed them yet
product_status_db = KVDiskStore("product_status_SINGLE", 
                      serialize=lambda p:p.tojson(),
                      deserialize=pstatus.json2product)

# DataStore to persist product_id -> word_counts
product_worddata_db = KVDiskStore("product_data_SINGLE",
                      serialize=json.dumps,
                      deserialize=json.loads)

# Datastore to persist ALL WORD COUNTS
global_wordcount_db = WordCountDisk("global_word_count_SINGLE")

# file to persist top k elements to
persist_top_k_file = None

# MinHeap containing top k most words
K = 100 # default value for K
top_words = MaxCapacityPQDict(K)

global_wordcount_lock = threading.Lock()
def increment_global_wordcount(word, inc_amount):
  with global_wordcount_lock:
    new_amt = global_wordcount_db.increment(word, inc_amount)
    return new_amt

# helper method associate a product id with a lock
NLOCKS = 100 # default number of locks
pidlocks = [threading.Lock() for _ in range(NLOCKS)]
def pid2lock(pid):
  return pidlocks[hash(pid)%NLOCKS]

#################
# ROUTES    #   #
#################
@app.route('/', methods=["GET","POST"])
def default():
  # if product_url is not specified, simply return the top k words
  if "product_url" not in request.args:  
    return top_words.tojson()

  # extract product id from request
  purl = request.args['product_url']
  pid = Product.url_to_productid(purl)

  ## crucial region
  ## lock the following block based on the product id as to avoid
  ## potential race conditions when dealing with requests for the same product.
  ## this ensures that only one thread can be processing an id at a time
  with pid2lock(pid):
    # check state to see whether or not product_id has been processed or is being processed
    status = product_status_db.get(pid)
    if status and status.type in [pstatus.Types.PROCESSING, pstatus.Types.COMPLETED, pstatus.Types.INVALID]:
      return json.dumps({"pid": pid,
                           "status": status.type,
                           "current_words": dict(top_words.items())})
    # if product id is not valid display appropriate message and record in database
    if not Product.isvalid_pid(pid):
      product_status_db.put(pid, pstatus.InvalidStatus(pid))
      return json.dumps({"pid": pid,
                         "status": pstatus.Types.INVALID,
                         "current_words": dict(top_words.items())})
    # Change state of datastore to indicate this product is currently being processed
    product_status_db.put(pid, pstatus.ProcessingStatus(pid))

  # obtain product description
  product = Product.fetch_product(pid)
  # obtain word count for product description
  wcount = product.wordcounts
  # persist word count for product description
  product_worddata_db.put(pid, wcount)
  # update global word counts
  for (word, inc_amt) in wcount.items():
    new_amt = increment_global_wordcount(word, inc_amt)
    top_words.pushOrUpdate(word, new_amt)
  # update status for product to indicate completion
  product_status_db.put(pid, pstatus.CompletedStatus(pid))
  # persist top_k words
  if persist_top_k_file:
    with open(persist_top_k_file, 'w') as f:
      f.write(top_words.tojson())
  return json.dumps({"pid": pid,
                     "status": pstatus.Types.COMPLETED,
                     "current_words": dict(top_words.items())})
      

@app.route('/product_status')
def info():
    return str(product_status_db.items())

if __name__ == '__main__':
  import argparse
  
  parser = argparse.ArgumentParser()
  parser.add_argument('--port', action="store", default=9999, dest='port', help="port to bind to", type=int)
  parser.add_argument('--k', action="store", default=100, dest='k', help="number of top largest words to find", type=int)
  parser.add_argument('--top-k-file', action="store", default=None, dest='top_k_file', help="file to persist top k largest words")
  parser.add_argument('--n-locks', action="store", default=100, dest='n_locks', help="number of locks to have for coordinating product parsing")
  parser.add_argument('--debug', action="store", default=False, dest='debug', help="debug flag", type=bool)
  args = parser.parse_args()

  # update global variables
  persist_top_k_file = args.top_k_file
  K = args.k
  NLOCKS = args.n_locks

  # compute top_words
  top_words = MaxCapacityPQDict(K)
  for (w,c) in global_wordcount_db.items():
    top_words.pushOrUpdate(w,c)

  # app settings
  app.debug = args.debug
  app.run(port=args.port)
