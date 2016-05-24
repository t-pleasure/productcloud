import json, os, threading
import amazon_products.product_status as pstatus
from amazon_products.product import Product
from data_services.mapstore import KVDiskStore, WordCountDisk
from data_services.queue import KafkaTopicQueue
from algos.heaps import MaxCapacityPQDict
from urllib3 import PoolManager
from flask import Flask, request
app = Flask(__name__)

K = 100

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

# MinHeap containing top k most words
top_words = MaxCapacityPQDict(K)

global_wordcount_lock = threading.Lock()
def increment_global_wordcount(word, inc_amount):
  with global_wordcount_lock:
    new_amt = global_wordcount_db.increment(word, inc_amount)
    return new_amt

@app.route('/', methods=["GET","POST"])
def default():
    if "product_url" not in request.args:  
      return top_words.tojson()

    # extract product id from request
    purl = request.args['product_url']
    pid = Product.url_to_productid(purl)

    # check state to see whether or not product_id has been processed or is being processed
    status = product_status_db.get(pid)
    if status and status.type in [psatus.Types.PROCESSING, pstatus.Types.COMPLETED, pstatus.Types.INVALID]:
        return json.dumps({"pid": pid,
                           "status": status.type,
                           "current_words": top_words.tojson()})
    # if product id is not valid display appropriate message and record in database
    if not Product.isvalid_pid(pid):
      product_status_db.put(pid, pstatus.InvalidStatus(pid))
      return json.dumps({"pid": pid,
                         "status": pstatus.Types.INVALID,
                         "current_words": top_words.tojson()}

    # Change state of datastore to indicate this product is currently being processed
    product_status_db.put(pid, pstatus.ProcessingStatus(pid))
    # obtain product description's wordcount
    product = Product.fetch_product(pid)
    # obtain word count for product description
    wcount = prduct.wordcounts
    # persist word count for product description
    product_worddata_db.put(pid, wcount)
    # update global word counts
    for (word, inc_amt) in wcount.items():
      new_amt = increment_global_wordcount(word, inc_amt)
      top_words.pushOrUpdate(word, new_amt)
    # update  
    product_status_db.put(pid, pstatus.CompletedStatus(pid))
    return top_words.tojson()
      

@app.route('/product_status')
def info():
    return str(product_status_db.items())

if __name__ == '__main__':
    app.debug = True
    app.run(port=6969)
