"""
Consumes product urls from task queues and performs processing
"""
import time, json, sys, os, argparse
from amazon_products.product import Product
from amazon_products.product_status import Types, CompletedStatus, ProcessingStatus, json2product
from data_services.mapstore import KVDiskStore, WordCountDisk
from data_services.queue import KafkaTopicQueue
from algos.heaps import MaxCapacityPQDict
from urllib3 import PoolManager

parser = argparse.ArgumentParser()
parser.add_argument('--debug', action="store", default=False, dest='debug', help="set to True if debug messages desired", type=bool)
parser.add_argument('--aggregator_url', action="store", default="localhost:5555", dest='aggregator_url', help="url of aggregator daemon", type=str)
parser.add_argument('--k', action="store", default=100, dest='k', help="number of top largest words to find", type=int)
parser.add_argument('--timeout_secs', action="store", default=2, dest='timeout_secs', help="number of seconds to sleep when queue is empty", type=int)
parser.add_argument('--product_status_db_file', action="store", dest='product_status_db_file', default="product_status", help="file to persist product status db data store")
parser.add_argument('--pid_to_wordcount_db_file', action="store", dest='pid_to_wordcount_db_file', default="pid_to_wordcount", help="file to persist product->wordcounts db data store")
parser.add_argument('--global_wordcount_db_file', action="store", dest='global_wordcount_db_file', default="global_wordcount", help="file to persist product->wordcounts db data store")
# REQUIRED
parser.add_argument('--topic', action="store", dest='topic', help="kafka topics to consume from", required=True)
args = parser.parse_args()

# GLOBAL VARIABLES
SLEEP_TIME = args.timeout_secs
K = args.k
AGGREGATOR_SUBMIT_URL = args.aggregator_url + "/update"

# DATA STORES
product_status_db = KVDiskStore(args.product_status_db_file, 
                      serialize=lambda p:p.tojson(),
                      deserialize=json2product
                      )

pid_to_wordcount_db = KVDiskStore(args.pid_to_wordcount_db_file,
                      serialize=json.dumps,
                      deserialize=json.loads)

global_wordcount_db = WordCountDisk(args.global_wordcount_db_file)

pool = PoolManager()
def send_to_aggregator(pq_dict):
  pool.urlopen('POST', AGGREGATOR_SUBMIT_URL, 
               headers={'Content-Type':'application/json'}, 
               body=pq_dict.tojson())


top_words = MaxCapacityPQDict(K)

if __name__ == "__main__":

  queue = KafkaTopicQueue(args.topic)
  while True:
    pid = queue.pop()
    # sleep if queue doesn't containing anything yet
    if not(pid):
      time.sleep(SLEEP_TIME)
      continue
    status = product_status_db.get(pid)
    # if this product is already being processed, or has been completed, then just ignore
    if status.type != Types.SUBMITTED:
      continue
    # set the state of this product to processing
    product_status_db.put(pid, ProcessingStatus(pid))
    # obtain word counts for this product
    product = Product.fetch_product(pid)
    wcounts = product.wordcounts
    # persist word count information for this product
    pid_to_wordcount_db.put(pid, wcounts)
    # for each word, compute its global word count (throughout history of time)
    for (w, c) in wcounts.items():
      new_count = global_wordcount_db.increment(w, c)
      top_words.pushOrUpdate(w, new_count)
    # send words to aggregator for processing
    send_to_aggregator(top_words)
    # update state of this product to "completed"
    product_status_db.put(pid, CompletedStatus(pid))
