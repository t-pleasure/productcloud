import time, json, sys, os
from amazon_products.product import Product
from amazon_products.product_status import Types, CompletedStatus, ProcessingStatus, json2product
from data_services.mapstore import KVDiskStore, WordCountDisk
from data_services.queue import KafkaTopicQueue
from urllib3 import PoolManager

SLEEP_TIME = 2

product_status_db = KVDiskStore("product_status", 
                      serialize=lambda p:p.tojson(),
                      deserialize=json2product
                      )

pid_to_wordcount_db = KVDiskStore("pid_to_wordcount",
                      serialize=json.dumps,
                      deserialize=json.loads)

global_wordcount_db = WordCountDisk("global_word_count")

pool = PoolManager()
AGGREGATOR_URL=os.environ.get("AGGREGATOR_URL", "localhost:5555")
def send_to_aggregator(wcount):
  pool.urlopen('POST', AGGREGATOR_URL+"/update", 
               headers={'Content-Type':'application/json'}, 
               body=json.dumps(wcount))


if __name__ == "__main__":
  if len(sys.argv) != 2:
    print "usage: queue_consumer.py [queue_topic_id]"

  topic = sys.argv[1]
  queue = KafkaTopicQueue(topic)
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
      wcounts[w] = new_count
    # send words to aggregator for processing
    send_to_aggregator(wcounts)
    # update state of this product to "completed"
    product_status_db.put(pid, CompletedStatus(pid))
