"""
Word aggregator daemon -- takes top-k words from queue instances to form a
                          GLOBAL top-k word count.
"""
import json, argparse
from data_services.mapstore import WordCountDisk
from algos.heaps import MaxCapacityPQDict
from threading import Lock
from flask import Flask, request
app = Flask(__name__)


parser = argparse.ArgumentParser()
parser.add_argument('--debug', action="store", default=False, dest='debug', help="set to True if debug messages desired", type=bool)
parser.add_argument('--port', action="store", default=5555, dest='port', help="port to bind to", type=int)
parser.add_argument('--k', action="store", default=100, dest='k', help="number of top largest words to find", type=int)
parser.add_argument('--global_wordcount_db_file', action="store", dest='global_wordcount_db_file', default="global_wordcount", help="file to persist product->wordcounts db data store")
args = parser.parse_args()


K = args.k
lock = Lock()
# POPULATE INITIAL HEAP WITH DATA FROM GLOBAL WORD COUNT
global_wordcount_db = WordCountDisk(args.global_wordcount_db_file)
top_words = MaxCapacityPQDict(K)
for (word, count) in global_wordcount_db.items():
  top_words.pushOrUpdate(word, count)

################
# ROUTES
################
# default route returns the current top k words
@app.route('/')
def current():
  with lock:
    return top_words.tojson() 

# this update route takes in other instances' top k elements and merges them together
# to form the top_words variable
@app.route('/update', methods=['GET', 'POST'])
def update():
  for (word, count) in request.get_json().items():
    with lock:
      top_words.addOrUpdate(word, count)
  return top_words.tojson()

if __name__ == '__main__':
    app.debug = args.debug
    app.run(port=args.port)

