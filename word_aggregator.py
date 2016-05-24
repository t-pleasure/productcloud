import json
from data_services.mapstore import WordCountDisk
from algos.heaps import MaxCapacityPQDict
from threading import Lock
from flask import Flask, request
app = Flask(__name__)

K = 100
lock = Lock()
# POPULATE INITIAL HEAP WITH DATA FROM GLOBAL WORD COUNT
global_wordcount_db = WordCountDisk("global_word_count")
top_words = MaxCapacityPQDict(K)
for (word, count) in global_wordcount_db.items():
  top_words.pushOrUpdate(word, count)

@app.route('/')
def current():
  with lock:
    return top_words.tojson()

@app.route('/update', methods=['GET', 'POST'])
def update():
  for (word, count) in request.get_json().items():
    with lock:
      top_words.addOrUpdate(word, count)
  return top_words.tojson()

if __name__ == '__main__':
    app.debug = True
    app.run(port=5555)

