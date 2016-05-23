import json
from data_services.mapstore import WordCountDisk
from algos.heaps import IncreasingKeyValueMinHeap
from threading import Lock
from flask import Flask, request
app = Flask(__name__)

K = 100
lock = Lock()
# POPULATE INITIAL HEAP WITH DATA FROM GLOBAL WORD COUNT
global_wordcount_db = WordCountDisk("global_word_count")
heap = IncreasingKeyValueMinHeap(capacity=K)
for (word, count) in global_wordcount_db.items():
  heap.addOrUpdate(count, word)

print heap.minheap.max_capacity, heap.minheap.n_items
@app.route('/')
def current():
  ret_str = ""
  with lock:
    ret_str = json.dumps(heap.item2priority)
  return ret_str

@app.route('/update', methods=['GET', 'POST'])
def update():
  for (word, count) in request.get_json().items():
    with lock:
      heap.addOrUpdate(count, word)
  return json.dumps(heap.item2priority)

if __name__ == '__main__':
    app.debug = True
    app.run(port=5555)

