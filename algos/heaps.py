import threading, pqdict, sys, json

class MaxCapacityPQDict(object):
  """
  Wrapper class around PQDict to allow for easier management
  of objects when the underlying priority queue reaches a maximum
  capacity
  """
  def __init__(self, max_capacity=sys.maxint):
    self.max_capacity = max_capacity
    self.pq = pqdict.PQDict()
    self.lock = threading.Lock()

  def pushOrUpdate(self, key, value):
    with self.lock:
      if key in self.pq:
        self.pq[key] = value
      elif len(self.pq) == self.max_capacity:
        min_val = self.pq[self.pq.top()]
        if value > min_val:
          self.pq.pop()
          self.pq[key] = value
      else:
        self.pq[key] = value

  def pop(self):
    with self.lock:
      if len(self.pq) > 0:
        top_key = self.pq.top()
        top_val = self.pq[top_key]
        self.pq.pop()
        return (top_key, top_val)

  def top(self):
    with self.lock:
      if len(self.pq)>0:
        top_key = self.pq.top()
        return (top_key, self.pq[top_key])

  def items(self):
    with self.lock:
      return [_ for _ in self.pq.items()]

  def tojson(self):
    return json.dumps(dict(self.pq.items()))
