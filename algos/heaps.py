import heapq

class MinHeap(object):
  def __init__(self, capacity=None):
    """
    input:
      * capacity (int) - max size of queue at any one point in time
    """
    self.heap = []
    self.max_capacity = capacity
    self.n_items = 0

  def push(self, key, value=None):
    if self.max_capacity != None and self.n_items >= self.max_capacity:
      raise Exception('reached max capacity of queue')
    heapq.heappush(self.heap, (key, value))
    self.n_items += 1


  def pop(self):
    if self.n_items == 0:
      raise Exception('attempting to pop from an empty heap')
    self.n_items -= 1
    return heapq.heappop(self.heap)

  def top(self):
    return self.heap[0] if self.heap else None

  def update_key_for_value(self, value, key):
    for (ind, (k, v)) in enumerate(self.heap):
      if v == value:
        self.heap[ind] = (key, value)
        break
    heapq.heapify(self.heap)

  def isEmpty(self):
    return self.n_items == 0

  def isFull(self):
    return self.n_items == self.max_capacity

  def size(self):
    return self.n_items

  def __repr__(self):
    return str(self.heap)

  def values(self):
    return map(lambda elm: elm[1], self.heap)

  def keys(self):
    return map(lambda elm: elm[0], self.heap)

  def items(self):
    return self.heap

class MaxHeap(MinHeap):
  def __init__(self, capacity=None):
    MinHeap.__init__(self, capacity)
  
  def push(self, key, value):
    super(MaxHeap, self).push(-key, value)

  def pop(self):
    (k,v) = super(MaxHeap, self).pop()
    return (-k, v)

  def update_key_for_value(self, value, key):
    super(MaxHeap, self).update_key_for_value(value, -key)

  def top(self):
    (k,v) = self.heap[0]
    return (-k, v)

  def __repr__(self):
    return str([(-k,v) for (k,v) in self.heap])


class IncreasingKeyValueMinHeap:
  def __init__(self, capacity=None):
    self.minheap = MinHeap(capacity=capacity)
    self.item2priority = dict()

  def addOrUpdate(self, item, priority):
    # if the value is already in our minheap, we will simply need to update it
    if item in self.item2priority:
      print "updating", item, "with", priority
      self.item2priority[item] = priority
      self.minheap.update_key_for_value(item, priority)
      return
    # if the heap is full, we will need to potential kick an item out
    if self.minheap.isFull():
      print "HE"
      (min_p, min_v) = self.minheap.top()
      if priority > min_p:
        print "HO", priority, min_p
        self.minheap.pop()
        del self.item2priority[min_v]
        self.minheap.push(priority, item)
        self.item2priority[item] = priority
    else:
      print "junk"
      self.minheap.push(priority, item)
      self.item2priority[item] = priority

if __name__ == "__main__":
  """
  hp = MaxHeap()
  items = [-3,4,100,2,200,3, -1000]
  print "original input: ", items
  for elm in items:
    hp.push(elm, None)
  while not(hp.isEmpty()):
    print hp.pop()
  """
  hp = IncreasingKeyValueMinHeap(capacity=2)
  for v,p in {"hi": 100, "by": 2, "oh": -1}.items():
    hp.addOrUpdate(v,p)
  print hp.minheap
  hp.addOrUpdate(300, "oh")
  hp.addOrUpdate(302, "key")
  print hp.minheap.heap, "BEFORE"
  hp.minheap.pop()
  print hp.minheap.heap, "AFTER "

  
