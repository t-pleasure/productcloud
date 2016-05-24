from lsm import LSM

identity_fn = lambda _:_

class KVDiskStore:
  def __init__(self, id, serialize=identity_fn, deserialize=identity_fn):
    self.dbfile = "%s.kvdb"%id
    self.db = LSM(self.dbfile)
    self.serialize = serialize
    self.deserialize = deserialize

  def put(self, k, v):
    self.db[k] = self.serialize(v)

  def get(self, k, defaultVal=None):
    if k in self.db: 
      return self.deserialize(self.db[k])
    else:
      return defaultVal

  def exists(self, word):
    return word in self.db

  def items_gen(self):
    for (k,v) in self.db:
      yield (k, self.deserialize(v))

  def items(self):
    return [_ for _ in self.items_gen()]


class WordCountDisk(KVDiskStore):
  def __init__(self, id):
    KVDiskStore.__init__(self, id, serialize=lambda d:str(d), deserialize=lambda s:int(s))
    
  def increment(self, word, amount):
    if self.exists(word):
      new_amt = self.get(word) + amount
      self.put(word, new_amt)
      return new_amt
    else:
      self.put(word, amount)
      return amount
