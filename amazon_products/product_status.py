from datetime import datetime
import json, calendar

def datetime_now():
  return datetime.utcnow()

def timestamp_now():
  return dt_to_timestamp(datetime.utcnow())

def dt_to_timestamp(dtime):
  return calendar.timegm(dtime.timetuple())

def timestamp_to_dt(ts):
  return datetime.utcfromtimestamp(ts)

def json2product(jstr):
  d = json.loads(jstr)
  if d["type"] in [Types.INVALID, Types.SUBMITTED, Types.PROCESSING, Types.COMPLETED]:
    ts = int(d["time_stamp"])
    return ProductStatus(d["pid"], d["type"], time_stamp=ts)

class Types:
  INVALID = "INVALID"
  SUBMITTED = "SUBMITTED"
  PROCESSING = "PROCESSING"
  COMPLETED = "COMPLETED"

class ProductStatus:
  def __init__(self, pid, type_str, time_stamp=None):
    self.pid = pid
    self.type = type_str
    self.time_stamp = time_stamp if time_stamp else timestamp_now()

  def tojson(self):
    return json.dumps(
      {"pid": self.pid,
      "type": self.type,
      "time_stamp": self.time_stamp})

  def __repr__(self):
    return self.tojson()


class InvalidStatus(ProductStatus):
  def __init__(self, pid, time_stamp=None):
    ProductStatus.__init__(self, pid, Types.INVALID, time_stamp=time_stamp) 

class SubmittedStatus(ProductStatus):
  def __init__(self, pid, time_stamp=None):
    ProductStatus.__init__(self, pid, Types.SUBMITTED, time_stamp=time_stamp) 

class ProcessingStatus(ProductStatus):
  def __init__(self, pid, time_stamp=None):
    ProductStatus.__init__(self, pid, Types.PROCESSING, time_stamp=time_stamp) 

class CompletedStatus(ProductStatus):
  def __init__(self, pid, time_stamp=None):
    ProductStatus.__init__(self, pid, Types.COMPLETED, time_stamp=time_stamp) 
