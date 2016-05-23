from kafka.client import SimpleClient
from kafka import SimpleConsumer, SimpleProducer


class KafkaTopicQueue:
  def __init__(self, topic, host="localhost:9092"):
    self.topic = topic
    self.group = "group-for-%s"%(self.topic)
    self.kafka = SimpleClient(host)
    self.producer = SimpleProducer(self.kafka)
    self.consumer = SimpleConsumer(self.kafka, self.group, self.topic)

  def push(self, v):
    self.producer.send_messages(self.topic, v)

  def pop(self):
    item = self.consumer.get_message()
    return item.message.value if item else None
