# productcloud

## Single Machine, Single Process:
```bash
# see:
python simple_solution_api.py --port 8080

# send request
curl localhost:8080?product_url=$PRODUCT_URL
```

## Mocking Distributed System:

```bash
## 
# YOU MUST START KAFKA LOCALLY (assuming on port 9092)

# start aggregator daemon
python word_aggregator.py --port 5555 &> $DEBUG_DIR/agg.log &

# start queue consumers
python queue_consumer.py --topic=Q1 &> $DEBUG_DIR/q1.log &
python queue_consumer.py --topic=Q2 &> $DEBUG_DIR/q2.log &
 
# start http api
python http_api.py --port 8080 --topics Q1 Q2 &> $DEBUG_DIR/http.log &

# send request
curl localhost:8080?product_url=$PRODUCT_URL
```

## Dependencies
* Flask

* kafka-python

* lsm-db

* pqdict

* urllib3
