# productcloud

## Single Machine, Single Process:
```
# see:
python simple_solution_api.py
```

## Mocking Distributed System:

```bash
# start aggregator daemon
python word_aggregator.py --port 5555 &> $DEBUG_DIR/agg.log &

# start queues
python queue_consumer.py --topic=Q1 &> $DEBUG_DIR/q1.log &
python queue_consumer.py --topic=Q2 &> $DEBUG_DIR/q2.log &
 
# start http api
python http_api.py --port 8080 --topics Q1 Q2 &> $DEBUG_DIR/http.log &
```
