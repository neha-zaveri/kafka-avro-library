# kafka-library

The kafka-library is a python based library.

### Pre-requisites
- Python 3.6 or above
- Poetry

### Install dependencies
`$ poetry install`

### Running containers in local
- Start Containers
`docker compose up &`
- Verify Containers are started successfully 
  `docker compose ps`
  
- Create Topic - Link
 
      curl --location --request POST 'localhost:8082/v3/clusters/0El9sA1xQamn3a2ySmea8A/topics' \
       --header 'Content-Type: application/json' \
       --data-raw ' {
             "topic_name": "user"
       }

- Register Schema

       curl --location --request POST 'http://localhost:8081/subjects/user/versions' \
        --header 'Content-Type: application/json' \
       --data-raw '
        {"schema": "{\"name\":\"user_management\",\"type\":\"record\",\"namespace\":\"com.admin\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"id\",\"type\":\"string\"}]}"}'

- Publishing Message 

- Consuming Message

### Run tests
`$ poetry run pytest --docker-compose=docker-compose.yml` Running tests starts confluent kafka stack using docker-compose.



### Example Usage
#### Producer 
- The  Producer in `producer` module implements latest Kafka 
- The `send_message()` method will emit the ProducerMessage object onto the topic

```python
import uuid

from kafka_avro_library.message import create_producer_message
from kafka_avro_library.producer import AvroProducer

producer = AvroProducer(producer_config, topic="test-topic")
message = create_producer_message(
    value={"type": "Data", "id": str(uuid.uuid4())},
    trace_id="trace_id",
    name="",
)
producer.send_message(message)
```

#### Consumer
```python
from kafka_avro_library.consumer import AvroConsumer

__consumer = AvroConsumer(consumer_config, "test-topic")
__consumer.subscribe(on_next=do_something_on_receiving_message)
```
