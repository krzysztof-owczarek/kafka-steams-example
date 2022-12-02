
#!/bin/bash

echo "Kafka sources set to=${KAFKA_HOME}"

${KAFKA_HOME}/bin/kafka-console-producer.sh --topic ${TOPIC} --bootstrap-server localhost:9093 --property "parse.key=true" --property "key.separator=:"

