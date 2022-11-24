
#!/bin/bash

echo "Kafka sources set to=${KAFKA_HOME}"

${KAFKA_HOME}/bin/kafka-console-consumer.sh --topic output-topic --bootstrap-server localhost:9093

