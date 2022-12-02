
#!/bin/bash

echo "Kafka sources set to=${KAFKA_HOME}"

${KAFKA_HOME}/bin/kafka-topics.sh --create --topic car-manufacturers --replication-factor 1 --partitions 1 --bootstrap-server localhost:9093 --if-not-exists --config cleanup.policy=compact

${KAFKA_HOME}/bin/kafka-topics.sh --create --topic input-topic --replication-factor 1 --partitions 1 --bootstrap-server localhost:9093 --if-not-exists

${KAFKA_HOME}/bin/kafka-topics.sh --create --topic output-topic --replication-factor 1 --partitions 1 --bootstrap-server localhost:9093 --if-not-exists

${KAFKA_HOME}/bin/kafka-topics.sh --create --topic xxx --replication-factor 1 --partitions 1 --bootstrap-server localhost:9093 --if-not-exists

