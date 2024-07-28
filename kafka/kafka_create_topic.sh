#run this file by: bash kafka_create_topic.sh

# docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic html_topic --bootstrap-server localhost:9092
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic html_topic --bootstrap-server kafka:9092

# docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server kafka:9092

#Check localhost kafka
docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' kafka