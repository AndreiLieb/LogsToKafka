# LogsToKafka

## Requirements:
```
wget -qO - http://packages.confluent.io/deb/3.2/archive.key | sudo apt-key add -

add-apt-repository "deb [arch=amd64] http://packages.confluent.io/deb/3.2 stable main"

apt-get update

apt-get install librdkafka1 librdkafka-dev libavro-c1 libavro-cpp1 libavro-c-dev libavro-cpp-dev confluent-libserdes1

pip install fastavro
pip install avro
pip install confluent-kafka
```