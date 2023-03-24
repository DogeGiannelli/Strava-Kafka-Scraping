# WINDOWS

cd to \kafka_2.12-3.1.0\bin\windows

# start zookeeper
zookeeper-server-start.bat ..\..\config\zookeeper.properties

# start kafka
kafka-server-start.bat ..\..\config\server.properties

# creazione topic (facoltativo)
kafka-topics.bat --create --topic ciclismo --bootstrap-server localhost:9092



# MacOS

#start zookeeper
kafka_2.13-3.1.0 % sh bin/zookeeper-server-start.sh config/zookeeper.properties

#start kafka
kafka_2.13-3.1.0 % sh bin/kafka-server-start.sh config/server.properties