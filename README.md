kafka-status
============

A simple, opinionated ruby script for checking the health of a kafka cluster

How it works
------------

1. The script reads kafka's `server.properties` file to determine the zookeeper hosts kafka is using
2. The script connects to zookeeper and reads the info of all the brokers in the cluster from `/brokers/ids`
3. The script then uses `kafka-topics.sh` to get a list of topics
4. It uses `--describe` on each topic to get the details of each topic
5. It prints out the list of brokers and their details, such as ID, IP address, hostname
6. It prints out a list of topics, and for each topic it lists out the replication factor and partition count

Usage
-----

1. Set the script to be executable

```
chmod +x kafka_status.rb
```

2. Place the script somewhere like `/usr/local/bin/kafka_status`

3. Run the script, e.g.

```
bburton@lookout-kafka-bburton-2:~$ /usr/local/bin/kafka_status
Kafka Cluster Status: bburton
  The members of this cluster are:
    Broker: lookout-kafka-bburton-1
      Broker ID: 169869504
      Broker IP: 10.1.1.11
    Broker: lookout-kafka-bburton-2
      Broker ID: 169869810
      Broker IP: 10.1.1.12
    Broker: lookout-kafka-bburton-0
      Broker ID: 169869792
      Broker IP: 10.1.1.10

  This cluster has the following topics:
    Topic: bburton.test3
      Replication Factor: 3
      Partition Count: 8
    Topic: bburton.test2
      Replication Factor: 1
      Partition Count: 1
    Topic: bburton.test1
      Replication Factor: 1
      Partition Count: 1
```
