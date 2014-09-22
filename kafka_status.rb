#!/usr/bin/env ruby
# kafka_status.rb

# Brandon Burton, 2014

require 'rubygems'
require 'zk'
require "json"

# Where kafka lives
kafka_dir = "/opt/kafka/"
kafka_bin = "#{kafka_dir}/bin"
kafka_conf = "#{kafka_dir}/config/server.properties"

# this assumes a hostname like 'lookout-zk-bburton-0', where the third bit is the "cluster name"
kafka_cluster_name = %x(hostname).split('-')[-2]

kafka_cluster_hosts = {}
zookeeper_cluster_hosts = []

# find out what zookeeper cluster this kafka broker uses
zookeeper_cluster_hosts = %x(grep zookeeper.connect #{kafka_conf} | cut -d '=' -f 2).split(",")
zookeeper_host = zookeeper_cluster_hosts[0]
zk = ZK.new(zookeeper_host)

# test that we're able to successfully connect to zookeeper
if zk.state.to_s != "connected"
  puts "Failed to connect to: #{zookeeper_host}"
  exit(1)
else
  #puts "Connected to : #{zookeeper_host}"
end

# get a list of broker ids from zookeeper
kafka_broker_ids = zk.children("/brokers/ids")

# get details on each broker in the cluster
kafka_brokers = {}

kafka_broker_ids.each do | broker_id |
  kafka_broker_details = {}

  # get details about broker in JSON and turn into a hash
  kafka_broker = zk.get("/brokers/ids/#{broker_id}")[0]
  kafka_broker = JSON.parse(kafka_broker)

  # set kafka_broker_host to short hostname
  kafka_broker_host = kafka_broker["host"].split(".")[0]

  # populate kafka_broker_details with what we've learned about the broker
  kafka_broker_details["hostname"] = kafka_broker["host"]
  kafka_broker_details["ip_address"] = %x(host #{kafka_broker["host"]} | cut -d ' ' -f 4)
  kafka_broker_details["broker_id"] = broker_id
  kafka_brokers["#{kafka_broker_host}"] = kafka_broker_details
end

# where you find kafka-topics.sh
kafka_topics_bin = "#{kafka_bin}/kafka-topics.sh"

# get a list of topics in this kafka cluster
kafka_topic_names = %x(#{kafka_topics_bin} --list --zookeeper #{zookeeper_host}).split("\n")

# get details on each kafka topic and build a hash map of it
kafka_topics = {}

kafka_topic_names.each do | topic |

  topic_details = {}

  # describe the details of the topic
  data_topic = %x(#{kafka_topics_bin} --describe --zookeeper #{zookeeper_host} --topic #{topic})

  # we get back the details as a string, let's split the string into an array by newline character
  data_topic = data_topic.split("\n")

  # extract and clean up the config data
  topic_configs = {}

  data_topic_config = data_topic[0].split("\t")
  data_topic_config.delete("Configs:")
  data_topic_config.each do | data |
    data = data.split(":")
    topic_configs["#{data[0]}"] = data[1].strip
  end

  # remove configs from array so we can process partition information
  data_topic.delete_at(0)

  # process partition information
  topic_partitions = {}

  # what's left in data_topic is at least one element about partitions
  # so we'll cycle through however many partitions are there and grab their info
  i = 0
  while data_topic.length() > 0
    topic_partition_details = {}

    # split by tab characters and delete the first element, which just contains
    # the topic name
    data_partition = data_topic[0].split("\t")
    data_partition.delete_at(0)

    data_partition.each do | data |
      data = data.split(":")
      # determine the number of replicas the partition has
      if data[0] == "Replicas"
        # turn this into an array to so we can get its length
        replicas = data[1].strip.split(",")
        topic_partition_details["NumberOfReplicas"] = replicas.length
      else
        topic_partition_details["#{data[0]}"] = data[1].strip
      end
    end

    # we want to store the data about each topic by the topic numeric id
    topic_partitions["partition_#{i.to_s}"] = topic_partition_details

    # remove this element since we're done with it and increment for the next pass
    data_topic.delete_at(0)
    i = i + 1
  end

  # we now have topic_configs and partition info
  topic_details["config"] = topic_configs
  topic_details["partitions"] = topic_partitions
  kafka_topics["#{topic}"] = topic_details
end

# show status
puts "Kafka Cluster Status: #{kafka_cluster_name}"
puts "\s\sThe members of this cluster are:"
kafka_brokers.each do | broker, broker_data |
  puts "\s\s\s\sBroker: #{broker_data['hostname']}"
  puts "\s\s\s\s\s\sBroker ID: #{broker_data['broker_id']}"
  puts "\s\s\s\s\s\sBroker IP: #{broker_data['ip_address']}"
end

puts "\n\s\sThis cluster has the following topics:"
kafka_topics.each do | topic, topic_data |

  puts "\s\s\s\sTopic: #{topic}"
  puts "\s\s\s\s\s\sReplication Factor: #{topic_data['config']['ReplicationFactor']}"
  puts "\s\s\s\s\s\sPartition Count: #{topic_data['config']['PartitionCount']}"
  # partition counts in kafka are zero based
  i = (topic_data["config"]["PartitionCount"].to_i - 1)
  while i >= 0
    if topic_data["partitions"]["partition_#{i}"]["NumberOfReplicas"].to_i == topic_data["config"]["ReplicationFactor"].to_i
    #puts "zomg"
    end
    i = i - 1
  end

end