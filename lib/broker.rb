# broker.rb
require 'json'

def get_broker_ids(zk)
  # get a list of broker ids from zookeeper
  kafka_broker_ids = zk.children("/brokers/ids")

  return kafka_broker_ids
end

def get_kafka_cluster_name()
  # this assumes a hostname like 'lookout-zk-bburton-0', where the third bit is the "cluster name"
  kafka_cluster_name = %x(hostname).split('-')[-2]

  return kafka_cluster_name
end

def get_broker_details(kafka_broker_ids, zk)
  # get details on each broker in the cluster
  kafka_brokers = {}

  kafka_broker_ids.each do | broker_id |

    # get details about broker in JSON and turn into a hash
    kafka_broker = zk.get("/brokers/ids/#{broker_id}")[0]
    kafka_broker = JSON.parse(kafka_broker)

    # set kafka_broker_host to short hostname
    kafka_broker_host = kafka_broker["host"].split(".")[0]

    # populate kafka_brokers with what we've learned about the broker
    kafka_brokers[kafka_broker_host] = {
      "hostname" => kafka_broker["host"],
      "broker_id" => broker_id,
      "ip_address" => IPSocket.getaddress(kafka_broker['host'])
    }

  end

  return kafka_brokers
end

def get_topic_details(kafka_brokers, zookeeper_cluster_hosts)
  # where you find kafka-topics.sh
  kafka_topics_bin = "#{$kafka_bin}/kafka-topics.sh"

  # get a list of topics in this kafka cluster
  kafka_topic_names = %x(#{kafka_topics_bin} --list --zookeeper #{zookeeper_cluster_hosts}).split("\n")

  # get details on each kafka topic and build a hash map of it
  kafka_topics = {}

  kafka_topic_names.each do | topic |

    topic_details = {}

    # describe the details of the topic
    data_topic = %x(#{kafka_topics_bin} --describe --zookeeper #{zookeeper_cluster_hosts} --topic #{topic})

    # we get back the details as a string, let's split the string into an array by newline character
    data_topic = data_topic.split("\n")

    # extract and clean up the config data
    topic_configs = {}

    data_topic_config = data_topic[0].split("\t")
    data_topic_config.delete("Configs:")
    data_topic_config.each do | data |
      key, value = data.split(":")
      topic_configs[key] = value.strip
    end

    # remove configs from array so we can process partition information
    data_topic.shift

    # process partition information
    topic_partitions = {}

    # what's left in data_topic is at least one element about partitions
    # so we'll cycle through however many partitions are there and grab their info
    data_topic.each do | data_partition |
      topic_partition_details = {}

      # the data is tab character separated, so we'll split it up in an array
      data_partition = data_partition.split("\t")

      # since there is a leading tab character we can get rid of the first element
      data_partition.shift

      data_partition.each do | data |
        key, value = data.split(":")
        # determine the number of replicas the partition has
        if key == "Replicas"
          # turn this into an array to so we can get its length
          replicas = value.strip.split(",")
          topic_partition_details["NumberOfReplicas"] = replicas.length
        else
          topic_partition_details[key] = value.strip
        end
      end

      # we want to store the data about each topic by the topic numeric id
      partition_numeric_id = topic_partition_details["Partition"]
      topic_partitions["partition_#{partition_numeric_id}"] = topic_partition_details

    end

    # we now have topic_configs and partition info
    topic_details["config"] = topic_configs
    topic_details["partitions"] = topic_partitions
    kafka_topics[topic] = topic_details
  end

  return kafka_topics
end
