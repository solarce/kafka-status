# # consumer.rb

require 'lib/zookeeper'
require 'simple-graphite'

def underscore(metric)
  metric.gsub(/::/, '/').
    gsub(/([A-Z]+)([A-Z][a-z])/,'\1_\2').
    gsub(/([a-z\d])([A-Z])/,'\1_\2').
    tr("-", "_").
    downcase
end

def dot_to_underscore(name)
  metric.gsub(/::/, '/').
    gsub(/([A-Z]+)([A-Z][a-z])/,'\1_\2').
    gsub(/([a-z\d])([A-Z])/,'\1_\2').
    tr("-", "_").
    downcase
end

def send_metric(consumer_group, metric_name, metric_value)
  metric_prefix = "storm.consumer_lag.#{$environment}"
  metric_input = "#{metric_prefix}.#{consumer_group}.#{metric_name}"
  g = Graphite.new
  g.host = $graphite_host
  g.port = 2003

  if metric_value == nil
    metric_value = 0
  end
  g.send_metrics({
    metric_input => metric_value
  })
end

def get_partition_offsets(topic_name)
  partition_offsets = {}
  total_topic_offset = 0.0
  partition_offsets_data = %x(#{$kafka_bin}/kafka-run-class.sh kafka.tools.GetOffsetShell \
    --broker-list localhost:6667 --topic #{topic_name} \
    --time -1 2> /dev/null).split("\n")

  partition_offsets_data.each do | partition |
    partition = partition.split(":")
    partition_name = "partition_#{partition[1]}"
    offset = partition[2].to_f
    partition_offsets[partition_name] = offset
    total_topic_offset = total_topic_offset + offset
  end

  partition_offsets["total_topic_offset"] = total_topic_offset
  return partition_offsets
end

def get_consumer_group_details(consumer_group, zk)
  # A consumer group will always map to a single partition

  consumer_group_info = {}
  consumer_group_partition_info = {}
  topic_name = ""

  topology_partitions = zk.children("/kafka_spout/#{consumer_group}")
  topology_partitions.each do | partition_name |
    lag_data = JSON.parse(zk.get("/kafka_spout/#{consumer_group}/#{partition_name}")[0])
    partition_name = "partition_#{lag_data['partition']}"
    topic_name = lag_data['topic']
    offset = lag_data['offset']
    consumer_group_partition_info[partition_name] = offset
  end

  consumer_group_info[topic_name] = consumer_group_partition_info
  return consumer_group_info
end

def partition_consumer_data(consumer_group_name, topic_name, topic_data)
  # calculates partition_consumer_lag and stores it with the
  # partition offset

  # convert any dots to dashes in the topic name because
  # dots are used as the separate in graphite metric paths
  metric_topic_name = topic_name.tr(".", "-")
  partition_consumer_data = {}
  total_topic_lag = 0.0

  partition_offsets = get_partition_offsets(topic_name)
  topic_data.each do | partition_name, consumer_offset |

    # partition_consumer_lag is the current partition head offset value minus
    # the current position of the offset of consumer
    partition_consumer_lag = partition_offsets[partition_name] - consumer_offset

    # send in per partition consumer lag
    send_metric(consumer_group_name, "#{metric_topic_name}.#{partition_name}.consumer_lag", partition_consumer_lag)
    # send in per partition offset
    send_metric(consumer_group_name, "#{metric_topic_name}.#{partition_name}.consumer_offset", consumer_offset)

    # calculate total_topic_lag
    total_topic_lag = total_topic_lag + partition_consumer_lag
  end

  # send in per topic total consumer lag
  send_metric(consumer_group_name, "#{metric_topic_name}.total_offset", partition_offsets["topic_total_offset"])
  # send in per topic total offset
  send_metric(consumer_group_name, "#{metric_topic_name}.total_consumer_lag", total_topic_lag)
end

def consumer_lag()

  if $verbose
    puts "Getting Consumer Lag:"
  end

  zk, zookeeper_cluster_hosts = connect_to_zookeeper()
  consumer_group_data = {}

  # get a list of storm topology consumer groups from zookeeper
  topology_consumer_groups = zk.children("/kafka_spout")

  topology_consumer_groups.each do | consumer_group |
    consumer_group_data[consumer_group] = get_consumer_group_details(consumer_group, zk)
  end

  consumer_group_data.each do | consumer_group_name, consumer_group_data |
    if $verbose
      puts "\s\sGetting Lag for #{consumer_group_name}"
    end
    consumer_group_data.each do | topic_name, topic_data |
      partition_consumer_data(consumer_group_name, topic_name, topic_data)
    end
  end 
end