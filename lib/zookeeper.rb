# zookeeper.rb

require 'zk'

def connect_to_zookeeper()

  kafka_cluster_hosts = {}
  zookeeper_cluster_hosts = []

  # find out what zookeeper cluster this kafka broker uses
  zookeeper_cluster_hosts = File.read($kafka_config).split("\n").grep(/zookeeper\.connect/)[0].split('=')[1]
  zk = ZK.new(zookeeper_cluster_hosts)
  zk.wait_until_connected(30)

  return zk, zookeeper_cluster_hosts
end