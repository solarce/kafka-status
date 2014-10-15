#!/usr/bin/env ruby
# cli.rb

# Brandon Burton, 2014

require 'rubygems'
require 'trollop'
require 'lib/status'

SUB_COMMANDS = %w(status consumer_lag)
global_opts = Trollop::options do
  banner "Kafka Status Utility"
  stop_on SUB_COMMANDS
end

cmd = ARGV.shift # get the subcommand
cmd_opts = case cmd
  when "status"
    Trollop::options do
      opt :verbose, "Verbose output?", :required => false
      next
    end
  when "consumer_lag"
    Trollop::options do
      opt :graphite_host, "Graphite Host to send metrics to", :required => true, :type => String
      opt :environment, "The environment the script is running in", :required => true, :type => String
      opt :verbose, "Verbose output?", :required => false
    end
  else
    Trollop::die "Use status or consumer_lag"
  end


# Where kafka lives
kafka_dir = "/opt/kafka"
$kafka_bin = "#{kafka_dir}/bin/"
$kafka_config = "#{kafka_dir}/config/server.properties"

if __FILE__ == $0
  $verbose = cmd_opts[:verbose]
  if cmd.inspect =~ /status/
    kafka_status()
  elsif cmd.inspect =~ /consumer_lag/
    $graphite_host = cmd_opts[:graphite_host]
    $environment = cmd_opts[:environment]
    consumer_lag()
  end
end
