
#!/usr/bin/env python
from resource_management import *

# server configurations
config = Script.get_config()

install_dir = config['configurations']['data-plane-config']['install_dir']
download_url = config['configurations']['data-plane-config']['download_url']
data_plane_ambari_host = config['configurations']['data-plane-config']['data_plane_ambari_host']

master_configs = config['clusterHostInfo']
ambari_host = str(master_configs['ambari_server_host'][0])

stack_version_unformatted = config['hostLevelParams']['stack_version']

master_configs = config['clusterHostInfo']
ambari_server_host = str(master_configs['ambari_server_host'][0])
namenode_host =  str(master_configs['namenode_host'][0])
namenode_port = get_port_from_url(config['configurations']['core-site']['fs.defaultFS'])
hive_metastore_host = str(master_configs['hive_metastore_host'][0])
hive_metastore_port = get_port_from_url(config['configurations']['hive-site']['hive.metastore.uris'])
hive_metastore_uri = 'thrift://'+hive_metastore_host+':'+hive_metastore_port
hbase_zookeeper = config['configurations']['hbase-site']['hbase.zookeeper.quorum']
#atlas_server_host = str(master_configs['atlas_server_host'][0])
kafka_broker_host = str(master_configs['kafka_broker_hosts'][0])
if 'port' in config['configurations']['kafka-broker']:
  kafka_port = str(config['configurations']['kafka-broker']['port'])
else:
  kafka_port = get_port_from_url(config['configurations']['kafka-broker']['listeners'])

list_of_configs = config['configurations']
jdk64_home=config['hostLevelParams']['java_home']

def get_port_from_url(address):
  if not is_empty(address):
    return address.split(':')[-1]
  else:
    return address