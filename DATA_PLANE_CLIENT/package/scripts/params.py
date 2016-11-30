
#!/usr/bin/env python
import requests, json
from resource_management import *

# server configurations
config = Script.get_config()

install_dir = config['configurations']['data-plane-config']['install_dir']
download_url = config['configurations']['data-plane-config']['download_url']
data_plane_ambari_host = config['configurations']['data-plane-config']['data_plane_ambari_host']
demo_install_dir = config['configurations']['control-config']['democontrol.install_dir']

master_configs = config['clusterHostInfo']
ambari_server_host = str(master_configs['ambari_server_host'][0])
ambari_server_port = '8080'

data_plane_cluster_name = str(json.loads(requests.get('http://'+data_plane_ambari_host+':'+ambari_server_port+'/api/v1/clusters', auth=('admin', 'admin')).content).get('items')[0].get('Clusters').get('cluster_name'))

cluster_name = config['configurations']['application-properties']['atlas.cluster.name']
stack_version_unformatted = config['hostLevelParams']['stack_version']

master_configs = config['clusterHostInfo']
namenode_host =  str(master_configs['namenode_host'][0])
namenode_port = get_port_from_url(config['configurations']['core-site']['fs.defaultFS'])
hive_server_host = str(master_configs['hive_server_host'][0])
hive_server_port = '10000'
hive_metastore_host = str(master_configs['hive_metastore_host'][0])
hive_metastore_port = get_port_from_url(config['configurations']['hive-site']['hive.metastore.uris'])
hive_metastore_uri = 'thrift://'+hive_metastore_host+':'+hive_metastore_port
hbase_zookeeper = config['configurations']['hbase-site']['hbase.zookeeper.quorum']
zookeeper_port = '2181'
atlas_host = str(master_configs['atlas_server_hosts'][0])
atlas_port = '21000'
kafka_broker_host = str(master_configs['kafka_broker_hosts'][0])
if 'port' in config['configurations']['kafka-broker']:
  kafka_port = str(config['configurations']['kafka-broker']['port'])
else:
  kafka_port = get_port_from_url(config['configurations']['kafka-broker']['listeners'])

nifi_host = str(master_configs['nifi_master_hosts'][0])
nifi_port = '9090'

ranger_port = '6080'

data_plane_hive_server_host = str(json.loads(requests.get('http://'+data_plane_ambari_host+':'+ambari_server_port+'/api/v1/clusters/'+data_plane_cluster_name+'/services/HIVE/components/HIVE_SERVER', auth=('admin', 'admin')).content).get('host_components')[0].get('HostRoles').get('host_name'))

data_plane_hive_metastore_host = str(json.loads(requests.get('http://'+data_plane_ambari_host+':'+ambari_server_port+'/api/v1/clusters/'+data_plane_cluster_name+'/services/HIVE/components/HIVE_METASTORE', auth=('admin', 'admin')).content).get('host_components')[0].get('HostRoles').get('host_name')) 

data_plane_hive_metastore_uri = 'thrift://'+data_plane_hive_metastore_host+':'+hive_metastore_port

data_plane_namenode_host = str(json.loads(requests.get('http://'+data_plane_ambari_host+':'+ambari_server_port+'/api/v1/clusters/'+data_plane_cluster_name+'/services/HDFS/components/NAMENODE', auth=('admin', 'admin')).content).get('host_components')[0].get('HostRoles').get('host_name')) 

data_plane_zookeeper_host = str(json.loads(requests.get('http://'+data_plane_ambari_host+':'+ambari_server_port+'/api/v1/clusters/'+data_plane_cluster_name+'/services/ZOOKEEPER/components/ZOOKEEPER_SERVER', auth=('admin', 'admin')).content).get('host_components')[0].get('HostRoles').get('host_name'))

data_plane_kafka_host = str(json.loads(requests.get('http://'+data_plane_ambari_host+':'+ambari_server_port+'/api/v1/clusters/'+data_plane_cluster_name+'/services/KAFKA/components/KAFKA_BROKER', auth=('admin', 'admin')).content).get('host_components')[0].get('HostRoles').get('host_name'))

data_plane_atlas_host = str(json.loads(requests.get('http://'+data_plane_ambari_host+':'+ambari_server_port+'/api/v1/clusters/'+data_plane_cluster_name+'/services/ATLAS/components/ATLAS_SERVER', auth=('admin', 'admin')).content).get('host_components')[0].get('HostRoles').get('host_name'))

data_plane_ranger_host = str(json.loads(requests.get('http://'+data_plane_ambari_host+':'+ambari_server_port+'/api/v1/clusters/'+data_plane_cluster_name+'/services/RANGER/components/RANGER_ADMIN', auth=('admin', 'admin')).content))

data_plane_ranger_hive_repo = 'data-plane_hive'

list_of_configs = config['configurations']
list_of_host_level_params = config['configurations']
jdk64_home=config['hostLevelParams']['java_home']

def get_port_from_url(address):
  if not is_empty(address):
    return address.split(':')[-1]
  else:
    return address
    