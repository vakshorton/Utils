import sys, os, pwd, signal, time, shutil
from subprocess import *
from resource_management import *

class DataPlaneClient(Script):
  def install(self, env):
    self.configure(env)
    import params
    #debug info
    Execute('echo list of config dump: ' + str(', '.join(params.list_of_configs)))    
    Execute('echo master config dump: ' + str(', '.join(params.master_configs)))
    Execute('echo host level config dump: ' + str(', '.join(params.list_of_host_level_params)))
    Execute('echo java home: ' + params.jdk64_home)
    Execute('echo ambari host: ' + params.ambari_server_host) 
    Execute('echo cluster name: ' + params.cluster_name) 
    Execute('echo namenode host: ' + params.namenode_host)
    Execute('echo namenode port: ' + params.namenode_port)
    Execute('echo hive metastore uri: ' + params.hive_metastore_uri)
    Execute('echo hbase zookeeper: ' + params.hbase_zookeeper)
    Execute('echo atlas host: ' + params.atlas_host)
    Execute('echo kafka host: ' + params.kafka_broker_host)
    Execute('echo kafka port: ' + params.kafka_port)
    Execute('echo stack_version: ' + params.stack_version_unformatted)
    
    Execute('echo data plane ambari host: ' + params.data_plane_ambari_host)
    Execute('echo data plane cluster name: ' + params.data_plane_cluster_name)
    Execute('echo data plane namenode host: ' + params.data_plane_namenode_host)
    Execute('echo data plane zookeeper host: ' + params.data_plane_zookeeper_host)
    Execute('echo data plane meta store host: ' + params.data_plane_meta_store_host)
    Execute('echo data plane hive server host: ' + params.data_plane_hive_server_host)
    Execute('echo data plane atlas host: ' + params.data_plane_atlas_host)
    Execute('echo data plane kafka host: ' + params.data_plane_kafka_host)
    Execute('echo Download Data Plane Util Bits')
    if not os.path.exists(params.install_dir):  
        os.makedirs(params.install_dir)
    os.chdir(params.install_dir)
    Execute('git clone ' + params.download_url)
    
    Execute('echo Install and configure Ranger Hive Plugin')
    Execute('echo Modify configuration files')
    src_dir = params.install_dir+'/Util/DATA_PLANE_CLIENT/package/configuration')
    
    Execute('sed -r -i "s;\{\{ZK_HOST\}\};'+params.data_plane_zookeeper_host+';"'+ src_dir+'/RANGER_HIVE_PLUGIN/package/configuration/ranger-hive-audit.xml')
    Execute('sed -r -i "s;\{\{NAMENODE_HOST\}\};'+params.data_plane_namenode_host+';"'+ src_dir+'/RANGER_HIVE_PLUGIN/package/configuration/ranger-hive-audit.xml')
    Execute('sed -r -i "s;\{\{RANGER_URL\}\};http://'+params.data_plane_ranger_host+':'+params.ranger_port+';"'+ src_dir+'/RANGER_HIVE_PLUGIN/package/configuration/ranger-hive-security.xml')
    Execute('sed -r -i "s;\{\{REPO_NAME\}\};'+params.data_plane_ranger_repo+';"'+ src_dir+'/RANGER_HIVE_PLUGIN/package/configuration/ranger-hive-security.xml')

    Execute('sed -r -i "s;\{\{ZK_HOST\}\};'+params.data_plane_zookeeper_host+';"'+ src_dir+'/hive-ranger-config/ranger-hive-audit')
    Execute('sed -r -i "s;\{\{NAMENODE_HOST\}\};'+params.data_plane_namenode_host+';"'+ src_dir+'/hive-ranger-config/ranger-hive-audit')
    Execute('sed -r -i "s;\{\{RANGER_URL\}\};http://'+params.data_plane_ranger_host+':'+params.ranger_port+';"'+ src_dir+'/hive-ranger-config/ranger-hive-security')
    Execute('sed -r -i "s;\{\{REPO_NAME\}\};'+params.data_plane_ranger_repo+';"'+ src_dir+'/hive-ranger-config/ranger-hive-security')
    
    Execute('echo Copying configuration files to Hive Server conf directory')
    dest_dir = ('/usr/hdp/current/hive-server2/conf/conf.server')
    src_files = os.listdir(src_dir)
    for file_name in src_files:
        full_file_name = os.path.join(src_dir, file_name)
        if (os.path.isfile(full_file_name)):
            shutil.copy(full_file_name, dest_dir)
     
    Execute('echo Setting Hive Plugin configuration')
    Execute('/var/lib/ambari-server/resources/scripts/configs.sh set '+params.data_plane_ambari_host+' '+params.data_plane_cluster_name+' hive-site hive.security.authorization.enabled true')

    Execute('/var/lib/ambari-server/resources/scripts/configs.sh set '+params.data_plane_ambari_host+' '+parmas.data_plane_cluster_name+' hive-site hive.conf.restricted.list hive.security.authorization.enabled,hive.security.authorization.manager,hive.security.authenticator.manager')

    Execute('/var/lib/ambari-server/resources/scripts/configs.sh set '+params.data_plane_ambari_host+' '+params.data_plane_cluster_name+' hiveserver2-site hive.security.authorization.enabled true')

    Execute('/var/lib/ambari-server/resources/scripts/configs.sh set '+params.data_plane_ambari_host+' '+params.data_plane_cluster_name+' hiveserver2-site hive.security.authorization.manager org.apache.ranger.authorization.hive.authorizer.RangerHiveAuthorizerFactory')

    Execute('/var/lib/ambari-server/resources/scripts/configs.sh set '+params.data_plane_ambari_host+' '+params.data_plane_cluster_name+' hiveserver2-site hive.security.authenticator.manager org.apache.hadoop.hive.ql.security.SessionStateUserAuthenticator')
    
    Execute('/var/lib/ambari-server/resources/scripts/configs.sh set '+params.data_plane_ambari_host+' '+params.data_plane_cluster_name+' ranger-hive-audit '+ src_dir+'/hive-ranger-config/ranger-hive-audit')

    Execute('/var/lib/ambari-server/resources/scripts/configs.sh set '+params.data_plane_ambari_host+' '+params.data_plane_cluster_name+' ranger-hive-plugin-properties '+ src_dir+'/hive-ranger-config/ranger-hive-plugin-properties')

    Execute('/var/lib/ambari-server/resources/scripts/configs.sh set '+params.data_plane_ambari_host+' '+params.data_plane_cluster_name+' ranger-hive-policymgr-ssl '+ src_dir+'/hive-ranger-config/ranger-hive-policymgr-ssl')

    Execute('/var/lib/ambari-server/resources/scripts/configs.sh set '+params.data_plane_ambari_host+' '+params.data_plane_cluster_name+' ranger-hive-security '+ src_dir+'/hive-ranger-config/ranger-hive-security')

Execute('echo Setting Hive Atlas Client Configuration...')
Execute('/var/lib/ambari-server/resources/scripts/configs.sh set '+params.data_plane_ambari_host+' '+params.data_plane_cluster_name+' hive-site "atlas.rest.address" "'+params.data_plane_atlas_host+':'+params.atlas_port+'"')

Execute('/var/lib/ambari-server/resources/scripts/configs.sh set '+params.data_plane_ambari_host+' '+params.data_plane_cluster_name+' hive-atlas-application.properties "atlas.kafka.bootstrap.servers" "'+params.data_plane_kafka_host+':'+params.kafka_port+'"')

Execute('/var/lib/ambari-server/resources/scripts/configs.sh set '+params.data_plane_ambari_host+' '+params.data_plane_cluster_name+' hive-atlas-application.properties "atlas.kafka.zookeeper.connect" "'+params.data_plane_zookeeper_host+':'+params.zookeeper_port+'"')

Execute('echo Setting Storm Atlas Client Configuration...')
Execute('/var/lib/ambari-server/resources/scripts/configs.sh set '+params.data_plane_ambari_host+' '+params.data_plane_cluster_name+' storm-atlas-application.properties "atlas.rest.address" "'+params.data_plane_atlas_host+':'+params.atlas_port+'"')

Execute('/var/lib/ambari-server/resources/scripts/configs.sh set '+params.data_plane_ambari_host+' '+params.data_plane_cluster_name+' storm-atlas-application.properties "atlas.kafka.zookeeper.connect" "'+params.data_plane_zookeeper_host+':'+params.zookeeper_port+'"')

Execute('/var/lib/ambari-server/resources/scripts/configs.sh set '+params.data_plane_ambari_host+' '+params.data_plane_cluster_name+' storm-atlas-application.properties "atlas.kafka.bootstrap.servers" "'+params.data_plane_kafka_host+':'+params.kafka_port+'"')

Execute('echo Setting Sqoop Atlas Client Configuration...')
Execute('/var/lib/ambari-server/resources/scripts/configs.sh set '+params.data_plane_ambari_host+' '+params.data_plane_cluster_name+' sqoop-atlas-application.properties "atlas.rest.address" "'+params.data_plane_atlas_host+':'+params.atlas_port+'"')

Execute('/var/lib/ambari-server/resources/scripts/configs.sh set '+params.data_plane_ambari_host+' '+params.data_plane_cluster_name+' sqoop-atlas-application.properties "atlas.kafka.zookeeper.connect" "'+params.data_plane_zookeeper_host+':'+params.zookeeper_port+'"')

Execute('/var/lib/ambari-server/resources/scripts/configs.sh set '+params.data_plane_ambari_host+' '+params.data_plane_cluster_name+' sqoop-atlas-application.properties "atlas.kafka.bootstrap.servers" "'+params.data_plane_kafka_host+':'+params.kafka_port+'"')

Execute('echo Setting Hive Meta Store Configuration...')
Execute('/var/lib/ambari-server/resources/scripts/configs.sh set '+params.data_plane_ambari_host+' '+params.data_plane_cluster_name+' hive-site "javax.jdo.option.ConnectionURL" "jdbc:mysql://'+data_plane_hiveserver_host+'/hive?createDatabaseIfNotExist=true"')

Execute('/var/lib/ambari-server/resources/scripts/configs.sh set '+params.data_plane_ambari_host+' '+params.data_plane_cluster_name+' hive-site "javax.jdo.option.ConnectionPassword" "hive"')

Execute('/var/lib/ambari-server/resources/scripts/configs.sh set '+params.data_plane_ambari_host+' '+params.data_plane_cluster_name+' hive-site "hive.metastore.uris" "'+params.data_plane_hive_metastore_uri+'"')

echo Execute('echo Restarting Services to refresh configurations...')

requests.put('http://'+params.ambari_server_host+':8080/api/v1/clusters/'+params.cluster_name+'/services/HIVE', auth=('admin', 'admin'),headers={'X-Requested-By':'ambari'},data=('{"RequestInfo": {"context": "Stop Hive"}, "ServiceInfo": {"state": "INSTALLED"}}'))

requests.put('http://'+params.ambari_server_host+':8080/api/v1/clusters/'+params.cluster_name+'/services/STORM', auth=('admin', 'admin'),headers={'X-Requested-By':'ambari'},data=('{"RequestInfo": {"context": "Stop Storm"}, "ServiceInfo": {"state": "INSTALLED"}}'))

requests.put('http://'+params.ambari_server_host+':8080/api/v1/clusters/'+params.cluster_name+'/services/SQOOP', auth=('admin', 'admin'),headers={'X-Requested-By':'ambari'},data=('{"RequestInfo": {"context": "Stop Sqoop"}, "ServiceInfo": {"state": "INSTALLED"}}'))

requests.put('http://'+params.ambari_server_host+':8080/api/v1/clusters/'+params.cluster_name+'/services/HIVE', auth=('admin', 'admin'),headers={'X-Requested-By':'ambari'},data=('{"RequestInfo": {"context": "Start Hive"}, "ServiceInfo": {"state": "STARTED"}}'))

requests.put('http://'+params.ambari_server_host+':8080/api/v1/clusters/'+params.cluster_name+'/services/STORM', auth=('admin', 'admin'),headers={'X-Requested-By':'ambari'},data=('{"RequestInfo": {"context": "Start Storm"}, "ServiceInfo": {"state": "STARTED"}}'))

requests.put('http://'+params.ambari_server_host+':8080/api/v1/clusters/'+params.cluster_name+'/services/SQOOP', auth=('admin', 'admin'),headers={'X-Requested-By':'ambari'},data=('{"RequestInfo": {"context": "Start Sqoop"}, "ServiceInfo": {"state": "STARTED"}}'))

  def status(self, env):
    raise ClientComponentHasNoStatus()

  def configure(self, env):
    import params
    env.set_params(params)
    
  def synchToDataPlane(self, env):
    import params
    env.set_params(params)
    Execute('redeployApplication.sh')
    
if __name__ == "__main__":
  DataPlaneClient().execute()
