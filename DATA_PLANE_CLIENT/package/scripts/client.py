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
    Execute('echo java home: ' + params.jdk64_home)
    Execute('echo ambari host: ' + params.ambari_server_host) 
    Execute('echo ambari port: ' + params.ambari_port)
    Execute('echo namenode host: ' + params.namenode_host)
    Execute('echo namenode port: ' + params.namenode_port)
    Execute('echo hive metastore uri: ' + params.hive_metastore_uri)
    Execute('echo hbase zookeeper: ' + params.hbase_zookeeper)
    Execute('echo kafka host: ' + params.kafka_broker_host)
    Execute('echo kafka port: ' + params.kafka_port)
    Execute('echo stack_version: ' + params.stack_version_unformatted)
    
    Execute('echo Download Data Plane Util Bits')
    if not os.path.exists(params.install_dir):  
        os.makedirs(params.install_dir)
    os.chdir(params.install_dir)
    Execute('git clone ' + params.download_url)
    os.chdir(params.install_dir+'/Util')
    
    Execute('echo Install the config client')
    src_dir = ('/var/lib/ambari-agent/cache/stacks/HDP/2.5/services/RANGER_HIVE_PLUGIN/package/configuration')
    dest_dir = ('/usr/hdp/current/hive-server2/conf/conf.server')
    src_files = os.listdir(src_dir)
    for file_name in src_files:
        full_file_name = os.path.join(src_dir, file_name)
        if (os.path.isfile(full_file_name)):
            #shutil.copy(full_file_name, dest_dir)

  def status(self, env):
    raise ClientComponentHasNoStatus()

  def configure(self, env):
    import params
    env.set_params(params)
    
if __name__ == "__main__":
  DataPlaneClient().execute()
