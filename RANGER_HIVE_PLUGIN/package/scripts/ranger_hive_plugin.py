import sys
from subprocess import *
from resource_management import *
 
class Plugin(Script):
  def install(self, env):
    print 'Install the config client';
    Execute('cp /var/lib/ambari-server/resources/stacks/HDP/2.5/services/RANGER_HIVE_PLUGIN/configuration/* /usr/hdp/current/hive-server2/conf/conf.server/')
  def configure(self, env):
    print 'Configure the config client';
  def status(self, env): 
  	raise ClientComponentHasNoStatus()
  
if __name__ == "__main__":
  Plugin().execute()
