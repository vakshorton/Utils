package com.hortonworks.util;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;

public class DataPlaneUtils {
	private static String hiveSitePath = "/usr/hdp/current/hive-client/conf/hive-site.xml";
	private static String hdfsSitePath = "/usr/hdp/current/hadoop-client/conf/hdfs-site.xml";
	private static String coreSitePath = "/usr/hdp/current/hadoop-client/conf/core-site.xml";
	private static String hdfsUserName = "hdfs";
	private static String hiveTableOwner = "hive";
	private static String hiveTableOwnerGroup = "hdfs";
	private static String hiveTablePermissions = "drwxrwxrwx";
	
	public static void main(String[] args) throws Exception {
		HiveConf hiveConf = new HiveConf(SessionState.class);
	    File hiveSiteFile = new File(hiveSitePath);
	    System.out.println("hive conf file: " + hiveSiteFile.toString());
	    HiveMetaStoreClient client = null;
	    try {
	    	hiveConf.addResource(hiveSiteFile.toURI().toURL());
	    	client = new HiveMetaStoreClient(hiveConf);
	    } catch (MetaException e) {
	        e.printStackTrace();
	    }
	    
	    String currentDB;
	    String currentTable;
	    Table hiveTable;
	    StorageDescriptor hiveTableStorage;
	    try {
	    	Iterator<String> dbIterator = client.getAllDatabases().iterator();
	    	while(dbIterator.hasNext()){	
	    		currentDB = dbIterator.next();
	    		System.out.println("*** Current DB: " + currentDB);
	    		Iterator<String> tableIterator = client.getAllTables(currentDB).iterator();
	    		while(tableIterator.hasNext()){
	    			currentTable = tableIterator.next();
	    			System.out.println("****** Current Table: " + currentTable);
	    			hiveTable = client.getTable(currentDB, currentTable);
	    			hiveTableStorage = hiveTable.getSd();
	    			System.out.println("****** Current Table Location: " + hiveTableStorage.getLocation());
	    			createHiveTableLocation(hiveTableStorage.getLocation());
	    		}
	    	}
	    } catch (MetaException e) {
	        e.printStackTrace();
	    } catch (TException e) {
	        e.printStackTrace();
	    }   
	}
	
	private static void createHiveTableLocation(final String tableLocation) throws IOException{
	    final Path tablePath = new Path(tableLocation);
		try {
            UserGroupInformation ugi= UserGroupInformation.createRemoteUser(hdfsUserName);
            ugi.doAs(new PrivilegedExceptionAction<Void>() {
                public Void run() throws Exception {
                	Configuration config = new Configuration();
            		config.addResource(new Path(coreSitePath));
            	    config.addResource(new Path(hdfsSitePath));
            	    config.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
            	    config.set("fs.file.impl", LocalFileSystem.class.getName());
            	    config.set("hadoop.job.ugi", hdfsUserName);
            	    FileSystem dfs = FileSystem.get(config);
            	    
            	    if(!dfs.exists(tablePath)){
            	    	dfs.mkdirs(tablePath);
            	    }
            	    dfs.setOwner(tablePath, hiveTableOwner, hiveTableOwnerGroup);
            	    dfs.setPermission(tablePath, FsPermission.valueOf(hiveTablePermissions));
            	    
            	    return null;
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
	    
	}
}
