package com.hortonworks.util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.typesystem.Referenceable;

public class AtlasBulkUpdate {
	private static AtlasClient atlasClient;
		
	public static void main(String[] args) throws Exception {
		if(args.length >= 6){
			String atlasUrl = args[0];
			String user = args[1]; 
			String password = args[2];
			String clusterName = args[3];
			String csvFile = args[4];//"/Users/vvaks/Documents/DD_Atlas.csv";
			String propsFile = args[5];
			String csvSplitBy = "\\|";//"/Users/vvaks/"
			
			Properties props = System.getProperties();
	        props.setProperty("atlas.conf", propsFile);
	        
	        String[] basicAuth = {user, password};
			String[] atlasURL = {atlasUrl};
	        atlasClient = new AtlasClient(atlasURL, basicAuth);
			System.out.println("***************** atlas.conf has been set to: " + props.getProperty("atlas.conf"));
			
			String line = "";
			String[] object = null;
			String objectName = null;
			String objectComment = null;
			String objectType = null;
			boolean attemptUpdate = true;
			Referenceable currentEntity;
			
			try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
				 while ((line = br.readLine()) != null) {
	            	object = line.split(csvSplitBy);
	            	if(object.length > 0){	
	            		objectName = object[0]+"@"+clusterName;
	            		System.out.println("object_name=" + objectName);
	            	}else{
	            		attemptUpdate = false;
	            		System.out.println("Object Name is missing");
	            	}
	            	if(object.length > 1){
	            		objectComment = object[1];
	            		System.out.println("object_comment=" + objectComment);
	            	}else{
	            		attemptUpdate = false;
	            		System.out.println("Object Comment is missing");
	            	}
	            	if(object.length > 2){
	            		objectType = object[2];
	            		System.out.println("object_type=" + objectType);
	            		if(objectType.equalsIgnoreCase("table")){
	            			objectType = "hive_table";
	            		}else if(objectType.equalsIgnoreCase("column")){
	            			objectType = "hive_column";
	            		}else{
	            			attemptUpdate = false;
	            			System.out.println("Object is not a table or a column");
	            		}
	            	}else{
	            		attemptUpdate = false;
	            		System.out.println("Object Type is missing");
	            	}
	            	if(attemptUpdate){
	            		currentEntity = atlasClient.getEntity(objectType, AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, objectName);
	            		System.out.println("Entity: "+currentEntity);
	            		atlasClient.updateEntityAttribute(currentEntity.getId()._getId(), "comment", objectComment);
	            	}
	            	attemptUpdate = true;
	            }

	        } catch (FileNotFoundException e) {
	            e.printStackTrace();
	        } catch (IOException e) {
	            e.printStackTrace();
	        } catch (AtlasServiceException e) {
				e.printStackTrace();
	        }
		}else{
			System.out.println("Not enough arguments provided, please make sure to provide the following arguments...");
			System.out.println("atlas url, atlas user, atlas password, target object source cluster name, source csv file, atlas config");
		}	
	}
}
