package edu.upc.essi.mongo.manager;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.util.JSON;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext; 
import edu.upc.essi.mongo.util.EnumDatabase;
import edu.upc.essi.mongo.util.EnumOperation;
import edu.upc.essi.mongo.util.EnumTestType;
import edu.upc.essi.mongo.util.MetricsManager;
import edu.upc.essi.mongo.util.Util;
import edu.upc.fib.benchmarkDB.util.reader.PlainFileReader;

public class MongoDBManager  {
	private String url, server,database,user, password,driverClass,databaseSuffix;
	int port=0;
	
	public MongoDBManager(String databaseSuffix) {
		this.databaseSuffix = databaseSuffix;
		loadProperties();
	}
	
	public void insertBulk(String directoryPath,String metricsPath,EnumTestType testType) {
		File dir=new File(directoryPath);
		File[] directoryListing = dir.listFiles();
		
		MetricsManager oMetric = new MetricsManager(metricsPath,EnumDatabase.MONGO,
				EnumOperation.INSERT,testType);
	
		if (directoryListing != null) {
			 for (File child : directoryListing) {
				long numberRecords = 0; 
				String objectName = Util.getTableNameFromPath(child.getName()) ;
				
				oMetric.startRecord( objectName);
				
				numberRecords = insertBulkFile(child.getAbsolutePath(),oMetric);
				System.out.println("Number of inserted Records: " + numberRecords);
				
				oMetric.makeDelay();
				double size = getSizeMB(objectName);
				oMetric.endRecord(numberRecords);
				oMetric.setSize(size);
				
				oMetric.recordMetrics();
				oMetric.flushToDisk();
			 }
		}
	}
	private long insertBulkFile(String filePath,MetricsManager oMetric) {
		LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
		Logger rootLogger = loggerContext.getLogger(driverClass);
		rootLogger.setLevel(Level.OFF);

		List<DBObject> documentsArray = new ArrayList<DBObject>();
		long numberOfRecords =0;
		File oFile = new File(filePath);
		String collectionName = Util.getTableNameFromPath(oFile.getName());//getCollection(filePath);
				
		Mongo mongo2 = new Mongo(server, port);
		DB db = mongo2.getDB(database);
		DBCollection collection = db.getCollection(collectionName);  
		
		PlainFileReader oReader = new PlainFileReader();
		DBObject dbObject = null;
		
		try {
			oReader.openConnection(filePath);
			String line =oReader.read(); 
			while(line!=null) {    
				numberOfRecords++;
			    dbObject = (DBObject)JSON.parse(line);
			    documentsArray.add(dbObject);
			    oMetric.measureMemory();
				if (numberOfRecords % 10000 == 0) { 
					if (!documentsArray.isEmpty()) {
						collection.insert(documentsArray);
						documentsArray.clear();
					}
				}

				line =oReader.read();
			}
			
			if (!documentsArray.isEmpty()) {
				collection.insert(documentsArray); 
				documentsArray.clear();
			}
		} catch (IOException e) { 
			e.printStackTrace();
		}
		

		mongo2.close();
		
		return numberOfRecords;
	}
	/*
	private String getCollection(String filePath) {
		String table = filePath.substring(filePath.lastIndexOf("\\"), filePath.indexOf(".dat"));
		if(table.indexOf(" ")>0) {
			table=table.substring(table.indexOf(" ")+1);
		}
		if(table.indexOf(".")>0) {
			table=table.substring(table.indexOf(".")+1);			
		}
		if(table.indexOf("\\")>0) {
			table=table.substring(table.indexOf("\\")+1);			
		}
		return table;
	}
	*/
	private void loadProperties() {
		try {

			InputStream inputStream = getClass().getResourceAsStream("/db.mongodb.properties");
			Properties props = new Properties();
			props.load(inputStream);

			this.url = props.getProperty("url");
			this.port = Integer.parseInt(props.getProperty("port"));
			this.database = props.getProperty("database")+ databaseSuffix;
			this.server = props.getProperty("server");
			
			this.user = props.getProperty("user");
			this.password = props.getProperty("password");
			this.driverClass = props.getProperty("driverClass");
			
			if(this.password.contains("%")) {
				this.password.replaceAll("%", "%25");
			}else if(password.contains("@")) {
				this.password.replaceAll("@", "%40");				
			}else if(password.contains(":")) {
				this.password.replaceAll(":", "%3A");				
			}else if(password.contains("/")) {
				this.password.replaceAll("/", "%2F");				
			}
			url=url + user + ":" + password+ "@" + server + ":" + port + "/" + database;
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void executeFullScanQuery(String sourceDirectoryPath,String destinationMetricsPath,EnumTestType testType) {
		File dir=new File(sourceDirectoryPath);
		File[] directoryListing = dir.listFiles();
		
		MetricsManager oMetric = new MetricsManager(destinationMetricsPath,EnumDatabase.MONGO,EnumOperation.SELECTALL,testType);
		
		if (directoryListing != null) {
			 for (File child : directoryListing) { 
				long numberRecords = 0; 
				String fileName = child.getAbsolutePath();
				String collectionName = Util.getTableNameFromPath(child.getName());
				oMetric.startRecord( collectionName);
				numberRecords = executeFullScanQueryPerCollection(collectionName,oMetric); 
				
				oMetric.endRecord(numberRecords);
				
				oMetric.makeDelay();				
				double size = getSizeMB(collectionName);
				oMetric.setSize(size);
								
				oMetric.recordMetrics();
				oMetric.flushToDisk();
			 }
		}
	}

	private long executeFullScanQueryPerCollection(String collectionName,MetricsManager oMetric) {
		String query = "db.#CollectionName.find( {} )";
		query.replace("#CollectionName", collectionName);
		 
		
		LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
		Logger rootLogger = loggerContext.getLogger(driverClass);
		rootLogger.setLevel(Level.OFF);
		 
		long numberOfDocuments = 0;
		Mongo mongo2 = new Mongo(server, port);
		DB db = mongo2.getDB(database);
		DBCollection collection = db.getCollection(collectionName);  
		
		PlainFileReader oReader = new PlainFileReader();
		DBObject dbObject = null;
		
		try {
			DBCursor oCollection =collection.find();
			while(oCollection.hasNext()) {
                DBObject o = oCollection.next();
                oMetric.measureMemory();
    			numberOfDocuments++;
            }
			
		} catch (Exception e) { 
			e.printStackTrace();
		}
		
		mongo2.close();
		return numberOfDocuments;
	}
 
	public double getSizeMB(String collectionName) {
		//String query = "db.#CollectionName.stats(1024)";
		//query.replace("#CollectionName", collectionName);
		double size=0;
		
		LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
		Logger rootLogger = loggerContext.getLogger(driverClass);
		rootLogger.setLevel(Level.OFF);
		 
		long numberOfDocuments = 0;
		Mongo mongo2 = new Mongo(server, port);
		DB db = mongo2.getDB(database);
		DBCollection collection = db.getCollection(collectionName);  
		
		PlainFileReader oReader = new PlainFileReader();
		DBObject dbObject = null;
		
		try {
			CommandResult oCollection =collection.getStats();
			if(oCollection.containsKey("size")) { 
                System.out.println("Size: "  + collectionName + " " + oCollection.getDouble("size"));
                size = oCollection.getDouble("size")/1024d/1024d;
               
            }
			
		} catch (Exception e) { 
			e.printStackTrace();
		}
		
		mongo2.close();
		return size ;
	}
	
}
