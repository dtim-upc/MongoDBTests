package edu.upc.essi.mongo.manager;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Properties;

import de.bytefish.pgbulkinsert.PgBulkInsert;
import edu.upc.essi.mongo.util.EnumDatabase;
import edu.upc.essi.mongo.util.EnumOperation;
import edu.upc.essi.mongo.util.EnumTestType;
import edu.upc.essi.mongo.util.MetricsManager;
import edu.upc.essi.mongo.util.Util;
import edu.upc.essi.mongo.util.reader.PlainFileReader;
import edu.upc.essi.mongo.util.writer.PlainFileWriter;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URLClassLoader;

public class PostgreSQLManager {
	private String base_url, full_url, server,port,database,user, password,driverClass,databaseSuffix,indexStamentFilePath;
	String createDBPath ="";
	boolean createDB = false;
	
	public PostgreSQLManager(String databaseSuffix,String indexStamentFilePath, boolean createDB, String createDBPath) {
		this.databaseSuffix = databaseSuffix;
		this.indexStamentFilePath = indexStamentFilePath; 
		loadProperties();
		this.createDB = createDB;
		this.createDBPath = createDBPath;
	} 
	
	public void insertBulk(String directoryPath,String destinationMetricsFile,EnumTestType testType) {
		
		if(createDB) {
			createDatabase(createDBPath);
		}
		
		
		File dir=new File(directoryPath);
		File[] directoryListing = dir.listFiles();
		boolean createIndexAfterInsert=false;
		switch (testType) {
			case NOPK_COD_JSON:
			case NOPK_NOCOD_JSON:
			case NOPK_NOCOD_RNF:
			case NOPK_COD_JSONB:
			case NOPK_NOCOD_JSONB:
				createIndexAfterInsert = true;
				break;
	
			default:
				break;
		}
		HashMap<String, String> statements = null;
		if(createIndexAfterInsert) {
			statements = getStatements(indexStamentFilePath);
		}
		
		MetricsManager oMetric = new MetricsManager(destinationMetricsFile,EnumDatabase.POSTGRES,EnumOperation.INSERT,testType);
		
		if (directoryListing != null) {
			 for (File child : directoryListing) { 
				long numberRecords = 0; 
				String fileName = child.getAbsolutePath();
				String object = Util.getTableNameFromPath(child.getName());
				oMetric.startRecord( object);
				numberRecords = insertBulkFile(fileName,oMetric); 
				if(createIndexAfterInsert) {
					System.out.println("Creating index...\n" + statements.get(object));
					executeNoQuery(statements.get(object));
					System.out.println("Index created");
				}
				oMetric.endRecord(numberRecords); 
				
				double size = getSizeMB(object);
				oMetric.setSize(size);
				
				oMetric.recordMetrics();
				oMetric.flushToDisk();
			 }
		}
		 
	}
	
	private long insertBulkFile(String fileName,MetricsManager oMetric) {
		long numberRecords = 0;
		try { 
			PlainFileReader oReader = new PlainFileReader();

			oReader.openConnection(fileName);
			String line =oReader.read(); 
			Class.forName(driverClass);
			Connection conn = DriverManager.getConnection(full_url, user, password);
			conn.setAutoCommit(false);
			Statement  statement = conn.createStatement();
			
			
			while(line!=null) {   
				statement.addBatch(line);
				numberRecords++;
				oMetric.measureMemory();
				if(numberRecords%10000==0) {
					statement.executeBatch();
					conn.commit();
				}
				
				line =oReader.read();
			}
			if(numberRecords%1000!=0) {
				statement.executeBatch();
			}
			statement.close();
			oReader.closeConnection();					
			
		} catch (SQLException e) {
			System.out.println(e.getNextException());
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return numberRecords;
	}
	public String executeSingleQuery(String query, int numberOfColumns) {
		String result = "";
		try {
			Class.forName(driverClass);
			Connection conn = DriverManager.getConnection(full_url, user, password);
			Statement  statement = conn.createStatement();
					
			ResultSet rs = statement.executeQuery(query);
			
			while(rs.next()) {
				for (int i = 1; i <= numberOfColumns; i++) {
					result+=rs.getString(i);
					if(i!=numberOfColumns) {
						result+=",";						
					}
				}
				result += ";";
			} 

			rs.close();
			rs = null;
			conn.close();		
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
		
		
		return result;
	}
	public void executeNoQuery(String statmt) {
		String result = "";
		try {
			Class.forName(driverClass);
			Connection conn = DriverManager.getConnection(full_url, user, password);
			Statement  statement = conn.createStatement();
					
			boolean rs = statement.execute(statmt);
			System.out.println("Executed: " + statmt + rs);
			conn.close();		
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
		
	}

	public void executeFullScanQuery(String sourceDirectoryPath,String destinationMetricsPath,EnumTestType testType) {
		File dir=new File(sourceDirectoryPath);
		File[] directoryListing = dir.listFiles();
		
		MetricsManager oMetric = new MetricsManager(destinationMetricsPath,EnumDatabase.POSTGRES,EnumOperation.SELECTALL,testType);
		
		if (directoryListing != null) {
			 for (File child : directoryListing) { 
				long numberRecords = 0; 
				String fileName = child.getAbsolutePath();
				String tableName = Util.getTableNameFromPath(child.getName());
				oMetric.startRecord( tableName);
				numberRecords = executeFullScanQueryPerTable(tableName,oMetric); 
				oMetric.endRecord(numberRecords);
				
				double size = getSizeMB(tableName);
				oMetric.setSize(size);
				
				oMetric.recordMetrics();
				oMetric.flushToDisk();
			 }
		}
	}
	
	private long executeFullScanQueryPerTable(String tableName,MetricsManager oMetric) {

		String queryColumns = "SELECT column_name FROM information_schema.columns WHERE table_name = '#Table'";
		queryColumns=queryColumns.replace("#Table", tableName);
		
		String columnResult = executeSingleQuery(queryColumns, 1);
		String query = "SELECT * FROM #Table";
		query = query.replace("#Table", tableName);
		
		int numberOfColumns = columnResult.split(";").length;
		long numberRecords = 0;
		String result = "";
		
		try {

			Class.forName(driverClass);
			Connection conn = DriverManager.getConnection(full_url, user, password);
			Statement  statement = conn.createStatement();
					
			ResultSet rs = statement.executeQuery(query);
			
			while(rs.next()) {
				numberRecords++;
				for (int i = 1; i <= numberOfColumns; i++) {
					result+=rs.getString(i);
					oMetric.measureMemory();
				}
				result = "";
			} 
			rs.close();
			rs = null;
			conn.close();	 
			
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
		return numberRecords;
	}

	private long executePKQueryPerTable(String tableName, String value, MetricsManager oMetric) {

		String queryColumns = "SELECT column_name FROM information_schema.columns WHERE table_name = '#Table'";
		queryColumns=queryColumns.replace("#Table", tableName);
		
		String columnResult = executeSingleQuery(queryColumns, 1);
		String query = "SELECT * FROM #Table";
		query = query.replace("#Table", tableName);
		
		int numberOfColumns = columnResult.split(";").length;
		long numberRecords = 0;
		String result = "";
		
		try {

			Class.forName(driverClass);
			Connection conn = DriverManager.getConnection(full_url, user, password);
			Statement  statement = conn.createStatement();
					
			ResultSet rs = statement.executeQuery(query);
			
			while(rs.next()) {
				numberRecords++;
				for (int i = 1; i <= numberOfColumns; i++) {
					result+=rs.getString(i);
					oMetric.measureMemory();
				}
				result = "";
			} 
			rs.close();
			rs = null;
			conn.close();	 
			
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
		return numberRecords;
	}
	

	
	private void loadProperties() {
		try {
	        
			InputStream inputStream = getClass().getResourceAsStream("/db.postgres.properties");
			Properties props = new Properties();
			props.load(inputStream);

			this.base_url = props.getProperty("url");
			this.port = props.getProperty("port");
			this.database = props.getProperty("database");
			this.server = props.getProperty("server");
			
			full_url= base_url + server + ":" + port + "/" + database+databaseSuffix;
			
			this.user = props.getProperty("user");
			this.password = props.getProperty("password");
			this.driverClass = props.getProperty("driverClass");
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}  
	
	public double getSizeMB(String tableName) {
		String selectSize ="SELECT pg_total_relation_size(relid) FROM pg_catalog.pg_statio_user_tables  WHERE relname ='#TableName'";
		selectSize=selectSize.replace("#TableName", tableName);
		String sizeResult = executeSingleQuery(selectSize, 1); //Result in B
		String[] results =sizeResult.split(";");
		Double size = Double.parseDouble(results[0])/1024d/1024d;
		
		return size;
	}
	
	private HashMap<String, String> getStatements(String filePath) {
		PlainFileReader oReader = new PlainFileReader();
		HashMap<String, String> statements= new HashMap<String,String>();
		try {
			oReader.openConnection(filePath);
			String line =oReader.read(); 
						
			while(line!=null) {   
				if(!line.trim().equals("")) {
					String table = "TABLE";
					String tableName = line.substring(line.indexOf(table)+table.length()+1,line.length());
					tableName=tableName.substring(0,tableName.indexOf(" ")).trim();
					if(!statements.containsKey(tableName)) {
						statements.put(tableName, line);
					}
				}
				line =oReader.read();
			}
			oReader.closeConnection();		
		}catch(Exception ex) {
			ex.printStackTrace();
		}
		return statements;
	}
	
	private void createDatabase(String fileTablePath){
		String createStm = "CREATE DATABASE " + database+databaseSuffix;
		try {
			//dropDatabase();
			
			Class.forName(driverClass);
			String url = base_url + server + ":" + port ;
			Connection conn = DriverManager.getConnection(url, user, password);
			Statement  statement = conn.createStatement();
					
			boolean rs = statement.execute(createStm);
			
			conn.close();	
			
			//Execute File with creation statements
			String query = "";
			PlainFileReader oReader = new PlainFileReader();
			oReader.openConnection(fileTablePath);
			String line = oReader.read();
			while(line!=null) {
				if(line.length()>0) {
					query+=line;
				}
				line = oReader.read();
			}
			String[] listOfQueries = query.split(";");
			for (int i = 0; i < listOfQueries.length; i++) {
				executeNoQuery(listOfQueries[i]);
			}

		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}
	public void dropDatabase() {
		String removeConnectionsStm = "SELECT pg_terminate_backend(pg_stat_activity.pid)" + 
				"FROM pg_stat_activity " + 
				"WHERE pg_stat_activity.datname = '" +  database+databaseSuffix + "'";
		
		String dropStm = "DROP DATABASE " + database+databaseSuffix;
		try {
			Class.forName(driverClass);
			String url = base_url + server + ":" + port ;
			Connection conn = DriverManager.getConnection(url, user, password);
			Statement  statement = conn.createStatement();
			boolean rsRm = statement.execute(removeConnectionsStm);
			Thread.sleep(3000);
			boolean rs = statement.execute(dropStm);
			
			conn.close();	
			
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}
}
