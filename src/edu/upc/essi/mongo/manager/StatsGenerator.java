package edu.upc.essi.mongo.manager;


import java.io.File;
import java.util.HashMap;

import edu.upc.essi.mongo.util.Util;
import edu.upc.essi.mongo.util.writer.PlainFileWriter;

public class StatsGenerator {
	private String sourceFolderPath, destinationFolderPath;
	public StatsGenerator(String sourceFolderPath, String destinationFolderPath){
		this.sourceFolderPath = sourceFolderPath;
		this.destinationFolderPath = destinationFolderPath;
	}
	public void generate() {
		try {
			File dir=new File(sourceFolderPath);
			HashMap<String, String> hm = new HashMap<String, String>();
			File[] directoryListing = dir.listFiles();
			if (directoryListing != null) {
				 for (File child : directoryListing) { 
					String tableName=Util.getTableNameFromPath(child.getName());
					if(hm.get(tableName)==null) { 
						String destinationFilePath = destinationFolderPath +Util.getConnectorPerOS() + tableName + ".stats";
						generatePerTable(tableName,destinationFilePath);
						hm.put(tableName, tableName);
					}
				 }
			}
			
		
		} catch (Exception e) { 
			e.printStackTrace();
		}
	}
	private void generatePerTable(String tableName, String destinationFilePath) {
		try {

			String queryCountTotal = "SELECT COUNT(*) FROM #Table;";
 			String queryColumns = "SELECT column_name FROM information_schema.columns WHERE table_name = '#Table'";
			String queryCountColumn = "SELECT COUNT(*) FROM (SELECT DISTINCT #Column FROM #Table) T";
			String queryCountDistincts="SELECT DISTINCT #Column, COUNT(*) FROM #Table \r\n" + 
					"GROUP BY #Column\r\n" + 
					"ORDER BY COUNT(*)\r\n" + 
					"LIMIT 5;";
			
			PlainFileWriter oWriter = new PlainFileWriter();
			oWriter.create(destinationFilePath);

			
			PostgreSQLManager psqlManager = new PostgreSQLManager("","",false,"");
			queryCountTotal = queryCountTotal.replace("#Table", tableName);
			String totalPerTable = psqlManager.executeSingleQuery(queryCountTotal, 1);
			String row = "#" + tableName + "=" + totalPerTable.replace(";", "").replace(",", "") + "\n"; 
			
			queryColumns = queryColumns.replace("#Table", tableName);

			String totalColumns = psqlManager.executeSingleQuery(queryColumns, 1);
			
			if(totalColumns.trim().length()>0) {
				String[] columns = totalColumns.split(";");
				for (int i = 0; i < columns.length; i++) {
					String columnName = columns[i];
					String queryCount = queryCountColumn.replace("#Table", tableName);
					queryCount = queryCount.replace("#Column", columnName);
					
					String totalPerColumn = psqlManager.executeSingleQuery(queryCount, 1);
							
					String queryCol = queryCountDistincts.replace("#Table", tableName);
					queryCol = queryCol.replace("#Column", columnName);

					String maxValuesPerColumn = psqlManager.executeSingleQuery(queryCol, 2);
					
					String[] values = maxValuesPerColumn.split(";");
					row += columnName + "\t" + totalPerColumn.replaceAll(";", "") + "\t" ;
					for (int j = 0; j < values.length; j++) {
						String[] valueCounter = values[j].split(",");
						String value = valueCounter[0].trim();
						String counter = valueCounter[1].trim();
						row += value + "," + counter + ";";
					}
					row +="\n";
				}
			}
			oWriter.write(row);
			oWriter.flush();
			oWriter.close();
		} catch (Exception e) {
			e.printStackTrace();
		}		
	}
}
