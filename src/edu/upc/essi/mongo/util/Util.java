package edu.upc.essi.mongo.util;

import java.io.File;
import java.lang.management.ManagementFactory;
public class Util {
	public static String getTableNameFromPath(String filePath) {
		if(filePath.indexOf("\\")>0) {
			filePath = filePath.substring(filePath.lastIndexOf("\\")+1, filePath.length());
		}else if(filePath.indexOf("/")>0) {
			filePath = filePath.substring(filePath.lastIndexOf("/")+1, filePath.length());			
		}
		
		String[] values=filePath.split("\\.");
		String tableName = "";
		for (int i = 0; i < values.length; i++) {
			if(values[i].trim().length()>3) {
				if(values[i].contains("\\")) {
					return tableName=values[i].substring(values[i].lastIndexOf("\\"), values[i].length());
				}else if(values[i].contains("/")) {
					return tableName=values[i].substring(values[i].lastIndexOf("/"), values[i].length());					
				}else {
					return tableName = values[i].trim();
				}
			}
		}
		return tableName;
	}
	public static String getConnectorPerOS() 
	{
		EnumOS os= getOS();
		switch (os) {
		case LINUX:
			return  "/";
		case WINDOWS:
			return "\\";
		default:
			return "";
		}
	}
	public static EnumOS getOS() 
	{
		String os= System.getProperty("os.name");
		 
		if(os.contains("Linux")) {
			return EnumOS.LINUX;
		}else if(os.contains("Win")) {
			return EnumOS.WINDOWS;
		}else {
			return EnumOS.NOT_DEFINED;
		}
	} 
	
	public static  long getCurrentlyUsedMemory() {
		 
		long memoryBytes =
				    ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed() +
				    ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage().getUsed();
		return memoryBytes/(1024l*1024l);
	}
	
	public static long getTotalRuntimeMemory() {
		long memoryBytes = Runtime.getRuntime().totalMemory();
		return memoryBytes/(1024l*1024l);
	}
	
	public static long getFreeRuntimeMemory() {
		long memoryBytes = Runtime.getRuntime().freeMemory();
		return memoryBytes/(1024l*1024l);
	}
	
	public static double getFileSizeMBFromDirectory(String directoryPath,String objectName) {
		File dir=new File(directoryPath);
		File[] directoryListing = dir.listFiles();
		
		if (directoryListing != null) {
			 for (File child : directoryListing) { 
				 if(child.getName().contains(objectName)) {
					 double fileSizeMB = Math.round(child.length()/(1024d*1024d)*100d)/100d;
					 return fileSizeMB;
				 }
			 }
		}
		return 0d;
	}
}
