package edu.upc.essi.mongo.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Metric {
	private EnumTestType testType;
	private String os;
	private String operation;
	private String datastore;
	private String object;
	private String format;
	private boolean encoded;
	private boolean hadPK;
	private long numberOfRecords;
	private long time;
	private double sizeMB;
	private double rawSize;
	private double memoryAvg;
	private double memoryMax;
	private double recordsPerSecond;
	private double MBperSecond;
	private int numberOfTests;
	private Date startDate;
	private Date endDate;
	
	public EnumTestType getTestType() {
		return testType;
	}
	public void setTestType(EnumTestType testType) {
		this.testType = testType;
	}
	public String getOs() {
		return os;
	}
	public void setOs(String os) {
		this.os = os;
	}
	public String getOperation() {
		return operation;
	}
	public void setOperation(String operation) {
		this.operation = operation;
	}
	public String getDatastore() {
		return datastore;
	}
	public void setDatastore(String datastore) {
		this.datastore = datastore;
	}
	public String getObject() {
		return object;
	}
	public void setObject(String object) {
		this.object = object;
	}
	public String getFormat() {
		return format;
	}
	public void setFormat(String format) {
		this.format = format;
	}
	public boolean isEncoded() {
		return encoded;
	}
	public void setEncoded(boolean encoded) {
		this.encoded = encoded;
	}
	public boolean isHadPK() {
		return hadPK;
	}
	public void setHadPK(boolean hadPK) {
		this.hadPK = hadPK;
	}
	public long getNumberOfRecords() {
		return numberOfRecords;
	}
	public void setNumberOfRecords(long numberOfRecords) {
		this.numberOfRecords = numberOfRecords;
	}
	public long getTime() {
		return time;
	}
	public void setTime(long time) {
		this.time = time;
	}
	public double getSizeMB() {
		return sizeMB;
	}
	public void setSizeMB(double sizeMB) {
		this.sizeMB = sizeMB;
	}
	public double getRawSize() {
		return rawSize;
	}
	public void setRawSize(double rawSize) {
		this.rawSize = rawSize;
	}
	public void setRecordsPerSecond(double recordsPerSecond) {
		this.recordsPerSecond = recordsPerSecond;
	}
	public void setKBperSecond(double MBperSecond) {
		MBperSecond = MBperSecond;
	}
	public double getMemoryAvg() {
		return memoryAvg;
	}
	public void setMemoryAvg(double memoryAvg) {
		this.memoryAvg = memoryAvg;
	}
	public double getMemoryMax() {
		return memoryMax;
	}
	public void setMemoryMax(double memoryMax) {
		this.memoryMax = memoryMax;
	}
	public double getRecordsPerSecond() {
		return recordsPerSecond;
	}
	public void setRecordsPerSecond() {
		if(this.time>0) {
			this.recordsPerSecond = this.numberOfRecords / (this.time/1000d);
		}
	}
	public double getMBperSecond() {
		return MBperSecond;
	}
	public void setMBperSecond() {
		if(this.time>0) {
			this.MBperSecond = this.sizeMB / (this.time/1000);
		}
	}
	public int getNumberOfTests() {
		return numberOfTests;
	}
	public void setNumberOfTests(int numberOfTests) {
		this.numberOfTests = numberOfTests;
	}
	public Date getStartDate() {
		return startDate;
	}
	public void setStartDate(Date startDate) {
		this.startDate = startDate;
	}
	public Date getEndDate() {
		return endDate;
	}
	public void setEndDate(Date endDate) {
		this.endDate = endDate;
	}
	public String getResume() {
		if(startDate==null || endDate == null) {
			return this.getDatastore() + ";" + this.getObject()+ ";" + this.getOperation() +";" + 
					this.getNumberOfRecords() +";;;" +this.getTime()+";" + this.getSizeMB()+ ";" + this.getMemoryAvg() + ";"+
					this.getMemoryMax()+";"+this.getRecordsPerSecond()+";"+this.getMBperSecond()+";" + this.getOs();
		}
		else {
			DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");  
			String strStartDate = dateFormat.format(startDate);  
			String strEndDate = dateFormat.format(endDate);  
				 
			return this.getDatastore() + ";" + this.getObject()+ ";" + this.getOperation() +";" + 
				this.getNumberOfRecords() +";"+strStartDate+";"+strEndDate+";" +this.getTime()+";" + this.getSizeMB()+ ";" + this.getMemoryAvg() + ";"+
				this.getMemoryMax()+";"+this.getRecordsPerSecond()+";"+this.getMBperSecond()+";"+ this.getOs();
		}
	}

	public String getFullResume() {
		return this.getOs()+";"+this.getTestType().getCod()+";"+ 
			this.getTestType().getCod()+ "_" +
			EnumDatabase.valueOf(this.getDatastore()).getCod()+"_" + this.getTestType().name()+";"+
			(this.isHadPK()?"BEFORE":"AFTER") + ";" +
			(this.isEncoded()?"Yes":"No") + ";" + this.getFormat() + ";" + 
			this.getDatastore() + ";" + this.getObject()+ ";" + 
			this.getRawSize() + ";" + this.getOperation() +";" + 
			this.getNumberOfRecords() +";"+
			Math.round(this.getTime()*1000d)/1000d+";" + 
			Math.round(this.getSizeMB()*1000d)/1000d+ ";" + 
			Math.round(this.getMemoryAvg()*1000d)/1000d+ ";"+
			Math.round(this.getMemoryMax()*1000d)/1000d+";"+
			Math.round(this.getRecordsPerSecond()*1000d)/1000d+";"+
			Math.round(this.getMBperSecond()*1000d)/1000d;
	}
	
	public static String getResumeHeader() {
		return "Datastore;Object;Operation;NumberOfRecords;Time;SizeMB;MemoryAVG;MemoryMax;RecordsPerSec;MBPerSec;OS";
	}
	
	public static String getFullResumeHeader() {
		return "OS;Type;Detail;PK;Encoded;Format;Datastore;Object;RawSizeMB;Operation;NumberOfRecords;Time;SizeMB;MemoryAVG;MemoryMax;RecordsPerSec;MBPerSec";
	}
	
	public Metric clone() {
		Metric oMetric = new Metric();
		oMetric.setTestType(this.getTestType());
		oMetric.setOs(this.getOs()); 
		oMetric.setOperation(this.getOperation()); 
		oMetric.setDatastore(this.getDatastore()); 
		oMetric.setObject(this.getObject());  
		oMetric.setFormat(this.getFormat());  
		oMetric.setEncoded(this.isEncoded());  
		oMetric.setHadPK(this.isHadPK());  
		oMetric.setNumberOfRecords(this.getNumberOfRecords());  
		oMetric.setTime(this.getTime());  
		oMetric.setSizeMB(this.getSizeMB());    
		oMetric.setRawSize(this.getRawSize());
		oMetric.setMemoryAvg(this.getMemoryAvg());   
		oMetric.setMemoryMax(this.getMemoryMax());    
		oMetric.setNumberOfTests(this.getNumberOfTests());    
		oMetric.setStartDate(this.getStartDate());
		oMetric.setEndDate(this.getEndDate()); 
		oMetric.setRecordsPerSecond();
		oMetric.setMBperSecond();
		return oMetric;
	}
}
