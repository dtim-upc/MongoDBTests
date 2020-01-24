package edu.upc.essi.mongo.util;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.io.File;

import edu.upc.essi.mongo.util.reader.PlainFileReader;
import edu.upc.essi.mongo.util.writer.PlainFileWriter;

public class MetricsManager {
	
	private String filePath;
	ArrayList<String> lstArray = new ArrayList<String>();
	private int bufferSize = 100;
	private EnumDatabase datastore;
	private EnumOperation operation;
	private String object;
	private Date startDate;
	private Date endDate;
	private long numberRecords;
	private long timeSec;
	private double sizeMB;
	private double totalMemory;
	private long totalMemoryMeasured;
	private double maxMemory=0;
	private String destinationDirPath;
	public void measureMemory() {
		numberRecords++;
		if(numberRecords%100==0) {
			totalMemory += Util.getCurrentlyUsedMemory();
			totalMemoryMeasured++;
		}
		maxMemoryUsed();
	}
	
	public void maxMemoryUsed() {
		double memory = Util.getCurrentlyUsedMemory();
		if(maxMemory<memory) {
			maxMemory=memory;
		}
	}
	
	public MetricsManager(String destinationDirectoryPath) {
		this.destinationDirPath = destinationDirectoryPath;
	}
	
	public MetricsManager(String directoryPath,EnumDatabase datastore,EnumOperation operation,EnumTestType testType) {
		
		this.datastore = datastore;
		this.operation = operation;
		DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmm");  
		String strStartDate = dateFormat.format(new Date()); 
		
		this.filePath = directoryPath+ Util.getConnectorPerOS() + Util.getOS()+"_" + datastore.getCod() +"_"+ testType.name() + "_" + operation.getOperation() + "_" +strStartDate +".stats";
		lstArray.add("Datastore;Object;Operation;Number of Records;Start Date;End Date;Time (ms); size (MB);memory(Avg); memory(Max)");
		
	}
	
	public void setSizeMB(double size) {
		this.sizeMB = size;
	}

	public void startRecord( String object) {
		this.object = object;
		this.delayToDecrease =0l;
		startDate = new Date();
		Long totalMemory = Util.getTotalRuntimeMemory();
		Long freeMemory = Util.getFreeRuntimeMemory();
		Long currentMemory = Util.getCurrentlyUsedMemory();
		System.out.println("Start recording: " + object + ": " +  operation + ": TotalMemory=" + totalMemory + ", FreeMemory= " + freeMemory + ", CurrentUse=" + currentMemory);
		this.numberRecords = 0; 
		this.totalMemory = 0d;
		this.totalMemoryMeasured = 0;
		this.maxMemory = 0;
	}
	
	public void endRecord(long numberRecords) {
		endDate=new Date();
		timeSec=(endDate.getTime()-startDate.getTime())-this.delayToDecrease;

		System.out.println(operation + " " + object + " " + timeSec);
		
		this.numberRecords=numberRecords;

		Long totalMemory = Util.getTotalRuntimeMemory();
		Long freeMemory = Util.getFreeRuntimeMemory();
		Long currentMemory = Util.getCurrentlyUsedMemory();
		System.out.println("End recording: " + object + ": " +  operation + ": TotalMemory=" + totalMemory + ", FreeMemory= " + freeMemory + ", CurrentUse=" + currentMemory);
		
	}
	
	public void setSize(double sizeMB) {
		this.sizeMB = sizeMB;
	}
	private long delay=3000; 
	private long delayToDecrease=0l;
	
	public void makeDelay() {
		try {
			Thread.sleep(delay);
			delayToDecrease +=delay;
		} catch (InterruptedException e) { 
			e.printStackTrace();
		}
	} 
	
	public void recordMetrics() {
		  
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");  
		String strStartDate = dateFormat.format(startDate);  
		String strEndDate = dateFormat.format(endDate);  
		
		double avgMemory = 0;
		if(totalMemoryMeasured >0) {
			avgMemory = totalMemory/totalMemoryMeasured;
		}
		
		String metric = datastore + ";" + object + ";" + operation +";" + numberRecords+";"+strStartDate + ";" + strEndDate+";" +timeSec+";" + sizeMB + ";" + avgMemory + ";"+maxMemory;
				
		lstArray.add(metric);
		if(bufferSize==1000) {
			flushToDisk();
			lstArray=new ArrayList<String>();
		}
	}
		
	public Double getInstancesPerS(Double numberRecords, Date startDate, Date endDate) {
		long diff= (startDate.getTime() - endDate.getTime())/1000;
		if (diff>0) {
			return numberRecords / diff;
		}else {
			return 0d;
		}
	}
	
	public void flushToDisk() {
		
		try {
			PlainFileWriter oWriter = new PlainFileWriter();
			
			oWriter.create(filePath);
			
			for (String string : lstArray) {
				oWriter.write(string+"\n");
				System.out.println(string);
			}
			oWriter.flush();
			oWriter.close();
			lstArray = null;
			lstArray = new ArrayList<String>();
		} catch (IOException e) { 
			e.printStackTrace();
		}
	}

	public MetricsList consolidate(String directoryPath, String directoryPathDataRaw) {
		File dir=new File(directoryPath);
		File[] directoryListing = dir.listFiles();
		MetricsList lstMetrics=new MetricsList();
		
		if (directoryListing != null) {
			 for (File child : directoryListing) { 
				 String[] keywords = child.getName().split("_");
				 String os = keywords[0];
				 String db = keywords[1];
				 String datastore = EnumDatabase.valueOfCod(db).name();
				 EnumTestType testType = EnumTestType.valueOfCod(keywords[2]); 
				 String operation = keywords[3];
				 if(testType==null) {
					 testType = EnumTestType.valueOf(keywords[2]+"_"+keywords[3]+"_"+keywords[4]); 
					 operation = keywords[5];
				 }
				 String format = testType.getFormat();
				 
				 boolean hadPK,encoded; 
				 hadPK = testType.hadPK();
				 encoded  = testType.isEncoded();
				 
				 /*
				 String pk = keywords[3];
				 String cod = keywords[4];
				 String format = keywords[5]; 
				 */
				 PlainFileReader oReader =new PlainFileReader();
				 try {
					oReader.openConnection(child.getAbsolutePath());
					/*
					if(pk.contains("NO")) {
						hadPK = false;	
					}else{
						hadPK = true;								
					}
					*/
					
					/*
					if(cod.contains("NO")) {
						encoded = false;
					}else {
						encoded = true;
					}
					*/
					
					String line=oReader.read();
					line=oReader.read();
					while(line != null) {
						if(line.trim().length()>0) {
							if(line.trim().length()>0) {
								String[] values = line.split(";");
								if(values.length<9)
									System.out.println(directoryPath + " " +  line);
								Metric oMetric= new Metric();
								oMetric.setOs(os);
								oMetric.setDatastore(datastore);
								oMetric.setOperation(operation);
								oMetric.setFormat(format);
								oMetric.setTestType(testType);
								oMetric.setHadPK(hadPK);			
								oMetric.setEncoded(encoded);
								oMetric.setObject(values[1]);
								oMetric.setNumberOfRecords(Long.parseLong(values[3].replace(",", "")));
								oMetric.setTime(Long.parseLong(values[6].replace(",", "")));
								oMetric.setSizeMB(Double.parseDouble(values[7].replace(",", "")));
								oMetric.setMemoryAvg(Double.parseDouble(values[8].replace(",", "")));
								oMetric.setMemoryMax(Double.parseDouble(values[9].replace(",", "")));
								oMetric.setRecordsPerSecond();
								oMetric.setMBperSecond();
								lstMetrics.add(oMetric); 
							}
						}
						line=oReader.read();
					}
					
					oReader.closeConnection();
					
				} catch (IOException e) { 
					e.printStackTrace();
				}
				 
			 }
		}
		HashMap<String,ArrayList<Metric>> listGrouped = groupByTest(lstMetrics);
		HashMap<String,ArrayList<Metric>> listAverage = getAverage(listGrouped);
		HashMap<String,ArrayList<Metric>> listMedia = getMedia(listGrouped);
		saveMetricsToDisk(listAverage,directoryPathDataRaw,true);
		saveMetricsToDisk(listMedia,directoryPathDataRaw,false);
		saveMetricsToDisk(lstMetrics,directoryPathDataRaw);
		
		return lstMetrics;		 	
	}
	
	public HashMap<String,ArrayList<Metric>> groupByTest(MetricsList lstMetrics){
		HashMap<String,ArrayList<Metric>> listMetrics = new HashMap<String,ArrayList<Metric>> ();
		for (Metric oMetric : lstMetrics) {
			String k = oMetric.getDatastore()+"_"+
					oMetric.getOs()	+"_"+ oMetric.getOperation() +"_"+
					iif(oMetric.isHadPK(),"PK","NOPK") +"_"+
					iif(oMetric.isEncoded(),"COD","NOCOD") +"_"+
					oMetric.getFormat();
			if(k!=null) {
				if(listMetrics.get(k)==null) {
					listMetrics.put(k, new ArrayList<Metric>()); 
				} 
				listMetrics.get(k).add(oMetric);
			}
		}
		
		return listMetrics;
	}
	
	private String iif(boolean hadPK, String string1, String string2) { 
		if(hadPK)
			return string1;
		else
			return string2;
	}

	public HashMap<String,ArrayList<Metric>> getAverage(HashMap<String,ArrayList<Metric>> lstMetrics){
		HashMap<String,ArrayList<Metric>> lstAverage= new HashMap<String,ArrayList<Metric>>();
		HashMap<String,ArrayList<Metric>> lstMinMemoryAvg= new HashMap<String,ArrayList<Metric>>();
		HashMap<String,ArrayList<Metric>> lstMaxMemoryAvg= new HashMap<String,ArrayList<Metric>>();
		HashMap<String,ArrayList<Metric>> lstMinMemoryMax= new HashMap<String,ArrayList<Metric>>();
		HashMap<String,ArrayList<Metric>> lstMaxMemoryMax= new HashMap<String,ArrayList<Metric>>();
		HashMap<String,ArrayList<Metric>> lstMinTime= new HashMap<String,ArrayList<Metric>>();
		HashMap<String,ArrayList<Metric>> lstMaxTime= new HashMap<String,ArrayList<Metric>>();
		
		Iterator<Entry<String, ArrayList<Metric>>> it = lstMetrics.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, ArrayList<Metric>> pair = (Map.Entry<String, ArrayList<Metric>>) it.next();
			String k = pair.getKey();
			ArrayList<Metric> v = pair.getValue();
			
			if(lstAverage.get(k)==null) {
				lstAverage.put(k, new ArrayList<Metric>());
				lstMinMemoryAvg.put(k, new ArrayList<Metric>());
				lstMaxMemoryAvg.put(k, new ArrayList<Metric>());
				lstMinMemoryMax.put(k, new ArrayList<Metric>());
				lstMaxMemoryMax.put(k, new ArrayList<Metric>());
				lstMinTime.put(k, new ArrayList<Metric>());
				lstMinTime.put(k, new ArrayList<Metric>());
				lstMinTime.put(k, new ArrayList<Metric>());
				lstMaxTime.put(k, new ArrayList<Metric>());
			}
			for (int iMetric = 0; iMetric < v.size(); iMetric++) {
				Metric oMetric = v.get(iMetric);
				boolean added=false;
				for (int j = 0; j < lstAverage.get(k).size(); j++) {
					if(oMetric.getObject().equals(lstAverage.get(k).get(j).getObject())){
						
						//This two will be eliminated
						
						if(oMetric.getMemoryMax()>lstMaxMemoryMax.get(k).get(j).getMemoryMax()){
							lstMaxMemoryMax.get(k).get(j).setMemoryMax(oMetric.getMemoryMax());	
						}

						if(oMetric.getMemoryMax()<lstMinMemoryMax.get(k).get(j).getMemoryMax()){
							lstMinMemoryMax.get(k).get(j).setMemoryMax(oMetric.getMemoryMax());
						}
						
						//This two will be eliminated
						
						if(oMetric.getMemoryAvg()>lstMaxMemoryAvg.get(k).get(j).getMemoryAvg()){
							lstMaxMemoryAvg.get(k).get(j).setMemoryAvg(oMetric.getMemoryAvg());
						}
						
						if(oMetric.getMemoryAvg()<lstMinMemoryAvg.get(k).get(j).getMemoryAvg()){
							lstMinMemoryAvg.get(k).get(j).setMemoryAvg(oMetric.getMemoryAvg());
							
						}
						
						
						//This two will be eliminated
				
						if(oMetric.getTime()>lstMaxTime.get(k).get(j).getTime()){
							lstMaxTime.get(k).get(j).setTime(oMetric.getTime());
						}
						
						if(oMetric.getTime()<lstMinTime.get(k).get(j).getTime()){
							lstMinTime.get(k).get(j).setTime(oMetric.getTime());
						}
						
						
						added=true;		
						break;
					}else {
						added=false;
					}
				}
				if(!added) {
					
					Metric newMaxMetric = new Metric();
					newMaxMetric.setOs(oMetric.getOs());
					newMaxMetric.setDatastore(oMetric.getDatastore());
					newMaxMetric.setOperation(oMetric.getOperation());
					newMaxMetric.setFormat(oMetric.getFormat());
					newMaxMetric.setHadPK(oMetric.isHadPK());			
					newMaxMetric.setEncoded(oMetric.isEncoded());
					newMaxMetric.setObject(oMetric.getObject());
					newMaxMetric.setNumberOfRecords(oMetric.getNumberOfRecords());
					newMaxMetric.setTestType(oMetric.getTestType());
					newMaxMetric.setNumberOfTests(0);
					newMaxMetric.setTime(oMetric.getTime());
					newMaxMetric.setSizeMB(oMetric.getSizeMB());
					newMaxMetric.setMemoryAvg(oMetric.getMemoryAvg());
					newMaxMetric.setMemoryMax(oMetric.getMemoryMax());
					
					Metric newAvgMetric = newMaxMetric.clone();		
					newAvgMetric.setTime(0l); 
					newAvgMetric.setMemoryAvg(0d);
					newAvgMetric.setMemoryMax(0d);
					
					Metric newMinMetric = newMaxMetric.clone();
					
					lstAverage.get(k).add(newAvgMetric);
					lstMaxMemoryAvg.get(k).add(newMaxMetric.clone());					
					lstMinMemoryAvg.get(k).add(newMinMetric.clone());					
					lstMaxMemoryMax.get(k).add(newMaxMetric.clone());
					lstMinMemoryMax.get(k).add(newMinMetric.clone());
					lstMaxTime.get(k).add(newMaxMetric.clone());
					lstMinTime.get(k).add(newMinMetric.clone());
				}
				
			} 
			
			for (int j = 0; j < lstAverage.get(k).size(); j++) {
				
				for (int iMetric = 0; iMetric < v.size(); iMetric++) {
					Metric oMetric = v.get(iMetric);
					if(oMetric.getObject().equals(lstAverage.get(k).get(j).getObject())){
						lstAverage.get(k).get(j).setTime(lstAverage.get(k).get(j).getTime() + oMetric.getTime());
						lstAverage.get(k).get(j).setNumberOfTests(lstAverage.get(k).get(j).getNumberOfTests()+1);
										
						lstAverage.get(k).get(j).setMemoryAvg(lstAverage.get(k).get(j).getMemoryAvg() + oMetric.getMemoryAvg());
						lstAverage.get(k).get(j).setMemoryMax(lstAverage.get(k).get(j).getMemoryMax() + oMetric.getMemoryMax());
						
					}
				}
			}
			//Calculate the average of the metrics, except the first and last.
			for (int j = 0; j < lstAverage.get(k).size(); j++) {
					double maxMemoryAvg=0;
					double minMemoryAvg=0;
					double maxMemoryMax=0;
					double minMemoryMax=0;
					long maxTime=0;
					long minTime=0;
					
					maxMemoryAvg = lstMaxMemoryAvg.get(k).get(j).getMemoryAvg();
					minMemoryAvg = lstMinMemoryAvg.get(k).get(j).getMemoryAvg();
					maxMemoryMax = lstMaxMemoryMax.get(k).get(j).getMemoryMax();
					minMemoryMax = lstMinMemoryMax.get(k).get(j).getMemoryMax();
					minTime = lstMinTime.get(k).get(j).getTime();
					maxTime = lstMaxTime.get(k).get(j).getTime(); 
					
					lstAverage.get(k).get(j).setMemoryAvg(lstAverage.get(k).get(j).getMemoryAvg() -maxMemoryAvg -minMemoryAvg);
					lstAverage.get(k).get(j).setMemoryMax(lstAverage.get(k).get(j).getMemoryMax() -maxMemoryMax -minMemoryMax);
					lstAverage.get(k).get(j).setTime(lstAverage.get(k).get(j).getTime()-maxTime-minTime);  
			}
			
			
			
			for (int j = 0; j < lstAverage.get(k).size(); j++) {
				if(lstAverage.get(k).get(j).getNumberOfTests()<=2) {
					lstAverage.get(k).get(j).setMemoryAvg(0);
					lstAverage.get(k).get(j).setMemoryMax(0);
					lstAverage.get(k).get(j).setTime(0);
					lstAverage.get(k).get(j).setNumberOfTests(0);
				}else {
					long divisor = (long)(lstAverage.get(k).get(j).getNumberOfTests()-2);
					lstAverage.get(k).get(j).setMemoryAvg(lstAverage.get(k).get(j).getMemoryAvg() / divisor);
					lstAverage.get(k).get(j).setMemoryMax(lstAverage.get(k).get(j).getMemoryMax() / divisor);
					lstAverage.get(k).get(j).setTime(lstAverage.get(k).get(j).getTime() / divisor);
					lstAverage.get(k).get(j).setNumberOfTests((int)divisor);
				}
			}				
		}
		return lstAverage;
		
	}

	public HashMap<String,ArrayList<Metric>> getMedia(HashMap<String,ArrayList<Metric>> lstMetrics){
		HashMap<String,HashMap<String,ArrayList<Metric>>> lstMedia= new HashMap<String,HashMap<String,ArrayList<Metric>>>();
		HashMap<String,ArrayList<Metric>> lstMediaTotal= new HashMap<String,ArrayList<Metric>>();
		
		Iterator<Entry<String, ArrayList<Metric>>> it = lstMetrics.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, ArrayList<Metric>> pair = (Map.Entry<String, ArrayList<Metric>>) it.next();
			String k = pair.getKey();
			ArrayList<Metric> v = pair.getValue();
			
			if(lstMedia.get(k)==null) {
				HashMap<String,ArrayList<Metric>> listObjects = new HashMap<String,ArrayList<Metric>>();
				listObjects.put(v.get(0).getObject(), new ArrayList<Metric>());
				lstMedia.put(k,listObjects);
			}

			
			for (int iMetric = 0; iMetric < v.size(); iMetric++) {
				Metric oMetric = v.get(iMetric);

				if(!lstMedia.get(k).containsKey(oMetric.getObject())) {
					lstMedia.get(k).put(oMetric.getObject(), new ArrayList<Metric>());
				}

				//for (int j = 0; j < lstMedia.get(k).size(); j++) {
				lstMedia.get(k).get(oMetric.getObject()).add(oMetric);
				//}
			}
		}
		//Sort inside each object
 
		Iterator<Entry<String, HashMap<String, ArrayList<Metric>>>> it2= lstMedia.entrySet().iterator();
		while (it2.hasNext()) {
			Map.Entry<String, HashMap<String, ArrayList<Metric>>> pair2 =  (Map.Entry<String, HashMap<String, ArrayList<Metric>>>) it2.next();
			String k = pair2.getKey();
			HashMap<String, ArrayList<Metric>> lv= pair2.getValue();
			Iterator<Entry<String, ArrayList<Metric>>> iMetrics = lv.entrySet().iterator();
			
			while(iMetrics.hasNext()) {
				Entry<String, ArrayList<Metric>>  pair3= iMetrics.next();
				ArrayList<Metric> values=pair3.getValue();
				for(int j=0;j<values.size();j++) {
					for(int m=1;m<values.size()-j;m++) {
						double tempValue=0;
						long tempValuel=0l;	
						if(values.get(m-1).getMemoryAvg()>values.get(m).getMemoryAvg()) {
							tempValue = values.get(m-1).getMemoryAvg();
							values.get(m-1).setMemoryAvg(values.get(m).getMemoryAvg());
							values.get(m).setMemoryAvg(tempValue);
						}
						if(values.get(m-1).getMemoryMax()>values.get(m).getMemoryMax()) {
							tempValue = values.get(m-1).getMemoryMax();
							values.get(m-1).setMemoryMax(values.get(m).getMemoryMax());
							values.get(m).setMemoryMax(tempValue);
						}
						if(values.get(m-1).getTime()>values.get(m).getTime()) {
							tempValuel = values.get(m-1).getTime();
							values.get(m-1).setTime(values.get(m).getTime());
							values.get(m).setTime(tempValuel);
						}
					}
				}
				
				if(!lstMediaTotal.containsKey(k)) {
					lstMediaTotal.put(k, new ArrayList<Metric>());
				}
				
				//Choose the media
				//Add the value
				int size = values.size();
				
				if(size%2==0) {//Calculate the average of the two median values
					int j1 = size/2;
					int j2 = j1-1;
					
					Metric oMetric = values.get(j1);
					oMetric.setMemoryAvg((values.get(j1).getMemoryAvg()+values.get(j2).getMemoryAvg())/2);
					oMetric.setMemoryMax((values.get(j1).getMemoryMax()+values.get(j2).getMemoryMax())/2);
					oMetric.setTime((values.get(j1).getTime()+values.get(j2).getTime())/2);
					lstMediaTotal.get(k).add(oMetric);
				}else {//Return the median value	
					int j = (size-1)/2;
					Metric oMetric = values.get(j);
					lstMediaTotal.get(k).add(oMetric);
				}				
			} 
		}	
						
		return lstMediaTotal;
		
	}
	
	
	public void saveMetricsToDisk(MetricsList  lstMetrics, String rawDataPathFile) { 
		PlainFileWriter oWriterFull = new PlainFileWriter();
		try {
			oWriterFull.create(destinationDirPath+ Util.getConnectorPerOS()+"full.csv");
			oWriterFull.write(Metric.getFullResumeHeader()+"\n");
			for (Metric metric : lstMetrics) {
				String content = metric.getFullResume()+"\n";
				oWriterFull.write(content);
			}
			oWriterFull.flush();
			oWriterFull.close();
		} catch (IOException e) { 
			e.printStackTrace();
		}
		
	}
	public void saveMetricsToDisk(HashMap<String,ArrayList<Metric>> lstAverage, String rawDataPathFile, boolean isAverage) { 
		HashMap<String, PlainFileWriter> fileWriter = new HashMap<String, PlainFileWriter>();
		PlainFileWriter oWriterConsolidated = new PlainFileWriter();
		
		HashMap<String, Double> sizes= new HashMap<String, Double>();
		try { 
			Iterator<Entry<String, ArrayList<Metric>>> it = lstAverage.entrySet().iterator();
			String suffix="";
			if(isAverage) {
				suffix="AVG";
			}else {
				suffix="MED";
			}
			
			oWriterConsolidated.create(destinationDirPath+ Util.getConnectorPerOS()+"fullConsolidated" + suffix + ".csv");
			oWriterConsolidated.write(Metric.getFullResumeHeader()+"\n");
			
			while (it.hasNext()) {
				Map.Entry<String, ArrayList<Metric>> pair = (Map.Entry<String, ArrayList<Metric>>) it.next();
				//String k1 = pair.getKey();
				ArrayList<Metric> v1 = pair.getValue();
			 
				try {
					for (Metric metric : v1) {
						if(!fileWriter.containsKey(metric.getObject())) {
							PlainFileWriter oFileWriter = new PlainFileWriter();
							double sizeMB = Util.getFileSizeMBFromDirectory(rawDataPathFile, metric.getObject());				
							
							oFileWriter.create(destinationDirPath + Util.getConnectorPerOS() + sizeMB +"MB."+metric.getObject() + "." + suffix + ".cst");
							sizes.put(metric.getObject(), sizeMB);
							oFileWriter.write(Metric.getFullResumeHeader()+"\n");
							fileWriter.put(metric.getObject(), oFileWriter);
						}
					}
					for (Metric metric : v1) { 	
						PlainFileWriter oFileWriter = fileWriter.get(metric.getObject());
						metric.setRawSize(sizes.get(metric.getObject()));
						String content = metric.getFullResume()+"\n";
						oFileWriter.write(content);
						oWriterConsolidated.write(content);
					}
					
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			
			fileWriter.forEach((k,v)->{
				try {
					v.flush();
					v.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			});
			oWriterConsolidated.flush();
			oWriterConsolidated.close();
				
		} catch (Exception e) { 
			e.printStackTrace();
		}
		
		
	}
	
}
