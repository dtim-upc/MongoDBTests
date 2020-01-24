package edu.upc.essi.mongo.util;

import java.util.ArrayList;
import java.util.HashMap;

public class MetricsList extends ArrayList<Metric> {

	private static final long serialVersionUID = 7183902414734179416L;
	
	public HashMap<String,ArrayList<Metric>>  getListsByObject(){ 
		HashMap<String, ArrayList<Metric>> lstObjects = new HashMap<String, ArrayList<Metric>>();
		for (int i = 0; i < this.size(); i++) {
			Metric oMetric = this.get(i);
			if(!lstObjects.containsKey(oMetric.getObject())) {
				if(lstObjects.get(oMetric.getObject())==null) {
					lstObjects.put(oMetric.getObject(), new  ArrayList<Metric>());
				}
				lstObjects.get(oMetric.getObject()).add(oMetric);
			}
			
		}  
		return lstObjects;
	}
}
