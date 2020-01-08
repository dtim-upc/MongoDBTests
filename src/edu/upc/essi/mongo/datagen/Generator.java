package edu.upc.essi.mongo.datagen;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.RandomStringUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import edu.upc.essi.mongo.exp.CSVUtils;

public class Generator {

	private static Generator generator_instance = null;

	// variable of type String
	private List list64m;
	public String s;

	// private constructor restricted to this class itself
	private Generator() {
		list64m = CSVUtils.fillIds("data/80-2m");
		System.out.println("Ids loaded");
	}

	// static method to create instance of Singleton class
	public static Generator getInstance() {
		if (generator_instance == null)
			generator_instance = new Generator();
		return generator_instance;
	}

	/**
	 * @param includeIDs if true, documents will contain an "_id" attribute
	 * @param documentCount How many documents to generate
	 * @param attribCount How many attributes to consider
	 * @param attribSize Size of the string values in attributes
	 * @param nullProbability Fraction of nulls
	 * @param arrayProbability if true with a 50% probability an attribute will be an array (using this same method)
	 * @return
	 */
	public JSONArray generate(boolean includeIDs, int documentCount, int attribCount, int attribSize, double nullProbability, double arrayProbability) {
		JSONArray list = new JSONArray();
		List ids = includeIDs ? list64m.subList(0, documentCount) : Lists.newArrayList();
		Collections.shuffle(ids);
		for (int i = 0; i < documentCount; i++) {
			JSONObject obj = new JSONObject();
			if (includeIDs) obj.put("_id", ids.get(i));
			for (int j = 0; j < attribCount; ++j) {
				Object value;
				//TODO this has to be improved, right now null dominates
				if (Math.random() < nullProbability)
					value = "null";
				else if (Math.random() < arrayProbability)
					value = getInstance().generate(false,10,2,3,.5f,0f);
				else
					value = RandomStringUtils.randomAlphanumeric(attribSize);
				obj.put("attrib"+j,value);
			}
			list.add(obj);
		}
		return list;
	}

}
