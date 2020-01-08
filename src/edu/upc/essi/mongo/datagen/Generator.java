package edu.upc.essi.mongo.datagen;

import java.util.Collections;
import java.util.List;

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
	 *
	 * @param count How many documents to generate
	 * @param attribSize Size of the string values in attributes
	 * @param nulls Fraction of nulls
	 * @return
	 */
	public JSONArray generate(int count, int attribSize, double nulls) {
		JSONArray list = new JSONArray();
		List ids = list64m.subList(0, count);
		Collections.shuffle(ids);
		for (int i = 0; i < count; i++) {
			JSONObject obj = new JSONObject();
			obj.put("_id", ids.get(i));


			obj.put("attrib1", Math.random()<nulls ? "null" : RandomStringUtils.randomAlphanumeric(attribSize));
			list.add(obj);
		}
		return list;
	}

}
