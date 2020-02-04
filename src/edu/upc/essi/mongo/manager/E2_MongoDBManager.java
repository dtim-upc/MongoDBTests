package edu.upc.essi.mongo.manager;

//import com.mongodb.Block;

import com.google.common.collect.Lists;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.opencsv.CSVWriter;
import edu.upc.essi.mongo.datagen.DocumentSet;
import org.apache.commons.io.IOUtils;
import org.bson.Document;

import javax.json.Json;
import javax.json.JsonObject;
import java.io.StringReader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

//import com.mongodb.client.MongoClient;
//import com.mongodb.client.MongoDatabase;

public class E2_MongoDBManager {

	private static E2_MongoDBManager instance = null;
	private static String collection;
	private String schema;
	private static CSVWriter writer;

	private static MongoDatabase theDB;

	public static E2_MongoDBManager getInstance(String collection, String schema, CSVWriter writer) {
		if (instance == null)
			instance = new E2_MongoDBManager(collection, schema, writer);
		return instance;
	}

	public E2_MongoDBManager(String collection, String schema, CSVWriter writer) {
		this.collection = collection;
		this.schema = schema;
		this.writer = writer;

		MongoClient client = MongoClients.create();
		theDB = client.getDatabase("ideas_experiments");
		theDB.drop();
	}

	public void insert(String table) {
		long startTime = System.nanoTime();
		theDB.getCollection(collection + table).insertMany(DocumentSet.getInstance().documents);
		long elapsedTime = System.nanoTime() - startTime;
		//writer.writeNext(new String[] { "Mongo", "insert", "JSONWithArray", String.valueOf(elapsedTime) });
	}

	public void sum(int nLevels, int nAttributes, String table) throws Exception {
		String path = "$";
		for (int i = 1; i < nLevels; ++i) {
			path += "a"+(i<10?'0'+String.valueOf(i):i) + ".";
		}
		path += "a"+(nAttributes<10?'0'+String.valueOf(nAttributes):nAttributes);
		Document groupStage = new Document();
		groupStage.put("_id", null);
		groupStage.put("sum", new Document("$sum", path));
		long startTime = System.nanoTime();
		int res = theDB.getCollection(collection + table)
				.aggregate(Lists.newArrayList(new Document("$group", groupStage)))
				.first().getInteger("sum");
		System.out.println(res);
		long elapsedTime = System.nanoTime() - startTime;
		//writer.writeNext(new String[] { "Mongo", "sum", "JSONWithArray", String.valueOf(elapsedTime) });
	}

	public void size(String table) {
		Document result = theDB.runCommand(new Document("collStats", collection + table));
		writer.writeNext(new String[] { "Mongo", "size", table, "", result.get("size").toString(),
				result.get("storageSize").toString() });
	}

}
