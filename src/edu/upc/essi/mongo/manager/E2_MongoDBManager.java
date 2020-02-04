package edu.upc.essi.mongo.manager;

import com.google.common.collect.Lists;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.opencsv.CSVWriter;
import edu.upc.essi.mongo.datagen.DocumentSet;
import org.bson.Document;

public class E2_MongoDBManager {

	private static E2_MongoDBManager instance = null;
	private static String collection;
	private int nLevels;
	private int nAttributes;
	private static CSVWriter writer;

	private static MongoDatabase theDB;

	public static E2_MongoDBManager getInstance(String collection, int nLevels, int nAttributes, CSVWriter writer) {
		if (instance == null)
			instance = new E2_MongoDBManager(collection, nLevels, nAttributes, writer);
		return instance;
	}

	public E2_MongoDBManager(String collection, int nLevels, int nAttributes, CSVWriter writer) {
		this.collection = collection;
		this.nLevels = nLevels;
		this.nAttributes = nAttributes;
		this.writer = writer;

		MongoClient client = MongoClients.create();
		theDB = client.getDatabase("ideas_experiments");
		theDB.drop();
	}

	public void insert() {
		long startTime = System.nanoTime();
		theDB.getCollection(collection).insertMany(DocumentSet.getInstance().documents);
		long elapsedTime = System.nanoTime() - startTime;
		writer.writeNext(new String[] { "Mongo", "insert", collection, String.valueOf(nLevels),
				String.valueOf(nAttributes), String.valueOf(elapsedTime)	});
	}

	public void sum() throws Exception {
		String path = "$";
		for (int i = 1; i < nLevels; ++i) {
			path += "a"+(i<10?'0'+String.valueOf(i):i) + ".";
		}
		path += "a"+(nAttributes<10?'0'+String.valueOf(nAttributes):nAttributes);
		Document groupStage = new Document();
		groupStage.put("_id", null);
		groupStage.put("sum", new Document("$sum", path));
		long startTime = System.nanoTime();
		int res = theDB.getCollection(collection)
				.aggregate(Lists.newArrayList(new Document("$group", groupStage)))
				.first().getInteger("sum");
		System.out.println(res);
		long elapsedTime = System.nanoTime() - startTime;
		writer.writeNext(new String[] { "Mongo", "sum", collection, String.valueOf(nLevels),
				String.valueOf(nAttributes), String.valueOf(elapsedTime)});
	}

	public void size() {
		Document result = theDB.runCommand(new Document("collStats", collection));
		writer.writeNext(new String[] { "Mongo", "size", collection, String.valueOf(nLevels),
				String.valueOf(nAttributes), "", result.get("size").toString(),
				result.get("storageSize").toString() });
	}

	public void destroyme() {
		instance = null;
	}

}
