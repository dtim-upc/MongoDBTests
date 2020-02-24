package edu.upc.essi.mongo.manager;

import com.google.common.collect.Lists;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ValidationOptions;
import com.opencsv.CSVWriter;
import edu.upc.essi.mongo.datagen.DocumentSet;
import edu.upc.essi.mongo.datagen.E3_DocumentSet;
import org.bson.Document;

import javax.json.JsonObject;

public class E4_MongoDBManager {

	private static E4_MongoDBManager instance = null;
	private static String collection;
	private static JsonObject JSONSchema;
	private static CSVWriter writer;

	private static MongoDatabase theDB;

	public static E4_MongoDBManager getInstance(String collection, JsonObject JSONSchema, CSVWriter writer) {
		if (instance == null)
			instance = new E4_MongoDBManager(collection, JSONSchema, writer);
		return instance;
	}

	public E4_MongoDBManager(String collection, JsonObject JSONSchema, CSVWriter writer) {
		this.collection = collection;
		this.JSONSchema = JSONSchema;
		this.writer = writer;

		MongoClient client = MongoClients.create();
		theDB = client.getDatabase("ideas_experiments");
		theDB.drop();

		theDB.createCollection(collection,
				new CreateCollectionOptions().validationOptions(
						new ValidationOptions().validator(Filters.jsonSchema(Document.parse(JSONSchema.toString())))));
	}

	public void insert() {
		long startTime = System.nanoTime();
		theDB.getCollection(collection).insertMany(DocumentSet.getInstance().documents);
		long elapsedTime = System.nanoTime() - startTime;
		//writer.writeNext(new String[] { "Mongo", "insert", kind.substring(kind.lastIndexOf("_")+1),
		//		String.valueOf(1d-Math.pow(2,-probability)), String.valueOf(elapsedTime)	});
	}

	public void sum(String kind) {
		Document groupStage = new Document();
		groupStage.put("_id", null);
		groupStage.put("sum", new Document("$sum", "$a"));
		long startTime = System.nanoTime();
		int res = theDB.getCollection(collection+kind)
				.aggregate(Lists.newArrayList(new Document("$group", groupStage)))
				.first().getInteger("sum");
		long elapsedTime = System.nanoTime() - startTime;
		System.out.println(res);

		//writer.writeNext(new String[] { "Mongo", "sum", kind.substring(kind.lastIndexOf("_")+1),
		//		String.valueOf(1d-Math.pow(2,-probability)),String.valueOf(elapsedTime)});
	}
/**
	public void size(String kind) {
		Document result = theDB.runCommand(new Document("collStats", collection+kind));
		writer.writeNext(new String[] { "Mongo", "size", kind.substring(kind.lastIndexOf("_")+1),
				String.valueOf(1d-Math.pow(2,-probability)),"", result.get("size").toString(),
				result.get("storageSize").toString() });
	}
**/
	public void destroyme() {
		instance = null;
	}

}
