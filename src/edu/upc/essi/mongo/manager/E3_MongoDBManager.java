package edu.upc.essi.mongo.manager;

import com.google.common.collect.Lists;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.opencsv.CSVWriter;
import edu.upc.essi.mongo.datagen.DocumentSet;
import edu.upc.essi.mongo.datagen.E3_DocumentSet;
import org.bson.Document;

public class E3_MongoDBManager {

	private static E3_MongoDBManager instance = null;
	private static String collection;
	private static float probability;
	private static CSVWriter writer;

	private static MongoDatabase theDB;

	public static E3_MongoDBManager getInstance(String collection, float probability, CSVWriter writer) {
		if (instance == null)
			instance = new E3_MongoDBManager(collection, probability, writer);
		return instance;
	}

	public E3_MongoDBManager(String collection, float probability, CSVWriter writer) {
		this.collection = collection;
		this.probability = probability;
		this.writer = writer;

		MongoClient client = MongoClients.create();
		theDB = client.getDatabase("ideas_experiments");
		theDB.drop();
	}

	public void insert(String kind) {
		long startTime = System.nanoTime();
		theDB.getCollection(collection+kind).insertMany(E3_DocumentSet.getInstance().getByName(kind));
		long elapsedTime = System.nanoTime() - startTime;
		writer.writeNext(new String[] { "Mongo", "insert", collection+kind, String.valueOf(probability), String.valueOf(elapsedTime)	});
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

		writer.writeNext(new String[] { "Mongo", "sum", kind, String.valueOf(probability),
				String.valueOf(elapsedTime)});
	}

	public void countNulls(String kind) {
		long startTime, elapsedTime = 0;
		if (kind.contains("_NULLS_ARE_TEXT")) {
			startTime = System.nanoTime();
			AggregateIterable<Document> res = theDB.getCollection(collection+kind).aggregate(
					Lists.newArrayList(new Document("$match",new Document("a",new Document("$eq",null))), new Document("$count","a"))
			);
			elapsedTime = System.nanoTime() - startTime;
			System.out.println(res.first()==null ? 0 : res.first().getInteger("a"));
		}
		else if (kind.contains("_NULLS_ARE_NOTHING")) {
			startTime = System.nanoTime();
			AggregateIterable<Document> res = theDB.getCollection(collection+kind).aggregate(
				Lists.newArrayList(new Document("$match",new Document("a",new Document("$exists",false))), new Document("$count","a"))
			);
			elapsedTime = System.nanoTime() - startTime;
			System.out.println(res.first()==null ? 0 : res.first().getInteger("a"));
		}
		else if (kind.contains("_NULLS_ARE_ZERO")) {
			startTime = System.nanoTime();
			AggregateIterable<Document> res =theDB.getCollection(collection+kind).aggregate(
					Lists.newArrayList(new Document("$match",new Document("a",new Document("$eq",0))), new Document("$count","a"))
			);
			elapsedTime = System.nanoTime() - startTime;
			System.out.println(res.first()==null ? 0 : res.first().getInteger("a"));
		}
		writer.writeNext(new String[] { "Mongo", "countNulls", kind, String.valueOf(probability),String.valueOf(elapsedTime)});
	}

	public void countNotNulls(String kind) {
		long startTime, elapsedTime = 0;
		if (kind.contains("_NULLS_ARE_TEXT")) {
			startTime = System.nanoTime();
			AggregateIterable<Document> res = theDB.getCollection(collection+kind).aggregate(
					Lists.newArrayList(new Document("$match",new Document("a",new Document("$ne",null))), new Document("$count","a"))
			);
			elapsedTime = System.nanoTime() - startTime;
			System.out.println(res.first()==null ? 0 : res.first().getInteger("a"));
		}
		else if (kind.contains("_NULLS_ARE_NOTHING")) {
			startTime = System.nanoTime();
			AggregateIterable<Document> res = theDB.getCollection(collection+kind).aggregate(
					Lists.newArrayList(new Document("$match",new Document("a",new Document("$not",new Document("$exists",false)))), new Document("$count","a"))
			);
			elapsedTime = System.nanoTime() - startTime;
			System.out.println(res.first()==null ? 0 : res.first().getInteger("a"));
		}
		else if (kind.contains("_NULLS_ARE_ZERO")) {
			startTime = System.nanoTime();
			AggregateIterable<Document> res = theDB.getCollection(collection+kind).aggregate(
					Lists.newArrayList(new Document("$match",new Document("a",new Document("$ne",0))), new Document("$count","a"))
			);
			elapsedTime = System.nanoTime() - startTime;
			System.out.println(res.first()==null ? 0 : res.first().getInteger("a"));
		}
		writer.writeNext(new String[] { "Mongo", "countNulls", kind, String.valueOf(probability),
				String.valueOf(elapsedTime)});

	}

	public void size(String kind) {
		Document result = theDB.runCommand(new Document("collStats", collection+kind));
		writer.writeNext(new String[] { "Mongo", "size", kind, String.valueOf(probability),
				"", result.get("size").toString(),
				result.get("storageSize").toString() });
	}

	public void destroyme() {
		instance = null;
	}

}
