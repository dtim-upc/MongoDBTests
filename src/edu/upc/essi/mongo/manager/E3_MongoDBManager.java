package edu.upc.essi.mongo.manager;

import com.google.common.collect.Lists;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.opencsv.CSVWriter;
import edu.upc.essi.mongo.datagen.DocumentSet;
import edu.upc.essi.mongo.datagen.E3_DocumentSet;

import org.bson.BsonNull;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
		theDB.getCollection(collection + kind).insertMany(E3_DocumentSet.getInstance().getByName(kind));
		long elapsedTime = System.nanoTime() - startTime;

		writer.writeNext(new String[] { "Mongo", "insert", kind.substring(kind.lastIndexOf("_") + 1),
				String.valueOf(1d - Math.pow(2, -probability)), String.valueOf(elapsedTime) });
	}

	public void sumJSONWithAttributes(String kind) throws Exception {
//		ArrayList<String> attribs = Lists.newArrayList(
//				IntStream.range(1,65).boxed().map(i->"a"+(i < 10 ? '0' + String.valueOf(i) : String.valueOf(i)))
//						.sorted().collect(Collectors.toList()));
//		List<String> mongoAtts = attribs.stream().map(a -> "$" + a).collect(Collectors.toList());

//		Document groupStage = new Document();
//		groupStage.put("_id", null);
//		groupStage.put("totalsum", new Document("$sum", "$localsum"));
//		long startTime = System.nanoTime();
//		int res = theDB.getCollection(collection+kind)
//				.aggregate(Lists.newArrayList(
//						new Document("$project", new Document("localsum", new Document("$sum", mongoAtts))),
//						new Document("$group", groupStage)))
//				.first().getInteger("totalsum");
//		System.out.println("MongoDB sumJSONWithAttributes");

		List<Document> groupStage = Arrays.asList(new Document("$group",
				new Document("_id", new BsonNull()).append("sum", new Document("$sum", "$a01"))));
//				new Document();
//		groupStage.put("_id", new BsonNull());
//		groupStage.put("sum", new Document("$sum", "$a01"));
		System.out.println(groupStage);
		long startTime = System.nanoTime();
		int res = theDB.getCollection(collection + kind).aggregate(groupStage).first().getInteger("sum");
		System.out.println(res);

//		System.out.println(res);
		long elapsedTime = System.nanoTime() - startTime;
		writer.writeNext(new String[] { "Mongo", "sum", kind.substring(kind.lastIndexOf("_") + 1),
				String.valueOf(1d - Math.pow(2, -probability)), String.valueOf(elapsedTime) });
	}

	public void countNulls(String kind) {
		long startTime, elapsedTime = 0;
		if (kind.contains("_NULLS_ARE_TEXT")) {
			startTime = System.nanoTime();
		long y = theDB.getCollection(collection + kind).countDocuments(new Document("a01", new Document("$eq", null)));
		System.out.println(y);
//			AggregateIterable<Document> res = theDB.getCollection(collection + kind).aggregate(
//					Lists.newArrayList(new Document("$match", new Document("a01", new Document("$eq", null))),
//							new Document("$count", "a01")));
//			System.out.println(res.first() == null ? 0 : res.first().getInteger("a01"));
			elapsedTime = System.nanoTime() - startTime;
		} else if (kind.contains("_NULLS_ARE_NOTHING")) {
			startTime = System.nanoTime();
			long y = theDB.getCollection(collection + kind).countDocuments(new Document("a01", new Document("$exists", false)));
			System.out.println(y);	
			
//			AggregateIterable<Document> res = theDB.getCollection(collection + kind)
//					.aggregate(Lists.newArrayList(
//							new Document("$match", new Document("a01", new Document("$exists", false))),
//							new Document("$count", "a01")));
//			System.out.println(res.first() == null ? 0 : res.first().getInteger("a01"));
			elapsedTime = System.nanoTime() - startTime;
		} else if (kind.contains("_NULLS_ARE_ZERO")) {
			startTime = System.nanoTime();
			long y = theDB.getCollection(collection + kind).countDocuments(new Document("a01", new Document("$eq", 0)));
			System.out.println(y);
//			AggregateIterable<Document> res = theDB.getCollection(collection + kind)
//					.aggregate(Lists.newArrayList(new Document("$match", new Document("a01", new Document("$eq", 0))),
//							new Document("$count", "a01")));
//			System.out.println(res.first() == null ? 0 : res.first().getInteger("a01"));
			elapsedTime = System.nanoTime() - startTime;
		}
		writer.writeNext(new String[] { "Mongo", "countNulls", kind.substring(kind.lastIndexOf("_") + 1),
				String.valueOf(1d - Math.pow(2, -probability)), String.valueOf(elapsedTime) });
	}

	public void countNotNulls(String kind) {
		long startTime, elapsedTime = 0;
		if (kind.contains("_NULLS_ARE_TEXT")) {
			startTime = System.nanoTime();
			long y = theDB.getCollection(collection + kind).countDocuments(new Document("a01", new Document("$ne", null)));
			System.out.println(y);
//			AggregateIterable<Document> res = theDB.getCollection(collection + kind).aggregate(
//					Lists.newArrayList(new Document("$match", new Document("a01", new Document("$ne", null))),
//							new Document("$count", "a01")));
//			System.out.println(res.first() == null ? 0 : res.first().getInteger("a01"));
			elapsedTime = System.nanoTime() - startTime;
		} else if (kind.contains("_NULLS_ARE_NOTHING")) {
			startTime = System.nanoTime();
			long y = theDB.getCollection(collection + kind).countDocuments(new Document("a01", new Document("$exists", true)));
			System.out.println(y);
//			AggregateIterable<Document> res = theDB
//					.getCollection(
//							collection + kind)
//					.aggregate(Lists.newArrayList(
//							new Document("$match",
//									new Document("a01", new Document("$not", new Document("$exists", false)))),
//							new Document("$count", "a01")));
//			System.out.println(res.first() == null ? 0 : res.first().getInteger("a01"));
			elapsedTime = System.nanoTime() - startTime;
		} else if (kind.contains("_NULLS_ARE_ZERO")) {
			startTime = System.nanoTime();
			long y = theDB.getCollection(collection + kind).countDocuments(new Document("a01", new Document("$ne", 0)));
			System.out.println(y);
//			AggregateIterable<Document> res = theDB.getCollection(collection + kind)
//					.aggregate(Lists.newArrayList(new Document("$match", new Document("a01", new Document("$ne", 0))),
//							new Document("$count", "a01")));
//			System.out.println(res.first() == null ? 0 : res.first().getInteger("a01"));
			elapsedTime = System.nanoTime() - startTime;
		}
		writer.writeNext(new String[] { "Mongo", "countNotNulls", kind.substring(kind.lastIndexOf("_") + 1),
				String.valueOf(1d - Math.pow(2, -probability)), String.valueOf(elapsedTime) });

	}

	public void size(String kind) {
		Document result = theDB.runCommand(new Document("collStats", collection + kind));
		writer.writeNext(new String[] { "Mongo", "size", kind.substring(kind.lastIndexOf("_") + 1),
				String.valueOf(1d - Math.pow(2, -probability)), "", result.get("size").toString(),
				result.get("storageSize").toString() });
	}

	public void destroyme() {
		instance = null;
	}

}
