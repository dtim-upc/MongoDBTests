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
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class E4_MongoDBManager {

	private static E4_MongoDBManager instance = null;
	private static String collection;
	private int attributes;
	private static JsonObject JSONSchema;
	private static CSVWriter writer;

	private static MongoDatabase theDB;

	public static E4_MongoDBManager getInstance(String collection, int attributes, JsonObject JSONSchema, CSVWriter writer) {
		if (instance == null)
			instance = new E4_MongoDBManager(collection, attributes, JSONSchema, writer);
		return instance;
	}

	public E4_MongoDBManager(String collection, int attributes, JsonObject JSONSchema, CSVWriter writer) {
		this.collection = collection;
		this.attributes = attributes;
		this.JSONSchema = JSONSchema;
		this.writer = writer;

		MongoClient client = MongoClients.create();
		theDB = client.getDatabase("ideas_experiments");
		theDB.drop();

		theDB.createCollection(collection+"_JSON_withoutVal");
		theDB.createCollection(collection+"_JSON_withVal",
				new CreateCollectionOptions().validationOptions(
						new ValidationOptions().validator(Filters.jsonSchema(Document.parse(JSONSchema.toString())))));
	}

	public void insert(String kind) {
		long startTime = System.nanoTime();
		theDB.getCollection(collection+kind).insertMany(DocumentSet.getInstance().documents);
		long elapsedTime = System.nanoTime() - startTime;
		writer.writeNext(new String[] { "Mongo", "insert", kind.substring(kind.lastIndexOf("_") + 1),
				String.valueOf(attributes), String.valueOf(elapsedTime)});
	}

	public void sumJSONWithAttributes(String kind) throws Exception {
		ArrayList<String> attribs = Lists.newArrayList(
				IntStream.range(1,65).boxed().map(i->"a"+(i < 10 ? '0' + String.valueOf(i) : String.valueOf(i)))
						.sorted().collect(Collectors.toList()));
		List<String> mongoAtts = attribs.stream().map(a -> "$" + a).collect(Collectors.toList());

		Document groupStage = new Document();
		groupStage.put("_id", null);
		groupStage.put("totalsum", new Document("$sum", "$localsum"));
		long startTime = System.nanoTime();
		int res = theDB.getCollection(collection+kind)
				.aggregate(Lists.newArrayList(
						new Document("$project", new Document("localsum", new Document("$sum", mongoAtts))),
						new Document("$group", groupStage)))
				.first().getInteger("totalsum");

//		System.out.println("MongoDB sumJSONWithAttributes");
		System.out.println(res);
		long elapsedTime = System.nanoTime() - startTime;
		writer.writeNext(new String[] { "Mongo", "sum", kind.substring(kind.lastIndexOf("_") + 1),
				String.valueOf(attributes),String.valueOf(elapsedTime)});
	}

	public void size(String kind) {
		Document result = theDB.runCommand(new Document("collStats", collection+kind));
		writer.writeNext(new String[] { "Mongo", "size", kind.substring(kind.lastIndexOf("_") + 1),
				String.valueOf(attributes),"", result.get("size").toString(),
				result.get("storageSize").toString() });
	}

	public void destroyme() {
		instance = null;
	}

}
