package edu.upc.essi.mongo.manager;

//import com.mongodb.Block;
import com.google.common.collect.Lists;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoClient;
//import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
//import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Accumulators.*;
import edu.upc.essi.mongo.datagen.DocumentSet;
import static com.mongodb.client.model.Projections.*;

import com.mongodb.Block;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.BsonField;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Projections;
import com.opencsv.CSVWriter;
import com.mongodb.client.model.Filters;

import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import javax.json.Json;
import javax.json.JsonObject;

import org.apache.commons.io.IOUtils;
import org.bson.BsonNull;
import org.bson.Document;
import org.bson.codecs.*;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

public class E1_MongoDBManager {

	private static E1_MongoDBManager instance = null;
	private static String collection;
	private String schema;
	private static CSVWriter writer;
	private int size;

	private static MongoDatabase theDB;

	public static E1_MongoDBManager getInstance(String collection, String schema, CSVWriter writer) throws Exception {
		if (instance == null)
			instance = new E1_MongoDBManager(collection, schema, writer);
		return instance;
	}

	public void resetInstance() {
		instance = null;
	}

	public E1_MongoDBManager(String collection, String schema, CSVWriter writer) throws Exception {
		this.collection = collection;
		this.schema = schema;
		this.writer = writer;
		JsonObject obj = Json.createReader(new StringReader(IOUtils.toString(Paths.get(schema).toUri()))).readObject();
		this.size = obj.getJsonObject("properties").getJsonObject("A00").getInt("maxSize");

		MongoClient client = MongoClients.create();
		theDB = client.getDatabase("ideas_experiments");
		theDB.drop();
	}

	public void insertAsJSONWithArray() {
		long startTime = System.nanoTime();
		theDB.getCollection(collection + "_JSON_withArray").insertMany(DocumentSet.getInstance().documents);
		long elapsedTime = System.nanoTime() - startTime;
		writer.writeNext(
				new String[] { "Mongo", String.valueOf(size), "insert", "JSONWithArray", String.valueOf(elapsedTime) });
	}

	public void sumJSONWithArray() throws Exception {
		Document groupStage = new Document();
		groupStage.put("_id", null);
		groupStage.put("totalsum", new Document("$sum", "$localsum"));
		long startTime = System.nanoTime();
		int res = theDB.getCollection(collection + "_JSON_withArray")
				.aggregate(Lists.newArrayList(
						new Document("$project", new Document("localsum", new Document("$sum", "$A00"))),
						new Document("$group", groupStage)))
				.first().getInteger("totalsum");

//		System.out.println("MongoDB sumJSONWithArray");
		System.out.println(res);
		long elapsedTime = System.nanoTime() - startTime;
		writer.writeNext(
				new String[] { "Mongo", String.valueOf(size), "sum", "JSONWithArray", String.valueOf(elapsedTime) });
	}

	public void insertAsJSONWithAttributes() {
		List<Document> data = DocumentSet.getInstance().documents.stream().map(d -> {
			Document copy = Document.parse(d.toJson());
			for (int i = 0; i < copy.getList("A00", Integer.class).size(); ++i) {
				if (i < 9)
					copy.put("A0" + (i + 1), copy.getList("A00", Integer.class).get(i));
				else
					copy.put("A" + (i + 1), copy.getList("A00", Integer.class).get(i));
			}
			copy.remove("A00");
			return copy;
		}).collect(Collectors.toList());
		long startTime = System.nanoTime();
		theDB.getCollection(collection + "_JSON_withAttributes").insertMany(data);
		long elapsedTime = System.nanoTime() - startTime;
		writer.writeNext(new String[] { "Mongo", String.valueOf(size), "insert", "JSONWithAttributes",
				String.valueOf(elapsedTime) });
	}

	public void sumJSONWithAttributes() throws Exception {
		ArrayList<String> attribs = getAttributListForE1(false);
		List<String> mongoAtts = attribs.stream().map(a -> "$" + a).collect(Collectors.toList());

		Document groupStage = new Document();
		groupStage.put("_id", null);
		groupStage.put("totalsum", new Document("$sum", "$localsum"));
		long startTime = System.nanoTime();
		int res = theDB.getCollection(collection + "_JSON_withAttributes")
				.aggregate(Lists.newArrayList(
						new Document("$project", new Document("localsum", new Document("$sum", mongoAtts))),
						new Document("$group", groupStage)))
				.first().getInteger("totalsum");

//		System.out.println("MongoDB sumJSONWithAttributes");
		System.out.println(res);
		long elapsedTime = System.nanoTime() - startTime;
		writer.writeNext(new String[] { "Mongo", String.valueOf(size), "sum", "JSONWithAttributes",
				String.valueOf(elapsedTime) });
	}

	public ArrayList<String> getAttributListForE1(boolean withTypes) throws Exception {
		ArrayList<String> list = new ArrayList<>();

		for (int i = 0; i < size; ++i) {
			if (i < 9)
				list.add("A0" + (i + 1));
			else
				list.add("A" + (i + 1));
		}
		return list;
	}

	public void sizeJSONWithAttributes() {

		Document result = theDB.runCommand(new Document("collStats", collection + "_JSON_withAttributes"));

		writer.writeNext(new String[] { "Mongo", String.valueOf(size), "size", "JSONWithAttributes", "",
				result.get("size").toString(), result.get("storageSize").toString() });

	}

	public void sizeJSONWithArray() {

		Document result = theDB.runCommand(new Document("collStats", collection + "_JSON_withArray"));

		writer.writeNext(new String[] { "Mongo", String.valueOf(size), "size", "JSONWithArray", "",
				result.get("size").toString(), result.get("storageSize").toString() });
	}

}
