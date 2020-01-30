package edu.upc.essi.mongo.manager;

//import com.mongodb.Block;
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
import com.mongodb.client.model.Filters;

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
import org.bson.conversions.Bson;

public class E1_MongoDBManager {

	private static E1_MongoDBManager instance = null;
	private static String collection;
	private String schema;

	private static MongoDatabase theDB;

	public static E1_MongoDBManager getInstance(String collection, String schema) {
		if (instance == null)
			instance = new E1_MongoDBManager(collection, schema);
		return instance;
	}

	public E1_MongoDBManager(String collection, String schema) {
		this.collection = collection;
		this.schema = schema;

		MongoClient client = MongoClients.create();
		theDB = client.getDatabase("ideas_experiments");
		theDB.drop();
	}

	public void insertAsJSONWithArray() {
		theDB.getCollection(collection + "_JSON_withArray").insertMany(DocumentSet.getInstance().documents);
	}

	public void sumJSONWithArray() {
//
//		BsonField code = Accumulators.last("code", "$ape.code");
//		BsonField label = Accumulators.last("label", "$ape.label");
		BsonField count = Accumulators.sum("total", "$theArray");
		Bson computed =Projections.computed("total", count);
		
		theDB.getCollection(collection + "_JSON_withArray")
				.aggregate(Arrays.asList(
						Aggregates.project(
								computed))).first();

	}

	public void insertAsJSONWithAttributes() {
		theDB.getCollection(collection + "_JSON_withAttributes")
				.insertMany(DocumentSet.getInstance().documents.stream().map(d -> {
					Document copy = Document.parse(d.toJson());
					for (int i = 0; i < copy.getList("theArray", Integer.class).size(); ++i) {
						copy.put("a" + i, copy.getList("theArray", Integer.class).get(i));
					}
					copy.remove("theArray");
					return copy;
				}).collect(Collectors.toList()));
	}

	public void sumJSONWithAttributes() throws Exception {

		ArrayList<String> attribs = getAttributListForE1(false);

		List<? extends Bson> pipeline = Arrays.asList(project(computed("fld", sum("fld", attribs))),
				group(new BsonNull(), sum("fieldN", "$fld")));

		System.out.println(pipeline);

		System.out.println(theDB.getCollection(collection + "_JSON_withAttributes").aggregate(pipeline).first());

//		theDB.getCollection(collection + "_JSON_withAttributes").aggregate(pipeline);

	}

	public ArrayList<String> getAttributListForE1(boolean withTypes) throws Exception {
		ArrayList<String> list = new ArrayList<>();
		JsonObject obj = Json.createReader(new StringReader(IOUtils.toString(Paths.get(schema).toUri()))).readObject();
		int size = obj.getJsonObject("properties").getJsonObject("theArray").getInt("maxSize");
		for (int i = 0; i < size; ++i) {
			list.add("a" + i);
		}
		return list;
	}

}