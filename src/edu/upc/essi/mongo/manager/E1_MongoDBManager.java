package edu.upc.essi.mongo.manager;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import edu.upc.essi.mongo.datagen.DocumentSet;
import org.bson.Document;

import java.util.stream.Collectors;

public class E1_MongoDBManager {

	private static E1_MongoDBManager instance = null;
	private static String collection;

	private static MongoDatabase theDB;

	public static E1_MongoDBManager getInstance(String collection) {
		if (instance == null)
			instance = new E1_MongoDBManager(collection);
		return instance;
	}


	public E1_MongoDBManager(String collection) {
		this.collection = collection;
		MongoClient client = MongoClients.create();
		theDB = client.getDatabase("ideas_experiments");
		theDB.drop();
	}

	public void insertAsJSONWithArray() {
		theDB.getCollection(collection+"_JSON_withArray").insertMany(DocumentSet.getInstance().documents);
	}

	public void insertAsJSONWithAttributes() {
		theDB.getCollection(collection+"_JSON_withAttributes").insertMany(
				DocumentSet.getInstance().documents
						.stream().map(d -> {
							Document copy = Document.parse(d.toJson());
							for (int i = 0; i < copy.getList("theArray",Integer.class).size(); ++i) {
								copy.put("a"+i,copy.getList("theArray",Integer.class).get(i));
							}
							copy.remove("theArray");
							return copy;
						}).collect(Collectors.toList())
		);
	}
	
}
