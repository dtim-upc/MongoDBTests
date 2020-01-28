package edu.upc.essi.mongo.manager;

import java.util.List;
import com.google.common.collect.Lists;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import edu.upc.essi.mongo.datagen.DocumentSet;
import org.bson.Document;

public class MongoDBManager  {

	private static MongoDBManager instance = null;
	private static String collection;

	public static MongoDBManager getInstance(String collection) {
		if (instance == null)
			instance = new MongoDBManager(collection);
		return instance;
	}


	public MongoDBManager(String collection) {
		this.collection = collection;
		//loadProperties();
	}

	public void insert() {
		MongoClient client = MongoClients.create();
		MongoDatabase theDB = client.getDatabase("ideas_experiments");
		theDB.getCollection(collection).insertMany(DocumentSet.getInstance().documents);
	}
	
}
