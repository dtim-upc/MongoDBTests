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

	private static MongoDatabase theDB;

	public static MongoDBManager getInstance(String collection) {
		if (instance == null)
			instance = new MongoDBManager(collection);
		return instance;
	}


	public MongoDBManager(String collection) {
		this.collection = collection;
		MongoClient client = MongoClients.create();
		theDB = client.getDatabase("ideas_experiments");
		theDB.drop();
	}

	public void insert() {
		theDB.getCollection(collection).insertMany(DocumentSet.getInstance().documents);
	}
	
}
