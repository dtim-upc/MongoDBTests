package edu.upc.essi.mongo.datagen;

import com.google.common.collect.Lists;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.List;

public class DocumentSet {

	private static DocumentSet instance = null;

	public List<Document> documents;

	public static DocumentSet getInstance() {
		if (instance == null)
			instance = new DocumentSet();
		return instance;
	}

	public DocumentSet() {
		documents = Lists.newArrayList();
	}

}
