package edu.upc.essi.mongo.manager;

import com.google.common.collect.Lists;
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

	public void insert() {
		long startTime = System.nanoTime();
		theDB.getCollection(collection+"_NULLS_ARE_TEXT").insertMany(E3_DocumentSet.getInstance().documents_NULLS_ARE_TEXT);
		long elapsedTime = System.nanoTime() - startTime;
		writer.writeNext(new String[] { "Mongo", "insert", collection+"_NULLS_ARE_TEXT", String.valueOf(probability), String.valueOf(elapsedTime)	});

		startTime = System.nanoTime();
		theDB.getCollection(collection+"_NULLS_ARE_NOTHING").insertMany(E3_DocumentSet.getInstance().documents_NULLS_ARE_NOTHING);
		elapsedTime = System.nanoTime() - startTime;
		writer.writeNext(new String[] { "Mongo", "insert", collection+"_NULLS_ARE_NOTHING", String.valueOf(probability), String.valueOf(elapsedTime)	});

		startTime = System.nanoTime();
		theDB.getCollection(collection+"_NULLS_ARE_ZERO)").insertMany(E3_DocumentSet.getInstance().documents_NULLS_ARE_ZERO);
		elapsedTime = System.nanoTime() - startTime;
		writer.writeNext(new String[] { "Mongo", "insert", collection+"_NULLS_ARE_ZERO", String.valueOf(probability), String.valueOf(elapsedTime)	});
	}

	public void sum() throws Exception {


	}

	public void size() {


	}

	public void destroyme() {
		instance = null;
	}

}
