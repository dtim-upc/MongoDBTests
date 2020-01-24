package edu.upc.essi.mongo.manager;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import com.google.common.collect.Lists;
import com.mongodb.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import edu.upc.essi.mongo.datagen.Generator;
import org.apache.commons.lang3.RandomStringUtils;
import org.bson.Document;
import org.slf4j.LoggerFactory;

import com.mongodb.util.JSON;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext; 
import edu.upc.essi.mongo.util.EnumDatabase;
import edu.upc.essi.mongo.util.EnumOperation;
import edu.upc.essi.mongo.util.EnumTestType;
import edu.upc.essi.mongo.util.MetricsManager;
import edu.upc.essi.mongo.util.Util;
import edu.upc.essi.mongo.util.reader.PlainFileReader;

public class MongoDBManager  {

	private static MongoDBManager instance = null;
	private static String collection;

	List<Document> documents;

	public static MongoDBManager getInstance(String collection) {
		if (instance == null)
			instance = new MongoDBManager(collection);
		return instance;
	}


	public MongoDBManager(String collection) {
		this.collection = collection;
		//loadProperties();
		documents = Lists.newArrayList();
	}
	
	public void insertBulk(String JSON) {
		documents.add(Document.parse(JSON));
		if (documents.size() > 10000) {
			insert();
		}
	}

	public void finalize() {
		insert();
	}

	private void insert() {
		MongoClient client = MongoClients.create();
		MongoDatabase theDB = client.getDatabase("ideas_experiments");
		theDB.getCollection(collection).insertMany(documents);
		documents.clear();
	}
	
}
