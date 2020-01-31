package edu.upc.essi.mongo.exp;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.RandomStringUtils;
import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Projections.*;
import com.mongodb.client.model.Field;

public class CreateData {

	public static void main(String[] args) {
		ArrayList<Exper> list = new ArrayList<>();
//		String folderBase = "/root/mongo/data/"; //root directory that each database is created
//		String idBase = "/root/mongo/idsnew/"; // directory where documentids saved

		// Constant disk size
		list.add(new Exper("40-13m", 40, 13000000));
//		list.add(new Exper("80-13m-2", 40 * 2, 13000000 / 2));
//		list.add(new Exper("160-13m-4", 40 * 4, 13000000 / 4));
//		list.add(new Exper("320-13m-8", 40 * 8, 13000000 / 8));
//		list.add(new Exper("640-13m-16", 40 * 16, 13000000 / 16));

		// change document count
		list.add(new Exper("80-2m", 80, 2000000));
		list.add(new Exper("80-4m", 80, 2000000 * 2));
		list.add(new Exper("80-8m", 80, 2000000 * 4));
		list.add(new Exper("80-16m", 80, 2000000 * 8));
		list.add(new Exper("80-32m", 80, 2000000 * 16));
		list.add(new Exper("80-64m", 80, 2000000 * 32));

		// change document size (40-13m also included from constant disk size)
		list.add(new Exper("80-13m", 40 * 2, 13000000));
		list.add(new Exper("160-13m", 40 * 4, 13000000));
		list.add(new Exper("320-13m", 40 * 8, 13000000));
		list.add(new Exper("640-13m", 40 * 16, 13000000));

//		
//		list.add(new Exper("1000-1m-a", 1000, 1000000,"Test4"));
//		list.add(new Exper("1000-1m-b", 1000, 1000000,"Test4"));

//		list.add(new Exper("40-13m", 40,13000000,"Test5"));
//		list.add(new Exper("80-13m", 80,13000000,"Test5"));
//		list.add(new Exper("160-13m", 160,13000000,"Test5"));
//		list.add(new Exper("320-13m", 320,13000000,"Test5"));
//		list.add(new Exper("640-13m", 640,13000000,"Test5"));
//		list.add(new Exper("1024-13m", 1024,13000000,"Test5"));

//		list.add(new Exper("320-1m", 320, 1000000,"Test6"));
//		list.add(new Exper("320-2m", 320, 2000000,"Test6"));
//		list.add(new Exper("320-4m", 320, 2000000 * 2,"Test6"));
//		list.add(new Exper("320-8m", 320, 2000000 * 4,"Test6"));
//		list.add(new Exper("320-8m-2", 320, 2000000 * 4,"Test6"));
//		list.add(new Exper("320-16m", 320, 2000000 * 8,"Test6"));
//		list.add(new Exper("320-32m", 320, 2000000 * 16,"Test6"));
//		list.add(new Exper("320-64m", 320, 2000000 * 32,"Test6"));

		try {
			for (Exper exper : list) {
				List<BasicDBObject> documents1 = new ArrayList<>();
				File dir = new File(Const.FOLDER_BASE + exper.name);
				dir.mkdir();
				// change according to the mongodb instance
				ProcessBuilder p1 = new ProcessBuilder("mongod", "--config", Const.CONFIG_LOC, "--dbpath",
						Const.FOLDER_BASE + exper.name, "--bind_ip_all", "--fork", "--logpath", Const.LOG_LOC);
				Process p;
				p = p1.start();
				int retval1 = p.waitFor();
				System.out.println(Const.FOLDER_BASE + exper.name);
				MongoClient client = MongoClients.create();
				MongoDatabase db = client.getDatabase("final");

//				Mongo mongo = new Mongo("localhost", 27017);
//				DB db = mongo.getDB("final");// movies

				MongoCollection collection1 = db.getCollection(exper.name);

				for (int i = 0; i < exper.count; i++) {

					BasicDBObject updateFields = new BasicDBObject();
					updateFields.append("item1", RandomStringUtils.randomAlphanumeric(exper.size - 34)); // 40
					documents1.add(updateFields);

					if (i > 0 && i % 10000 == 0) {
//						System.out.println(i);
						if (!documents1.isEmpty()) {
							collection1.insertMany(documents1);
							documents1.clear();
						}
					}

				}
				if (!documents1.isEmpty()) {
					collection1.insertMany(documents1);
					documents1.clear();
				}
				ArrayList al = new ArrayList();
				FindIterable<Document> x = collection1.find()
						.projection(Projections.fields(Projections.include("_id")));

				for (Document dbObject : x) {
					al.add(dbObject.get("_id"));
				}
				System.out.println(al.size());
				// ArrayList al = new ArrayList()
				// do something with your ArrayList
				FileOutputStream fos;
				fos = new FileOutputStream(Const.ID_BASE + exper.name);
				ObjectOutputStream oos = new ObjectOutputStream(fos);
				oos.writeObject(al);
				oos.close();

				client.close();
				ProcessBuilder p2 = new ProcessBuilder("mongo", "localhost:27017/admin", "--eval",
						"db.shutdownServer()");
				Process p3 = p2.start();
				int retval2 = p3.waitFor();
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
