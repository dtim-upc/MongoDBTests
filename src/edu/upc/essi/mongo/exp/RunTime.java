package edu.upc.essi.mongo.exp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.StringJoiner;

import org.apache.commons.lang3.RandomStringUtils;
//import org.slf4j.LoggerFactory;
import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

//import ch.qos.logback.classic.Level;
//import ch.qos.logback.classic.Logger;
//import ch.qos.logback.classic.LoggerContext;

public class RunTime {

	public static void main(String[] args) {
		String url = "jdbc:postgresql://localhost/postgres";
		String user = "postgres";
		String password = "user";
//		LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
//		Logger rootLogger = loggerContext.getLogger("org.mongodb.driver");
//		rootLogger.setLevel(Level.ERROR);
		ArrayList<Exper> list = new ArrayList<>();

//		list.add(new Exper("40-13m", 40, 13000000));
//		list.add(new Exper("80-13m-2", 40 * 2, 13000000 / 2));
//		list.add(new Exper("160-13m-4", 40 * 4, 13000000 / 4));
//		list.add(new Exper("320-13m-8", 40 * 8, 13000000 / 8));
//		list.add(new Exper("640-13m-16", 40 * 16, 13000000 / 16));

		list.add(new Exper("80-2m", 80, 2000000));
		list.add(new Exper("80-4m", 80, 2000000 * 2));
		list.add(new Exper("80-8m", 80, 2000000 * 4));
		list.add(new Exper("80-16m", 80, 2000000 * 8));
		list.add(new Exper("80-32m", 80, 2000000 * 16));
		list.add(new Exper("80-64m", 80, 2000000 * 32));

		list.add(new Exper("40-13m", 40, 13000000));
		list.add(new Exper("80-13m", 40 * 2, 13000000));
		list.add(new Exper("160-13m", 40 * 4, 13000000));
		list.add(new Exper("320-13m", 40 * 8, 13000000));
		list.add(new Exper("640-13m", 40 * 16, 13000000));

		try {
			FileWriter writer = new FileWriter("/root/mongo/singletime75.csv");
			BufferedWriter buffer = new BufferedWriter(writer);
			System.out.println(Const.CONFIG_LOC);
			for (Exper exper : list) {
				System.out.println(exper.name);
				List l1 = CSVUtils.fillIds(Const.ID_BASE + exper.name);

				for (int k = 0; k < 5; k++) {
					System.out.println(k);
					ProcessBuilder p1 = new ProcessBuilder(Const.MONGOD_LOC, "--config", Const.CONFIG_LOC, "--dbpath",
							Const.FOLDER_BASE + exper.name, "--bind_ip_all", "--fork", "--logpath", Const.LOG_LOC);
					Process p;
					p = p1.start();
					int retval1 = p.waitFor();
					MongoClient client = MongoClients.create();
					MongoDatabase db = client.getDatabase("final");
					MongoCollection collection = db.getCollection(exper.name);
					Random r = new Random();
					for (int j = 0; j < 50000; j++) {
						Thread.sleep(10);
						Document n = (Document) collection.find(new Document("_id", l1.get(r.nextInt(l1.size()))))
								.first();
						n.get("item1");
					}
					for (int i = 0; i < 1000; i++) {
						Thread.sleep(10);
						long start = System.nanoTime();
						Document n = (Document) collection.find(new Document("_id", l1.get(r.nextInt(l1.size()))))
								.first();
						buffer.write(exper.name + "," + String.valueOf(System.nanoTime() - start) + "," + k + "\n");
					}
					client.close();
					ProcessBuilder p2 = new ProcessBuilder("mongo", "localhost:27017/admin", "--eval",
							"db.shutdownServer()");
					Process p3 = p2.start();
					int retval2 = p3.waitFor();

					ProcessBuilder p21 = new ProcessBuilder("/root/mongo/clear.sh");
					Process p31 = p21.start();
//				int retval2 = p3.waitFor();
//				System.out.println(p2.command());
//				System.out.println(retval2);

					BufferedReader reader = new BufferedReader(new InputStreamReader(p31.getInputStream()));
					StringJoiner sj = new StringJoiner(System.getProperty("line.separator"));
					reader.lines().iterator().forEachRemaining(sj::add);
					String xresult = sj.toString();
					int retvalx = p31.waitFor();
					System.out.println(xresult);

				}

			}
			buffer.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
