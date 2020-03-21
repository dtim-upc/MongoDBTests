package edu.upc.essi.mongo.ideas_experiments;

import edu.upc.essi.mongo.datagen.DocumentSet;
import edu.upc.essi.mongo.datagen.Generator;
import edu.upc.essi.mongo.manager.E1_MongoDBManager;
import edu.upc.essi.mongo.manager.E1_PostgreSQLManager;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.util.StringJoiner;

import org.bson.Document;
import org.slf4j.LoggerFactory;

import com.opencsv.CSVWriter;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;

/**
 * Experiment 1: The goal of this experiment is to evaluate the impact of
 * monovalued vs. multivalued attributes. Precisely, we will evaluate the impact
 * of different alternatives to store arrays.
 */
public class E1 {

	public static void generate(String template, CSVWriter writer) throws Exception {

		Generator gen = Generator.getInstance();

		System.out.println("Inserting Data");
		for (int j = 0; j < 100; ++j) {
			System.out.println("Iteration " + j);
			gen.generateFromPseudoJSONSchema(10000, template).stream().map(d -> Document.parse(d.toString()))
					.forEach(DocumentSet.getInstance().documents::add);

			E1_MongoDBManager.getInstance("e1", template, writer).insertAsJSONWithArray();
			E1_MongoDBManager.getInstance("e1", template, writer).insertAsJSONWithAttributes();
			E1_PostgreSQLManager.getInstance("e1", template, writer).insertAsJSONWithArray();
			E1_PostgreSQLManager.getInstance("e1", template, writer).insertAsJSONWithAttributes();
			E1_PostgreSQLManager.getInstance("e1", template, writer).insertAsTupleWithArray();
			E1_PostgreSQLManager.getInstance("e1", template, writer).insertAsTupleWithAttributes();

			DocumentSet.getInstance().documents.clear();

		}

		System.out.println("Insettion Complete \n starting Sum");

		for (int j = 0; j < 20; j++) {
			System.out.println(j);
			ProcessBuilder p21 = new ProcessBuilder("/root/ideas/clear.sh");
			Process p31 = p21.start();
			BufferedReader reader = new BufferedReader(new InputStreamReader(p31.getInputStream()));
			StringJoiner sj = new StringJoiner(System.getProperty("line.separator"));
			reader.lines().iterator().forEachRemaining(sj::add);
			String xresult = sj.toString();
			int retvalx = p31.waitFor();
			System.out.println(xresult);
			E1_PostgreSQLManager.getInstance("e1", template, writer).reconnect();
			E1_MongoDBManager.getInstance("e1", template, writer).sumJSONWithAttributes();
			E1_MongoDBManager.getInstance("e1", template, writer).sumJSONWithArray();
			E1_PostgreSQLManager.getInstance("e1", template, writer).sumTupleWithArray();
			E1_PostgreSQLManager.getInstance("e1", template, writer).sumTupleWithAttributes();
			E1_PostgreSQLManager.getInstance("e1", template, writer).sumJSONWithAttributes();
			E1_PostgreSQLManager.getInstance("e1", template, writer).sumJSONWithArray();
		}
		E1_MongoDBManager.getInstance("e1", template, writer).sizeJSONWithAttributes();
		E1_MongoDBManager.getInstance("e1", template, writer).sizeJSONWithArray();
		E1_PostgreSQLManager.getInstance("e1", template, writer).sizeJSONWithArray();
		E1_PostgreSQLManager.getInstance("e1", template, writer).sizeJSONWithAttributes();
		E1_PostgreSQLManager.getInstance("e1", template, writer).sizeTupleWithArray();
		E1_PostgreSQLManager.getInstance("e1", template, writer).sizeTupleWithAttributes();

		E1_MongoDBManager.getInstance("e1", template, writer).resetInstance();
		E1_PostgreSQLManager.getInstance("e1", template, writer).resetInstance();
		gen.resetIndex();
	}

	public static void main(String[] args) throws Exception {
		LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
		Logger rootLogger = loggerContext.getLogger("org.mongodb.driver");
		rootLogger.setLevel(Level.ERROR);
		CSVWriter writer = new CSVWriter(new FileWriter("ideas_e1.csv"));
		writer.writeNext(
				new String[] { "DB", "Array size", "operation", "parameter", "runtime (ns)", "size", "compresed" });
//		generate("/root/ideas/schemas/e1_withArrays.json", writer);
//		generate("data/generator_schemas/e1_withArrays.json", writer);

//		File templateBase = new File("data/generator_schemas/e1_withArrays");
		File templateBase = new File("/root/ideas/schemas/e1_withArrays");
//		File templateBase = new File("/var/lib/postgresql/MongoDBTests/data/generator_schemas/e1_withArrays");

		for (String template : templateBase.list()) {
			generate(templateBase + "/" + template, writer);
		}

		writer.close();
	}

}
