package edu.upc.essi.mongo.ideas_experiments;

import com.google.common.collect.Lists;
import com.opencsv.CSVWriter;
import edu.upc.essi.mongo.datagen.DocumentSet;
import edu.upc.essi.mongo.datagen.E3_DocumentSet;
import edu.upc.essi.mongo.datagen.Generator;
import edu.upc.essi.mongo.manager.*;
import org.apache.commons.io.IOUtils;
import org.bson.Document;

import javax.json.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.nio.file.Files;
import java.util.StringJoiner;

/**
 * Experiment 4: The goal of this experiment is to evaluate the impact of
 * document structure and type declaration
 */
public class E4 {

	public static void generate(CSVWriter writer) throws Exception {
		Generator gen = Generator.getInstance();

		// 100i+1 number of attributes
//		for (int i = 0; i <= 7; ++i) {
//		int attributes = 100 * i + 1;
		for (int i : Lists.newArrayList(1, 2, 4, 8, 16, 32, 64, 128, 256)) {
			int attributes = i;

			// We reuse e2 template generator
			JsonObject template = E2.generateTemplate(1, 1, true, attributes);
			File fileForTemplate = File.createTempFile("template-", ".tmp");
			fileForTemplate.deleteOnExit();
			Files.write(fileForTemplate.toPath(), template.toString().getBytes());

			JsonObject mongoDB_JSONSchema = JSONSchema.generateJSONSchema(template, false, true);
			JsonObject PSQL_JSONSchema = JSONSchema.generateJSONSchema(template, false, false);
			for (int j = 0; j < 100; ++j) {
				gen.generateFromPseudoJSONSchema(10000, fileForTemplate.getAbsolutePath()).stream()
						.map(d -> Document.parse(d.toString())).forEach(DocumentSet.getInstance().documents::add);
				E4_MongoDBManager.getInstance("e4_" + i, attributes, mongoDB_JSONSchema, writer)
						.insert("_JSON_withoutVal");
				E4_MongoDBManager.getInstance("e4_" + i, attributes, mongoDB_JSONSchema, writer)
						.insert("_JSON_withVal");
				E4_PostgreSQLManager.getInstance("e4_" + i, attributes, PSQL_JSONSchema, writer).insert("_TUPLE");
				E4_PostgreSQLManager.getInstance("e4_" + i, attributes, PSQL_JSONSchema, writer)
						.insert("_JSON_withoutVal");
				E4_PostgreSQLManager.getInstance("e4_" + i, attributes, PSQL_JSONSchema, writer)
						.insert("_JSON_withVal");
				DocumentSet.getInstance().documents.clear();
			}
			for (int j = 0; j < 20; j++) {
				ProcessBuilder p21 = new ProcessBuilder("/root/mongo/distrib/clear.sh");
				Process p31 = p21.start();
				BufferedReader reader = new BufferedReader(new InputStreamReader(p31.getInputStream()));
				StringJoiner sj = new StringJoiner(System.getProperty("line.separator"));
				reader.lines().iterator().forEachRemaining(sj::add);
				String xresult = sj.toString();
				int retvalx = p31.waitFor();
				System.out.println(xresult);

				E4_MongoDBManager.getInstance("e4_" + i, attributes, mongoDB_JSONSchema, writer)
						.sum("_JSON_withoutVal");
				E4_MongoDBManager.getInstance("e4_" + i, attributes, mongoDB_JSONSchema, writer).sum("_JSON_withVal");
				E4_PostgreSQLManager.getInstance("e4_" + i, attributes, PSQL_JSONSchema, writer).sum("_TUPLE");
				E4_PostgreSQLManager.getInstance("e4_" + i, attributes, PSQL_JSONSchema, writer)
						.sum("_JSON_withoutVal");
				E4_PostgreSQLManager.getInstance("e4_" + i, attributes, PSQL_JSONSchema, writer).sum("_JSON_withVal");

				E4_MongoDBManager.getInstance("e4_" + i, attributes, mongoDB_JSONSchema, writer)
						.size("_JSON_withoutVal");
				E4_MongoDBManager.getInstance("e4_" + i, attributes, mongoDB_JSONSchema, writer).size("_JSON_withVal");
				E4_PostgreSQLManager.getInstance("e4_" + i, attributes, PSQL_JSONSchema, writer).size("_TUPLE");
				E4_PostgreSQLManager.getInstance("e4_" + i, attributes, PSQL_JSONSchema, writer)
						.size("_JSON_withoutVal");
				E4_PostgreSQLManager.getInstance("e4_" + i, attributes, PSQL_JSONSchema, writer).size("_JSON_withVal");
			}
			E4_MongoDBManager.getInstance("e4_" + i, attributes, mongoDB_JSONSchema, writer).destroyme();
			E4_PostgreSQLManager.getInstance("e4_" + i, attributes, PSQL_JSONSchema, writer).destroyme();
		}
	}

	public static void main(String[] args) throws Exception {
		CSVWriter writer = new CSVWriter(new FileWriter("ideas_e4.csv"));
		writer.writeNext(
				new String[] { "DB", "operation", "storage", "attributes", "runtime (ns)", "size", "compresed" });
		generate(writer);
		writer.close();
	}

}
