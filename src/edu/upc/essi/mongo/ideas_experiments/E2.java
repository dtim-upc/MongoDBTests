package edu.upc.essi.mongo.ideas_experiments;

import com.google.common.collect.Lists;
import com.opencsv.CSVWriter;
import edu.upc.essi.mongo.datagen.DocumentSet;
import edu.upc.essi.mongo.datagen.Generator;
import edu.upc.essi.mongo.manager.E1_MongoDBManager;
import edu.upc.essi.mongo.manager.E1_PostgreSQLManager;
import edu.upc.essi.mongo.manager.E2_MongoDBManager;
import edu.upc.essi.mongo.manager.E2_PostgreSQLManager;
import org.bson.Document;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.util.StringJoiner;

/**
 * Experiment 2: The goal of this experiment is to evaluate the impact of
 * embedded objects. Precisely, we will evaluate how the DB behaves in front of
 * different levels of nesting. Two settings will be considered, one where the
 * document size is kept constant, and one where the document size increases
 */
public class E2 {

	public static void generate(CSVWriter writer) throws Exception {

		Generator gen = Generator.getInstance();

		for (int levels : Lists.newArrayList(1, 2, 4, 8, 16, 32, 64)) {
			int attributes = 64;

			JsonObject templateWithSiblings = generateTemplate(1, levels, true, attributes);
			File fileForTemplateWithSiblings = File.createTempFile("template-", ".tmp");
			fileForTemplateWithSiblings.deleteOnExit();
			Files.write(fileForTemplateWithSiblings.toPath(), templateWithSiblings.toString().getBytes());

			JsonObject templateWithoutSiblings = generateTemplate(1, levels, false, attributes);
			File fileForTemplateWithoutSiblings = File.createTempFile("template-", ".tmp");
			fileForTemplateWithoutSiblings.deleteOnExit();
			Files.write(fileForTemplateWithoutSiblings.toPath(), templateWithoutSiblings.toString().getBytes());

			// Experiment with siblings
			String table1 = "e2_JSON_withSiblings" + "_" + levels + "levels";
			for (int j = 0; j < 100; ++j) {
				gen.generateFromPseudoJSONSchema(10000, fileForTemplateWithSiblings.getAbsolutePath()).stream()
						.map(d -> Document.parse(d.toString())).forEach(DocumentSet.getInstance().documents::add);
				E2_MongoDBManager.getInstance(table1, levels, attributes, writer).insert();
				E2_PostgreSQLManager.getInstance(table1, levels, attributes, writer).insert();

				DocumentSet.getInstance().documents.clear();
			}
			for (int j = 0; j < 20; j++) {
				ProcessBuilder p21 = new ProcessBuilder("/root/ideas/clear.sh");
				Process p31 = p21.start();
				BufferedReader reader = new BufferedReader(new InputStreamReader(p31.getInputStream()));
				StringJoiner sj = new StringJoiner(System.getProperty("line.separator"));
				reader.lines().iterator().forEachRemaining(sj::add);
				String xresult = sj.toString();
				int retvalx = p31.waitFor();
				System.out.println(xresult);
				E2_PostgreSQLManager.getInstance(table1, levels, attributes, writer).reconnect();
				E2_MongoDBManager.getInstance(table1, levels, attributes, writer).sum();
				E2_PostgreSQLManager.getInstance(table1, levels, attributes, writer).sum();
			}
			E2_MongoDBManager.getInstance(table1, levels, attributes, writer).size();
			E2_PostgreSQLManager.getInstance(table1, levels, attributes, writer).size();

			E2_MongoDBManager.getInstance(table1, levels, attributes, writer).destroyme();
			E2_PostgreSQLManager.getInstance(table1, levels, attributes, writer).destroyme();

			gen.resetIndex();

			String table2 = "e2_JSON_withoutSiblings" + "_" + levels + "levels";
			for (int j = 0; j < 100; ++j) {
				gen.generateFromPseudoJSONSchema(10000, fileForTemplateWithoutSiblings.getAbsolutePath()).stream()
						.map(d -> Document.parse(d.toString())).forEach(DocumentSet.getInstance().documents::add);
				E2_MongoDBManager.getInstance(table2, levels, attributes, writer).insert();
				E2_PostgreSQLManager.getInstance(table2, levels, attributes, writer).insert();

				DocumentSet.getInstance().documents.clear();
			}
			for (int j = 0; j < 20; j++) {
				ProcessBuilder p21 = new ProcessBuilder("/root/ideas/clear.sh");
				Process p31 = p21.start();
				BufferedReader reader = new BufferedReader(new InputStreamReader(p31.getInputStream()));
				StringJoiner sj = new StringJoiner(System.getProperty("line.separator"));
				reader.lines().iterator().forEachRemaining(sj::add);
				String xresult = sj.toString();
				int retvalx = p31.waitFor();
				System.out.println(xresult);
				E2_PostgreSQLManager.getInstance(table2, levels, attributes, writer).reconnect();
				E2_MongoDBManager.getInstance(table2, levels, attributes, writer).sum();
				E2_PostgreSQLManager.getInstance(table2, levels, attributes, writer).sum();
			}
			E2_MongoDBManager.getInstance(table2, levels, attributes, writer).size();
			E2_PostgreSQLManager.getInstance(table2, levels, attributes, writer).size();

			E2_MongoDBManager.getInstance(table2, levels, attributes, writer).destroyme();
			E2_PostgreSQLManager.getInstance(table2, levels, attributes, writer).destroyme();

			gen.resetIndex();
		}
	}

	public static JsonObject generateTemplate(int currentLevel, int levels, boolean addSiblings, int attributes) {
		JsonObjectBuilder out = Json.createObjectBuilder();
		if (currentLevel == 1)
			out.add("_id", JsonValue.TRUE);
		if (currentLevel < levels) {
			out.add("type", "object");
			JsonObjectBuilder properties = Json.createObjectBuilder();
			properties.add(
					"a" + (currentLevel < 10 ? '0' + String.valueOf(currentLevel) : String.valueOf(currentLevel)),
					generateTemplate(++currentLevel, levels, addSiblings, attributes));
			out.add("properties", properties);
		} else {
			out.add("type", "object");
			JsonObjectBuilder lastLevelProperties = Json.createObjectBuilder();
			if (addSiblings) {
				for (int i = currentLevel; i <= attributes; ++i) {
					JsonObjectBuilder A = Json.createObjectBuilder();
					A.add("type", "number");
					A.add("nullProbability", 0);
					A.add("minimum", -10);
					A.add("maximum", 10);
					lastLevelProperties.add("a" + (i < 10 ? '0' + String.valueOf(i) : String.valueOf(i)), A);
				}
			} else {
				JsonObjectBuilder A = Json.createObjectBuilder();
				A.add("type", "number");
				A.add("nullProbability", 0);
				A.add("minimum", -10);
				A.add("maximum", 10);
				lastLevelProperties.add(
						"a" + (attributes < 10 ? '0' + String.valueOf(attributes) : String.valueOf(attributes)), A);
			}
			out.add("properties", lastLevelProperties);
		}
		return out.build();
	}

	public static void main(String[] args) throws Exception {
		CSVWriter writer = new CSVWriter(new FileWriter("ideas_e2.csv"));
		writer.writeNext(new String[] { "DB", "operation", "table", "levels", "attributes", "runtime (ns)", "size",
				"compresed" });
//		generate("/root/ideas/schemas/e1_withArrays.json", writer);
		generate(writer);
		writer.close();
	}

}
