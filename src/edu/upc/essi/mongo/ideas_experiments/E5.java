package edu.upc.essi.mongo.ideas_experiments;

import com.google.common.collect.Lists;
import com.opencsv.CSVWriter;
import edu.upc.essi.mongo.datagen.DocumentSet;
import edu.upc.essi.mongo.datagen.Generator;
import edu.upc.essi.mongo.manager.E5_MongoDBManager;
import edu.upc.essi.mongo.manager.E5_PostgreSQLManager;
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
public class E5 {
	static int fixedattibs = 10;

	public static void generate(CSVWriter writer) throws Exception {

		Generator gen = Generator.getInstance();

		int attributes = 128;

		for (int attribs : Lists.newArrayList(1, 2, 4, 8, 16, 32, 64, 128)) {

			JsonObject templateWithCount = generateTemplate(attribs, true, attributes);
			File fileForTemplateWithCount = File.createTempFile("template-", ".tmp");
			fileForTemplateWithCount.deleteOnExit();
			Files.write(fileForTemplateWithCount.toPath(), templateWithCount.toString().getBytes());

			JsonObject templateWithLength = generateTemplate(attribs, false, attributes);
			File fileForTemplateWithLength = File.createTempFile("template-", ".tmp");
			fileForTemplateWithLength.deleteOnExit();
			Files.write(fileForTemplateWithLength.toPath(), templateWithLength.toString().getBytes());

			// Experiment with number of attributes
			String table1 = "e5_JSON_attribCount" + "_" + attribs + "_count";
			for (int j = 0; j < 200; ++j) {
				gen.generateFromPseudoJSONSchema(50000, fileForTemplateWithCount.getAbsolutePath()).stream()
						.map(d -> Document.parse(d.toString())).forEach(DocumentSet.getInstance().documents::add);
				E5_MongoDBManager.getInstance(table1, attribs, attributes, writer).insert();
				E5_PostgreSQLManager.getInstance(table1, attribs, attributes, writer).insert();

				DocumentSet.getInstance().documents.clear();
			}
			for (int j = 0; j < 50; j++) {
				E5_MongoDBManager.getInstance(table1, attribs, attributes, writer).sum(false);
				E5_PostgreSQLManager.getInstance(table1, attribs, attributes, writer).sum(false);
			}
			E5_MongoDBManager.getInstance(table1, attribs, attributes, writer).size();
			E5_PostgreSQLManager.getInstance(table1, attribs, attributes, writer).size();

			E5_MongoDBManager.getInstance(table1, attribs, attributes, writer).destroyme();
			E5_PostgreSQLManager.getInstance(table1, attribs, attributes, writer).destroyme();
//
			gen.resetIndex();
			// Experiment with attribute length
			String table2 = "e5_JSON_attribLength" + "_" + attribs + "_length";
			for (int j = 0; j < 200; ++j) {
				gen.generateFromPseudoJSONSchema(50000, fileForTemplateWithLength.getAbsolutePath()).stream()
						.map(d -> Document.parse(d.toString())).forEach(DocumentSet.getInstance().documents::add);
				E5_MongoDBManager.getInstance(table2, attribs, fixedattibs, writer).insert();
				E5_PostgreSQLManager.getInstance(table2, attribs, fixedattibs, writer).insert();

				DocumentSet.getInstance().documents.clear();
			}
			for (int j = 0; j < 50; j++) {
				ProcessBuilder p21 = new ProcessBuilder("/root/mongo/distrib/clear.sh");
				Process p31 = p21.start();
				BufferedReader reader = new BufferedReader(new InputStreamReader(p31.getInputStream()));
				StringJoiner sj = new StringJoiner(System.getProperty("line.separator"));
				reader.lines().iterator().forEachRemaining(sj::add);
				String xresult = sj.toString();
				int retvalx = p31.waitFor();
				System.out.println(xresult);
				E5_MongoDBManager.getInstance(table2, attribs, fixedattibs, writer).sum(true);
				E5_PostgreSQLManager.getInstance(table2, attribs, fixedattibs, writer).sum(true);
			}
			E5_MongoDBManager.getInstance(table2, attribs, fixedattibs, writer).size();
			E5_PostgreSQLManager.getInstance(table2, attribs, fixedattibs, writer).size();

			E5_MongoDBManager.getInstance(table2, attribs, fixedattibs, writer).destroyme();
			E5_PostgreSQLManager.getInstance(table2, attribs, fixedattibs, writer).destroyme();

			gen.resetIndex();
		}
	}

	private static JsonObject generateTemplate(int attribs, boolean isCount, int attributes) {
		JsonObjectBuilder out = Json.createObjectBuilder();
		out.add("_id", JsonValue.TRUE);
		out.add("type", "object");
		JsonObjectBuilder properties = Json.createObjectBuilder();

		if (isCount) {
			for (int i = attributes; i > attributes - attribs; i--) {
				JsonObjectBuilder A = Json.createObjectBuilder();
				A.add("type", "number");
				A.add("nullProbability", 0);
				A.add("minimum", -10);
				A.add("maximum", 10);

				properties.add("a"
						+ (i < 10 ? "00" + String.valueOf(i) : i < 100 ? '0' + String.valueOf(i) : String.valueOf(i)),
						A);
			}
			out.add("properties", properties);
		} else {

			StringBuilder sb = new StringBuilder();

			for (int i = 0; i < attribs; i++) {
				sb.append("0");
			}

			for (int i = 1; i < fixedattibs + 1; i++) {
				JsonObjectBuilder A = Json.createObjectBuilder();
				if (i == fixedattibs) {
					A.add("type", "number");
					A.add("nullProbability", 0);
					A.add("minimum", -10);
					A.add("maximum", 10);

					properties.add("a" + sb.toString() + (i < 10 ? '0' + String.valueOf(i) : String.valueOf(i)), A);
				} else {
					A.add("type", "string");
					A.add("size", attributes - attribs + 1 );

					properties.add("a" + sb.toString() + (i < 10 ? '0' + String.valueOf(i) : String.valueOf(i)), A);
				}
			}
			out.add("properties", properties);
		}
		return out.build();
	}

	public static void main(String[] args) throws Exception {
		CSVWriter writer = new CSVWriter(new FileWriter("ideas_e5.csv"));
		writer.writeNext(
				new String[] { "DB", "operation", "table", "size", "attributes", "runtime (ns)", "size", "compresed" });
		generate(writer);
		writer.close();
	}

}
