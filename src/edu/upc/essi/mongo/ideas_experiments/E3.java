package edu.upc.essi.mongo.ideas_experiments;

import com.google.common.collect.Lists;
import com.opencsv.CSVWriter;
import edu.upc.essi.mongo.datagen.DocumentSet;
import edu.upc.essi.mongo.datagen.E3_DocumentSet;
import edu.upc.essi.mongo.datagen.Generator;
import edu.upc.essi.mongo.manager.E2_MongoDBManager;
import edu.upc.essi.mongo.manager.E2_PostgreSQLManager;
import edu.upc.essi.mongo.manager.E3_MongoDBManager;
import edu.upc.essi.mongo.manager.E3_PostgreSQLManager;
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
 * Experiment 3: The goal of this experiment is to evaluate the impact of nulls
 */
public class E3 {

	private static boolean trueBooleans(int howMany, boolean... bools) {
		int total = 0;
		for (boolean b : bools)
			if (b && (++total == howMany))
				return true;
		return false;
	}

	public static boolean NULLS_ARE_TEXT = true;
	public static boolean NULLS_ARE_NOTHING = false;
	public static boolean NULLS_ARE_ZERO = false;

	public static void generate(CSVWriter writer) throws Exception {
		if (!trueBooleans(1, NULLS_ARE_TEXT, NULLS_ARE_NOTHING, NULLS_ARE_ZERO)) {
			throw new Exception("Only one boolean can be set");
		}

		Generator gen = Generator.getInstance();

		/**
		 * Probabilities follow a distribution $f(i) = 1-(2^{-i})$
		 *
		 * i=15 creates the following probabilities 0.5, 0.25, 0.125, 0.0625, 0.03125,
		 * 0.015625, 0.0078125, 0.00390625, 0.001953125, 9.765625E-4, 4.8828125E-4,
		 * 2.4414062E-4, 1.2207031E-4, 6.1035156E-5
		 */
		for (int i = 0; i < 15; ++i) {
			JsonObject template = generateTemplate(1d - Math.pow(2, -i));
			File templateFile = File.createTempFile("template-", ".tmp");// templateFile.deleteOnExit();
			Files.write(templateFile.toPath(), template.toString().getBytes());
			for (int j = 0; j < 100; ++j) {
				gen.generateFromPseudoJSONSchema(10000, templateFile.getAbsolutePath()).stream()
						.map(d -> Document.parse(d.toString())).forEach(d -> {
							// get rid of 0s
							if (d.get("a") != null && d.getInteger("a") == 0) {
								d.remove("a");
								d.put("a", 1);
							}
							E3_DocumentSet.getInstance().documents_NULLS_ARE_TEXT.add(d);

							Document d2 = Document.parse(d.toJson());
							if (d2.get("a") == null)
								d2.remove("a");
							E3_DocumentSet.getInstance().documents_NULLS_ARE_NOTHING.add(d2);

							Document d3 = Document.parse(d.toJson());
							if (d3.get("a") == null) {
								d3.remove("a");
								d3.put("a", 0);
							}
							E3_DocumentSet.getInstance().documents_NULLS_ARE_ZERO.add(d3);
						});
				E3_MongoDBManager.getInstance("e3_" + i, i, writer).insert("_NULLS_ARE_TEXT");
				E3_MongoDBManager.getInstance("e3_" + i, i, writer).insert("_NULLS_ARE_NOTHING");
				E3_MongoDBManager.getInstance("e3_" + i, i, writer).insert("_NULLS_ARE_ZERO");
				E3_PostgreSQLManager.getInstance("e3_" + i, i, writer).insert(true, "_NULLS_ARE_TEXT");
				E3_PostgreSQLManager.getInstance("e3_" + i, i, writer).insert(true, "_NULLS_ARE_NOTHING");
				E3_PostgreSQLManager.getInstance("e3_" + i, i, writer).insert(true, "_NULLS_ARE_ZERO");
				E3_PostgreSQLManager.getInstance("e3_" + i, i, writer).insert(false, "_NULLS_ARE_TEXT");
				E3_PostgreSQLManager.getInstance("e3_" + i, i, writer).insert(false, "_NULLS_ARE_ZERO");

				E3_DocumentSet.getInstance().documents_NULLS_ARE_TEXT.clear();
				E3_DocumentSet.getInstance().documents_NULLS_ARE_NOTHING.clear();
				E3_DocumentSet.getInstance().documents_NULLS_ARE_ZERO.clear();
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
				
				E3_MongoDBManager.getInstance("e3_" + i, i, writer).sum("_NULLS_ARE_TEXT");
				E3_MongoDBManager.getInstance("e3_" + i, i, writer).sum("_NULLS_ARE_NOTHING");
				E3_MongoDBManager.getInstance("e3_" + i, i, writer).sum("_NULLS_ARE_ZERO");
				E3_PostgreSQLManager.getInstance("e3_" + i, i, writer).sum(true, "_NULLS_ARE_TEXT");
				E3_PostgreSQLManager.getInstance("e3_" + i, i, writer).sum(true, "_NULLS_ARE_NOTHING");
				E3_PostgreSQLManager.getInstance("e3_" + i, i, writer).sum(true, "_NULLS_ARE_ZERO");
				E3_PostgreSQLManager.getInstance("e3_" + i, i, writer).sum(false, "_NULLS_ARE_TEXT");
				E3_PostgreSQLManager.getInstance("e3_" + i, i, writer).sum(false, "_NULLS_ARE_ZERO");

				E3_MongoDBManager.getInstance("e3_" + i, i, writer).countNulls("_NULLS_ARE_TEXT");
				E3_MongoDBManager.getInstance("e3_" + i, i, writer).countNulls("_NULLS_ARE_NOTHING");
				E3_MongoDBManager.getInstance("e3_" + i, i, writer).countNulls("_NULLS_ARE_ZERO");
				E3_MongoDBManager.getInstance("e3_" + i, i, writer).countNotNulls("_NULLS_ARE_TEXT");
				E3_MongoDBManager.getInstance("e3_" + i, i, writer).countNotNulls("_NULLS_ARE_NOTHING");
				E3_MongoDBManager.getInstance("e3_" + i, i, writer).countNotNulls("_NULLS_ARE_ZERO");
				E3_PostgreSQLManager.getInstance("e3_" + i, i, writer).countNulls(true, "_NULLS_ARE_TEXT");
				E3_PostgreSQLManager.getInstance("e3_" + i, i, writer).countNulls(true, "_NULLS_ARE_NOTHING");
				E3_PostgreSQLManager.getInstance("e3_" + i, i, writer).countNulls(true, "_NULLS_ARE_ZERO");
				E3_PostgreSQLManager.getInstance("e3_" + i, i, writer).countNotNulls(true, "_NULLS_ARE_TEXT");
				E3_PostgreSQLManager.getInstance("e3_" + i, i, writer).countNotNulls(true, "_NULLS_ARE_NOTHING");
				E3_PostgreSQLManager.getInstance("e3_" + i, i, writer).countNotNulls(true, "_NULLS_ARE_ZERO");
				E3_PostgreSQLManager.getInstance("e3_" + i, i, writer).countNulls(false, "_NULLS_ARE_TEXT");
				E3_PostgreSQLManager.getInstance("e3_" + i, i, writer).countNulls(false, "_NULLS_ARE_ZERO");
				E3_PostgreSQLManager.getInstance("e3_" + i, i, writer).countNotNulls(false, "_NULLS_ARE_TEXT");
				E3_PostgreSQLManager.getInstance("e3_" + i, i, writer).countNotNulls(false, "_NULLS_ARE_ZERO");

			}

			E3_MongoDBManager.getInstance("e3_" + i, i, writer).size("_NULLS_ARE_TEXT");
			E3_MongoDBManager.getInstance("e3_" + i, i, writer).size("_NULLS_ARE_NOTHING");
			E3_MongoDBManager.getInstance("e3_" + i, i, writer).size("_NULLS_ARE_ZERO");
			E3_PostgreSQLManager.getInstance("e3_" + i, i, writer).size(true, "_NULLS_ARE_TEXT");
			E3_PostgreSQLManager.getInstance("e3_" + i, i, writer).size(true, "_NULLS_ARE_NOTHING");
			E3_PostgreSQLManager.getInstance("e3_" + i, i, writer).size(true, "_NULLS_ARE_ZERO");
			E3_PostgreSQLManager.getInstance("e3_" + i, i, writer).size(false, "_NULLS_ARE_TEXT");
			E3_PostgreSQLManager.getInstance("e3_" + i, i, writer).size(false, "_NULLS_ARE_ZERO");

			E3_MongoDBManager.getInstance("e3_" + i, i, writer).destroyme();
			E3_PostgreSQLManager.getInstance("e3_" + i, i, writer).destroyme();

			gen.resetIndex();

		}
	}

	private static JsonObject generateTemplate(double probability) {
		JsonObjectBuilder out = Json.createObjectBuilder();
		out.add("_id", JsonValue.TRUE);
		out.add("type", "object");
		JsonObjectBuilder properties = Json.createObjectBuilder();
		JsonObjectBuilder A = Json.createObjectBuilder();
		A.add("type", "number");
		A.add("nullProbability", probability);
		A.add("minimum", -10);
		A.add("maximum", 10);
		JsonObjectBuilder B = Json.createObjectBuilder();
		B.add("type", "string");
		B.add("size", 10);
		properties.add("a", A);
		properties.add("b", B);
		out.add("properties", properties);
		return out.build();
	}

	public static void main(String[] args) throws Exception {
		CSVWriter writer = new CSVWriter(new FileWriter("ideas_e3.csv"));
		writer.writeNext(
				new String[] { "DB", "operation", "storage", "probability", "runtime (ns)", "size", "compresed" });
//		generate("/root/ideas/schemas/e1_withArrays.json", writer);
		generate(writer);
		writer.close();
	}

}
