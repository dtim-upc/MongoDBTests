package edu.upc.essi.mongo.ideas_experiments;

import com.google.common.collect.Lists;
import com.opencsv.CSVWriter;
import edu.upc.essi.mongo.datagen.DocumentSet;
import edu.upc.essi.mongo.datagen.E3_DocumentSet;
import edu.upc.essi.mongo.datagen.Generator;
import edu.upc.essi.mongo.manager.E3_MongoDBManager;
import edu.upc.essi.mongo.manager.E3_PostgreSQLManager;
import edu.upc.essi.mongo.manager.E4_MongoDBManager;
import org.apache.commons.io.IOUtils;
import org.bson.Document;

import javax.json.*;
import java.io.File;
import java.io.FileWriter;
import java.io.StringReader;
import java.nio.file.Files;

/**
 * Experiment 4: The goal of this experiment is to evaluate the impact of schema declaration
 */
public class E4 {

	public static void generate(CSVWriter writer) throws Exception {
		Generator gen = Generator.getInstance();
		File templateFile = new File("data/generator_schemas/mini.json");
		JsonObject template = Json.createReader(new StringReader(IOUtils.toString(templateFile.toURI()))).readObject();
		E4_MongoDBManager.getInstance("test",generateMongoDB_JSONSchema(template),writer);
		System.out.println(template);
		System.out.println(generateMongoDB_JSONSchema(template));
		//JsonObject template = Json.createParser(new StringReader(IOUtils.toString(templateFile.toURI()))).getObject();
		gen.generateFromPseudoJSONSchema(10,templateFile.getAbsolutePath())
				.forEach(d -> {
					System.out.println(d);
					DocumentSet.getInstance().documents.add(Document.parse(d.toString()));
				});
		E4_MongoDBManager.getInstance("test",generateMongoDB_JSONSchema(template),writer).insert();
	}

	public static JsonObject generateMongoDB_JSONSchema(JsonObject template) {
		JsonObjectBuilder out = Json.createObjectBuilder();
		if (template.containsKey("type") && template.getString("type").equals("string") &&
		    template.containsKey("domain")) {
			out.add("enum",template.getJsonArray("domain"));
		}
		else if (template.containsKey("type")) out.add("bsonType",
				template.getString("type").equals("number") ? "int" : template.getString("type"));

		template.keySet().forEach(k -> {
			if (!k.equals("type") && !k.equals("domain") && !k.equals("nullProbability") && !k.equals("_id")
					&& !k.equals("minSize") && !k.equals("maxSize") && !k.equals("size")) {
				out.add(k,template.get(k));
			}
		});

		//all properties are required for now
		if (template.containsKey("properties")) {
			JsonArrayBuilder required = Json.createArrayBuilder();
			JsonObjectBuilder properties = Json.createObjectBuilder();
			template.getJsonObject("properties").keySet().forEach(k-> {
				required.add(k);
				properties.add(k, generateMongoDB_JSONSchema(template.getJsonObject("properties").getJsonObject(k)));
			});
			out.add("required", required.build());
			out.add("properties", properties.build());
		}
		return out.build();
	}

	public static void main(String[] args) throws Exception {
		CSVWriter writer = new CSVWriter(new FileWriter("ideas_e4.csv"));
		/*writer.writeNext(new String[] { "DB", "operation", "storage", "probability",  "runtime (ns)", "size",
				"compresed" });*/
//		generate("/root/ideas/schemas/e1_withArrays.json", writer);
		generate(writer);
		writer.close();
	}

}
