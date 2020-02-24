package edu.upc.essi.mongo.ideas_experiments;

import com.google.common.collect.Lists;
import com.opencsv.CSVWriter;
import edu.upc.essi.mongo.datagen.DocumentSet;
import edu.upc.essi.mongo.datagen.E3_DocumentSet;
import edu.upc.essi.mongo.datagen.Generator;
import edu.upc.essi.mongo.manager.E3_MongoDBManager;
import edu.upc.essi.mongo.manager.E3_PostgreSQLManager;
import edu.upc.essi.mongo.manager.E4_MongoDBManager;
import edu.upc.essi.mongo.manager.JSONSchema;
import org.apache.commons.io.IOUtils;
import org.bson.Document;

import javax.json.*;
import java.io.File;
import java.io.FileWriter;
import java.io.StringReader;
import java.nio.file.Files;

/**
 * Experiment 4: The goal of this experiment is to evaluate the impact of
 * document structure and type declaration
 */
public class E4 {

	public static void generate(CSVWriter writer) throws Exception {
		Generator gen = Generator.getInstance();
		File templateFile = new File("data/generator_schemas/mini.json");
		JsonObject template = Json.createReader(new StringReader(IOUtils.toString(templateFile.toURI()))).readObject();
		E4_MongoDBManager.getInstance("test", JSONSchema.generateMongoDB_JSONSchema(template,false), writer);
		System.out.println(template);
		System.out.println(JSONSchema.generateMongoDB_JSONSchema(template,false));
		//JsonObject template = Json.createParser(new StringReader(IOUtils.toString(templateFile.toURI()))).getObject();
		gen.generateFromPseudoJSONSchema(10, templateFile.getAbsolutePath())
				.forEach(d -> {
					System.out.println(d);
					DocumentSet.getInstance().documents.add(Document.parse(d.toString()));
				});
		E4_MongoDBManager.getInstance("test",
				JSONSchema.generateMongoDB_JSONSchema(template,false), writer).insert();
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
