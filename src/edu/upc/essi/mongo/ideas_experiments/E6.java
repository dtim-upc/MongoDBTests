package edu.upc.essi.mongo.ideas_experiments;

import com.opencsv.CSVWriter;
import edu.upc.essi.mongo.datagen.DocumentSet;
import edu.upc.essi.mongo.datagen.Generator;
import edu.upc.essi.mongo.manager.E4_MongoDBManager;
import edu.upc.essi.mongo.manager.E4_PostgreSQLManager;
import edu.upc.essi.mongo.manager.E6_PostgreSQLManager;
import edu.upc.essi.mongo.manager.JSONSchema;
import org.bson.Document;

import javax.json.JsonObject;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;

/**
 * Experiment 4: The goal of this experiment is to evaluate the impact of
 * semantic validation (i.e., CHECK constraints)
 */
public class E6 {

	public static void generate(CSVWriter writer) throws Exception {
		Generator gen = Generator.getInstance();

		// 100i+1 number of attributes
		for (int i = 0; i <= 15; ++i) {
			int attributes = 100*i+1;
			//We reuse e2 template generator
			JsonObject template = E2.generateTemplate(1, 1, true, attributes);
			File fileForTemplate = File.createTempFile("template-", ".tmp");
			fileForTemplate.deleteOnExit();
			Files.write(fileForTemplate.toPath(), template.toString().getBytes());

			JsonObject mongoDB_JSONSchema = JSONSchema.generateJSONSchema(template,true,true);
			JsonObject PSQL_JSONSchema = JSONSchema.generateJSONSchema(template,true,false);
			for (int j = 0; j < 1; ++j) {
				gen.generateFromPseudoJSONSchema(10, fileForTemplate.getAbsolutePath()).stream()
						.map(d -> Document.parse(d.toString())).forEach(DocumentSet.getInstance().documents::add);
				E4_MongoDBManager.getInstance("e6_"+i, attributes, mongoDB_JSONSchema, writer).insert("_JSON_withoutVal");
				E4_MongoDBManager.getInstance("e6_"+i, attributes, mongoDB_JSONSchema, writer).insert("_JSON_withVal");
				E6_PostgreSQLManager.getInstance("e6_"+i, attributes, PSQL_JSONSchema, writer).insert("_TUPLE");
				E6_PostgreSQLManager.getInstance("e6_"+i, attributes, PSQL_JSONSchema, writer).insert("_JSON_withoutVal");
				E6_PostgreSQLManager.getInstance("e6_"+i, attributes, PSQL_JSONSchema, writer).insert("_JSON_withVal");
				DocumentSet.getInstance().documents.clear();
			}
			E4_MongoDBManager.getInstance("e6_"+i, attributes, mongoDB_JSONSchema, writer).sum("_JSON_withoutVal");
			E4_MongoDBManager.getInstance("e6_"+i, attributes, mongoDB_JSONSchema, writer).sum("_JSON_withVal");
			E6_PostgreSQLManager.getInstance("e6_"+i, attributes, PSQL_JSONSchema, writer).sum("_TUPLE");
			E6_PostgreSQLManager.getInstance("e6_"+i, attributes, PSQL_JSONSchema, writer).sum("_JSON_withoutVal");
			E6_PostgreSQLManager.getInstance("e6_"+i, attributes, PSQL_JSONSchema, writer).sum("_JSON_withVal");

			E4_MongoDBManager.getInstance("e6_"+i, attributes, mongoDB_JSONSchema, writer).size("_JSON_withoutVal");
			E4_MongoDBManager.getInstance("e6_"+i, attributes, mongoDB_JSONSchema, writer).size("_JSON_withVal");
			E6_PostgreSQLManager.getInstance("e6_"+i, attributes, PSQL_JSONSchema, writer).size("_TUPLE");
			E6_PostgreSQLManager.getInstance("e6_"+i, attributes, PSQL_JSONSchema, writer).size("_JSON_withoutVal");
			E6_PostgreSQLManager.getInstance("e6_"+i, attributes, PSQL_JSONSchema, writer).size("_JSON_withVal");

			E4_MongoDBManager.getInstance("e6_"+i, attributes, mongoDB_JSONSchema, writer).destroyme();
			E6_PostgreSQLManager.getInstance("e6_"+i, attributes, PSQL_JSONSchema, writer).destroyme();
		}
	}

	public static void main(String[] args) throws Exception {
		CSVWriter writer = new CSVWriter(new FileWriter("ideas_e6.csv"));
		writer.writeNext(new String[] { "DB", "operation", "storage", "attributes", "runtime (ns)", "size",
				"compresed" });
		generate(writer);
		writer.close();
	}

}
