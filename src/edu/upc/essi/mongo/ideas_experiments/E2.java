package edu.upc.essi.mongo.ideas_experiments;

import com.opencsv.CSVWriter;
import edu.upc.essi.mongo.datagen.DocumentSet;
import edu.upc.essi.mongo.datagen.Generator;
import edu.upc.essi.mongo.manager.E1_MongoDBManager;
import edu.upc.essi.mongo.manager.E1_PostgreSQLManager;
import org.bson.Document;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;

/**
 * Experiment 2: The goal of this experiment is to evaluate the impact of
 * embedded objects. Precisely, we will evaluate how the DB behaves in front
 * of different levels of nesting. Two settings will be considered, one where
 * the document size is kept constant, and one where the document size increases
 */
public class E2 {

	public static void generate(CSVWriter writer) throws Exception {

		Generator gen = Generator.getInstance();

		JsonObject templateWithSiblings = generateTemplate(1,3,true,64);
		File fileForTemplateWithSiblings = File.createTempFile("template-",".tmp");
		fileForTemplateWithSiblings.deleteOnExit();
		Files.write(fileForTemplateWithSiblings.toPath(),templateWithSiblings.toString().getBytes());
		gen.generateFromPseudoJSONSchema(10, fileForTemplateWithSiblings.getAbsolutePath())
				.stream().forEach(System.out::println);

		JsonObject templateWithoutSiblings = generateTemplate(1,3,false,64);
		File fileForTemplateWithoutSiblings = File.createTempFile("template-",".tmp");
		fileForTemplateWithoutSiblings.deleteOnExit();
		Files.write(fileForTemplateWithoutSiblings.toPath(),templateWithoutSiblings.toString().getBytes());
		gen.generateFromPseudoJSONSchema(10, fileForTemplateWithoutSiblings.getAbsolutePath())
				.stream().forEach(System.out::println);


		System.exit(1);

		for (int levels = 1; levels <= 64; ++levels) {
			String template = null;//generateTemplateForNonConstantDocument(levels,64,0);

			for (int i = 0; i < 10; ++i) {
				gen.generateFromPseudoJSONSchema(10, template).stream().map(d -> Document.parse(d.toString()))
						.forEach(DocumentSet.getInstance().documents::add);

				System.out.println(DocumentSet.getInstance().documents);

				//E1_MongoDBManager.getInstance("e1", template, writer).insertAsJSONWithArray();

				DocumentSet.getInstance().documents.clear();
			}
		}


/*
		E1_MongoDBManager.getInstance("e1", template, writer).sumJSONWithAttributes();
		E1_MongoDBManager.getInstance("e1", template, writer).sumJSONWithArray();
		E1_PostgreSQLManager.getInstance("e1", template, writer).sumTupleWithArray();
		E1_PostgreSQLManager.getInstance("e1", template, writer).sumTupleWithAttributes();
		E1_PostgreSQLManager.getInstance("e1", template, writer).sumJSONWithAttributes();
		E1_PostgreSQLManager.getInstance("e1", template, writer).sumJSONWithArray();

		E1_MongoDBManager.getInstance("e1", template, writer).sizeJSONWithAttributes();
		E1_MongoDBManager.getInstance("e1", template, writer).sizeJSONWithArray();
		E1_PostgreSQLManager.getInstance("e1", template, writer).sizeJSONWithArray();
		E1_PostgreSQLManager.getInstance("e1", template, writer).sizeJSONWithAttributes();
		E1_PostgreSQLManager.getInstance("e1", template, writer).sizeTupleWithArray();
		E1_PostgreSQLManager.getInstance("e1", template, writer).sizeTupleWithAttributes();

 */
	}

	private static JsonObject generateTemplate(int currentLevel, int levels, boolean addSiblings, int attributes) {
		JsonObjectBuilder out = Json.createObjectBuilder();
		if (currentLevel < levels) {
			out.add("type","object");
			JsonObjectBuilder properties = Json.createObjectBuilder();
			properties.add("a"+(currentLevel<10?'0'+String.valueOf(currentLevel):String.valueOf(currentLevel)),
					generateTemplate(++currentLevel,levels,addSiblings,attributes));
			out.add("properties",properties);
		} else {
			//JsonObjectBuilder lastLevel = Json.createObjectBuilder();
			out.add("type","object");
			JsonObjectBuilder lastLevelProperties = Json.createObjectBuilder();
			if (addSiblings) {
				for (int i = currentLevel; i <= attributes; ++i) {
					JsonObjectBuilder A = Json.createObjectBuilder();
					A.add("type","number");
					A.add("nullProbability", 0);
					A.add("minimum", -10);
					A.add("maximum", 10);
					lastLevelProperties.add("a"+(i<10?'0'+String.valueOf(i):String.valueOf(i)),A);
				}
			} else {
				JsonObjectBuilder A = Json.createObjectBuilder();
				A.add("type","number");
				A.add("nullProbability", 0);
				A.add("minimum", -10);
				A.add("maximum", 10);
				lastLevelProperties.add("a"+(attributes<10?'0'+String.valueOf(attributes):String.valueOf(attributes)),A);
			}
			out.add("properties",lastLevelProperties);
			//out.add()
/*
			out.add("type","number");
			out.add("nullProbability", 0);
			out.add("minimum", -10);
			out.add("maximum", 10);
			out.add()*/
		}
		return out.build();
	}

	public static void main(String[] args) throws Exception {
		CSVWriter writer = new CSVWriter(new FileWriter("ideas_e1.csv"));
		writer.writeNext(new String[] { "DB", "operation", "parameter", "runtime (ns)", "size", "compresed" });
//		generate("/root/ideas/schemas/e1_withArrays.json", writer);
		generate(writer);
		writer.close();
	}

}
