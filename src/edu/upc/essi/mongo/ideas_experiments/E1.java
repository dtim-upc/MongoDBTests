package edu.upc.essi.mongo.ideas_experiments;

import edu.upc.essi.mongo.datagen.DocumentSet;
import edu.upc.essi.mongo.datagen.Generator;
import edu.upc.essi.mongo.manager.E1_MongoDBManager;
import edu.upc.essi.mongo.manager.E1_PostgreSQLManager;
import org.bson.Document;

/**
 * Experiment 1: The goal of this experiment is to evaluate the impact of
 * monovalued vs. multivalued attributes. Precisely, we will evaluate the impact
 * of different alternatives to store arrays.
 */
public class E1 {

	public static void generate(String template) throws Exception {
		
		
		E1_PostgreSQLManager.getInstance("e1", template).sumTupleWithAttributes();
		Generator gen = Generator.getInstance();
		
		for (int i = 0; i < 10; ++i) {
			gen.generateFromPseudoJSONSchema(10, template).stream().map(d -> Document.parse(d.toString()))
					.forEach(DocumentSet.getInstance().documents::add);

            E1_MongoDBManager.getInstance("e1", template).insertAsJSONWithArray();
            E1_MongoDBManager.getInstance("e1", template).insertAsJSONWithAttributes();
			E1_PostgreSQLManager.getInstance("e1", template).insertAsJSONWithArray();
			E1_PostgreSQLManager.getInstance("e1", template).insertAsJSONWithAttributes();
			E1_PostgreSQLManager.getInstance("e1", template).insertAsTupleWithArray();
			E1_PostgreSQLManager.getInstance("e1", template).insertAsTupleWithAttributes();


			DocumentSet.getInstance().documents.clear();

		}
		
		E1_PostgreSQLManager.getInstance("e1", template).sumTupleWithArray();
		E1_PostgreSQLManager.getInstance("e1", template).sumTupleWithAttributes();
		E1_PostgreSQLManager.getInstance("e1", template).sumJSONWithAttributes();
		E1_PostgreSQLManager.getInstance("e1", template).sumJSONWithArray();
		E1_MongoDBManager.getInstance("e1", template).sumJSONWithAttributes();
		E1_MongoDBManager.getInstance("e1", template).sumJSONWithArray();
	}

	public static void main(String[] args) throws Exception {
		generate("data/generator_schemas/e1_withArrays.json");

	}

}
