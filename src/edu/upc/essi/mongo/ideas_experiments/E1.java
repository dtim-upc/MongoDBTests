package edu.upc.essi.mongo.ideas_experiments;

import edu.upc.essi.mongo.datagen.DocumentSet;
import edu.upc.essi.mongo.datagen.Generator;
import edu.upc.essi.mongo.manager.MongoDBManager;
import edu.upc.essi.mongo.manager.PostgreSQLManager;
import org.bson.Document;

/**
 * Experiment 1: The goal of this experiment is to evaluate the impact of monovalued vs. multivalued attributes.
 * Precisely, we will evaluate the impact of different alternatives to store arrays.
 */
public class E1 {

    public static void generate(String template) throws Exception {
        Generator gen = Generator.getInstance();
        for (int i = 0; i < 10; ++i) {
            gen.generateFromPseudoJSONSchema(10,template).stream().map(d->Document.parse(d.toString())).
                    forEach(DocumentSet.getInstance().documents::add);
            MongoDBManager.getInstance("e1").insert();
            PostgreSQLManager.getInstance("e1", "e1", template).insertAsJSON();
            PostgreSQLManager.getInstance("e1", "e1", template).insertAsTuple();

            DocumentSet.getInstance().documents.clear();
        }
    }

    public static void main(String[] args) throws Exception {
        generate("/home/snadal/UPC/Projects/MongoDBTests/data/generator_schemas/e1_withArrays.json");
    }

}
