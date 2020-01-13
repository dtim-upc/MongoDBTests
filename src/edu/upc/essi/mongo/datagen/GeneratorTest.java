package edu.upc.essi.mongo.datagen;

import javax.json.JsonArray;

public class GeneratorTest {

    public static void main(String args[]) throws Exception {
        Generator gen = Generator.getInstance();
        gen.generateFromPseudoJSONSchema(5,"/home/snadal/UPC/Projects/MongoDBTests/data/generator_schemas/coordinates.json")
            .forEach(System.out::println);
        //JsonArray out = gen.generate(true,10, 3, 3, .5f, .2f, 0f);
        //out.forEach(System.out::println);
    }
}
